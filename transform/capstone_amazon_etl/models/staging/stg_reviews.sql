{{ config(
  materialized = 'incremental',
  post_hook = [
    "
    update {{ this }}
    set effective_to = current_timestamp(),
        is_current = false
    where is_current = true
      and exists (
        select 1 from {{ this }} t2
        where t2.review_pk = {{ this }}.review_pk
          and t2.version > {{ this }}.version
      )
    "
  ]
) }}

--------------------------------------------------------------------------------
-- Source parsing/deduping
--------------------------------------------------------------------------------
with raw as (

  select
    t1."asin"                as asin_raw,
    t1."overall"             as overall_raw,
    t1."verified"            as verified_raw,
    t1."reviewTime"          as review_time_raw,
    t1."unixReviewTime"      as unix_review_time_raw,
    t1."reviewText"          as review_text_raw,
    t1."summary"             as summary_raw,
    t1."reviewerID"          as reviewer_id_raw,
    t1."reviewerName"        as reviewer_name_raw
  from {{ source('capstone_amazon_raw','capstone_amazon_review_raw_table') }} t1

),

parsed as (

  select
    coalesce(
      null,
      concat(upper(trim(asin_raw)),
             '::',
             coalesce(nullif(unix_review_time_raw::varchar, ''), nullif(trim(review_time_raw), '')))
    ) as review_id,

    upper(trim(asin_raw)) as asin,

    case
      when TRY_CAST(overall_raw AS FLOAT) is null then null
      else least(greatest(TRY_CAST(overall_raw AS FLOAT), 0.0), 5.0)
    end as overall,

    case
      when lower(nullif(trim(to_varchar(verified_raw)),''))
           in ('true','t','yes','y','1') then true
      when lower(nullif(trim(to_varchar(verified_raw)),''))
           in ('false','f','no','n','0') then false
      else null
    end as verified,

    coalesce(
      TRY_TO_DATE(review_time_raw, 'MON DD, YYYY'),
      TRY_TO_DATE(review_time_raw, 'MM DD, YYYY'),
      TRY_TO_DATE(review_time_raw, 'YYYY-MM-DD'),
      TRY_TO_DATE(review_time_raw, 'DD-MON-YYYY'),
      case when unix_review_time_raw is not null then DATEADD(second, unix_review_time_raw::int, '1970-01-01'::date) else null end
    ) as review_date,

    nullif(trim(regexp_replace(coalesce(review_text_raw,''), '\\s+', ' ')) , '') as review_text,
    nullif(trim(regexp_replace(coalesce(summary_raw,''), '\\s+', ' ')) , '') as summary,

    trim(coalesce(reviewer_id_raw,'')) as reviewer_id,
    nullif(trim(coalesce(reviewer_name_raw,'')), '') as reviewer_name,

    unix_review_time_raw::bigint as unix_review_time

  from raw

),

deduped as (

  select *
  from (
    select
      p.*,
      row_number() over (
        partition by upper(trim(asin)), coalesce(trim(reviewer_id), 'UNKNOWN'), coalesce(unix_review_time, 0)
        order by coalesce(unix_review_time, 0) desc nulls last
      ) rn
    from parsed p
  ) where rn = 1

),

source_final as (

  select
   md5(
  coalesce(asin,'') || '|' ||
  coalesce(reviewer_id,'') || '|' ||
  coalesce(to_char(review_date,'YYYY-MM-DD'),'') || '|' ||
  coalesce(cast(unix_review_time as varchar),'') || '|' ||
  md5(coalesce(review_text,''))) as review_pk,
    review_id,
    asin,
    overall,
    verified,
    review_date,
    to_char(review_date,'YYYY') as review_year,
    review_text,
    summary,
    reviewer_id,
    reviewer_name,
    unix_review_time
  from deduped
  where asin is not null
    and overall is not null
    and review_date is not null

)

--------------------------------------------------------------------------------
-- SCD-2 behavior
-- Non-incremental: seed table (version = 1)
-- Incremental: append new-version rows; post_hook expires previous current rows
--------------------------------------------------------------------------------

{% if not is_incremental() %}

select
  UUID_STRING()                         as surrogate_id,
  sf.review_pk,
  sf.review_id,
  sf.asin,
  sf.overall,
  sf.verified,
  sf.review_date,
  sf.review_year,
  sf.review_text,
  sf.summary,
  sf.reviewer_id,
  sf.reviewer_name,
  sf.unix_review_time,
  current_timestamp()                     as effective_from,
  null                                    as effective_to,
  true                                    as is_current,
  1                                       as version,
  current_timestamp()                     as loaded_at
from source_final sf

{% else %}

-- incremental run
, base_source as (
  select * from source_final
)

-- choose up to 10 random pks that are present BOTH in the current target (is_current) and in the new feed
, simulate_pks as (
  select t.review_pk
  from {{ this }} t
  join (select review_pk from base_source) s on s.review_pk = t.review_pk
  where t.is_current = true
  order by random()
  limit 10
)

-- simulated updates: feed rows for those pks but with a randomized overall value
, simulated_updates as (
  select
    b.*,
    least(
    greatest(
        round(1.0 + (uniform(0::float, 1::float, random()) * 4.0), 1),
        1.0
    ),
    5.0
) as overall_simulated
  from base_source b
  join simulate_pks s on s.review_pk = b.review_pk
)

-- apply the simulated replacements into the feed
, final_feed as (
  select * from base_source where review_pk not in (select review_pk from simulated_updates)
  union all
  select
    review_pk,
    review_id,
    asin,
    overall_simulated as overall,
    verified,
    review_date,
    review_year,
    review_text,
    summary,
    reviewer_id,
    reviewer_name,
    unix_review_time
  from simulated_updates
)

-- produce rows to INSERT (new or changed)
select
  UUID_STRING()                         as surrogate_id,
  ff.review_pk,
  ff.review_id,
  ff.asin,
  ff.overall,
  ff.verified,
  ff.review_date,
  ff.review_year,
  ff.review_text,
  ff.summary,
  ff.reviewer_id,
  ff.reviewer_name,
  ff.unix_review_time,
  current_timestamp()                     as effective_from,
  null                                    as effective_to,
  true                                    as is_current,
  coalesce(t_prev.max_version, 0) + 1     as version,
  current_timestamp()                     as loaded_at
from final_feed ff
left join (
  select review_pk, max(version) as max_version
  from {{ this }}
  group by review_pk
) t_prev
  on ff.review_pk = t_prev.review_pk
left join {{ this }} tcur
  on tcur.review_pk = ff.review_pk and tcur.is_current = true
where
  tcur.review_pk is null
  or (tcur.review_pk is not null and tcur.overall is distinct from ff.overall)

{% endif %}