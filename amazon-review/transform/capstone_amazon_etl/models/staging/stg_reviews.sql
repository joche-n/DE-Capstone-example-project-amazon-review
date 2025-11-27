{{ config(
    materialized='table'
) }}

with raw as (

  select
    -- exact source columns available in your raw table
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
    -- canonical id: if the raw feed later provides an explicit id, replace the null below
    coalesce(
      null,
      concat(upper(trim(asin_raw)),
             '::',
             coalesce(nullif(unix_review_time_raw::varchar, ''), nullif(trim(review_time_raw), '')))
    ) as review_id,

    -- normalize asin (uppercase, trimmed)
    upper(trim(asin_raw)) as asin,

    -- safe cast overall to FLOAT and clamp within 0..5
    case
      when TRY_CAST(overall_raw AS FLOAT) is null then null
      else least(greatest(TRY_CAST(overall_raw AS FLOAT), 0.0), 5.0)
    end as overall,

    -- robust verified normalization:
    -- 1) cast to varchar to avoid boolean parsing errors
    -- 2) collapse empty string to NULL
    -- 3) map common truthy/falsy values including '1'/'0'
    case
      when lower(nullif(trim(to_varchar(verified_raw)),''))
           in ('true','t','yes','y','1') then true
      when lower(nullif(trim(to_varchar(verified_raw)),''))
           in ('false','f','no','n','0') then false
      else null
    end as verified,

    -- parse review date robustly (tries several patterns; falls back to unix epoch)
    coalesce(
      TRY_TO_DATE(review_time_raw, 'MON DD, YYYY'),      -- e.g. "Aug 31, 2019"
      TRY_TO_DATE(review_time_raw, 'MM DD, YYYY'),       -- e.g. "08 31, 2019"
      TRY_TO_DATE(review_time_raw, 'YYYY-MM-DD'),        -- e.g. "2019-08-31"
      TRY_TO_DATE(review_time_raw, 'DD-MON-YYYY'),       -- e.g. "31-AUG-2019"
      case when unix_review_time_raw is not null then DATEADD(second, unix_review_time_raw::int, '1970-01-01'::date) else null end
    ) as review_date,

    -- extract year (string) from parsed date or from last 4 chars of review_time_raw as a fallback
    case
      when TRY_TO_DATE(review_time_raw, 'MON DD, YYYY') is not null then right(trim(review_time_raw),4)
      when TRY_TO_DATE(review_time_raw, 'YYYY-MM-DD') is not null then to_char(TRY_TO_DATE(review_time_raw,'YYYY-MM-DD'),'YYYY')
      else to_char(
        coalesce(
          TRY_TO_DATE(review_time_raw,'YYYY-MM-DD'),
          case when unix_review_time_raw is not null then DATEADD(second, unix_review_time_raw::int, '1970-01-01'::date) else null end
        ),
        'YYYY'
      )
    end as review_year,

    -- clean text fields: collapse whitespace, trim, and null out empty strings
    nullif(trim(regexp_replace(coalesce(review_text_raw,''), '\\s+', ' ')) , '') as review_text,
    nullif(trim(regexp_replace(coalesce(summary_raw,''), '\\s+', ' ')) , '') as summary,

    trim(coalesce(reviewer_id_raw,'')) as reviewer_id,
    nullif(trim(coalesce(reviewer_name_raw,'')), '') as reviewer_name,

    unix_review_time_raw::bigint as unix_review_time

  from raw

),

deduped as (

  -- dedupe: keep the most recent row per natural key (asin + reviewer_id + unix_review_time if available)
  select *
  from (
    select
      p.*,
      row_number() over (
        partition by asin,
                      coalesce(reviewer_id, 'UNKNOWN'),
                      coalesce(unix_review_time, 0),
                      coalesce(to_char(review_date,'YYYY-MM-DD'), 'UNKNOWN_DATE')
        order by coalesce(unix_review_time, 0) desc nulls last
      ) rn
    from parsed p
  ) where rn = 1

),

final as (

  select
    -- stable surrogate primary key
    md5( coalesce(asin,'') || '|' || coalesce(reviewer_id,'') || '|' || coalesce(to_char(review_date,'YYYY-MM-DD'),'') ) as review_pk,

    review_id,
    asin,
    overall,
    verified,
    review_date,
    review_year,
    review_text,
    summary,
    reviewer_id,
    reviewer_name,
    unix_review_time,
    current_timestamp() as loaded_at

  from deduped

  -- drop rows that are clearly invalid for downstream analysis
  where asin is not null
    and overall is not null
    and review_date is not null

)

select * from final
