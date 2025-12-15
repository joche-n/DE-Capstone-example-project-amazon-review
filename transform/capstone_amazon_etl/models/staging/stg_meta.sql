with raw as (

  select
    t2."asin"    as asin_raw,
    t2."title"   as title_raw,
    t2."feature" as feature_raw,
    t2."imageURL" as image_url_raw,
    t2."rank"    as rank_raw,
    t2."VALUE"   as value_raw,
    t2."date"    as date_raw
  from {{ source('capstone_amazon_raw', 'capstone_amazon_review_meta_raw_table') }} t2

),

normalized as (

  select
    upper(trim(asin_raw)) as asin,
    nullif(trim(regexp_replace(coalesce(title_raw,''), '\\s+', ' ')), '') as title,
    nullif(trim(regexp_replace(coalesce(feature_raw,''), '\\s+', ' ')), '') as feature,
    nullif(trim(image_url_raw), '') as image_url,
    nullif(trim(regexp_replace(coalesce(value_raw,''), '\\s+', ' ')), '') as value_field,
    try_cast(nullif(trim(rank_raw),'') as integer) as rank,
    case
      when TRY_TO_DATE(date_raw, 'YYYY-MM-DD') is not null then TRY_TO_DATE(date_raw, 'YYYY-MM-DD')
      when TRY_TO_DATE(date_raw, 'MON DD, YYYY') is not null then TRY_TO_DATE(date_raw, 'MON DD, YYYY')
      else null
    end as meta_date
  from raw

),

brand_extracted as (

  select
    n.*,

    nullif(
      coalesce(
        -- JSON extraction (safe: TRY_PARSE_JSON returns NULL for invalid JSON)
        try_cast(try_parse_json(value_field):brand::string as varchar),

        -- regex fallbacks (case-insensitive)
        regexp_substr(value_field, 'brand[:=]\\s*([^,;\\-\\|\\n]+)', 1, 1, 'i', 1),
        regexp_substr(value_field, 'manufacturer[:=]\\s*([^,;\\-\\|\\n]+)', 1, 1, 'i', 1),

        regexp_substr(feature, 'by\\s+([^,;\\-\\|\\n]+)', 1, 1, 'i', 1),
        regexp_substr(feature, 'brand[:=]\\s*([^,;\\-\\|\\n]+)', 1, 1, 'i', 1),
        regexp_substr(feature, 'manufacturer[:=]\\s*([^,;\\-\\|\\n]+)', 1, 1, 'i', 1)
      ),
      ''
    ) as brand_raw

  from normalized n

),

scored as (

  select
    b.*,
    (case when title is not null then 1 else 0 end)
  + (case when brand_raw is not null then 1 else 0 end)
  + (case when value_field is not null then 1 else 0 end)
  + (case when image_url is not null then 1 else 0 end)
  + (case when rank is not null then 1 else 0 end) as info_score
  from brand_extracted b

),

deduped as (

  select *
  from (
    select
      s.*,
      row_number() over (
        partition by asin
        order by info_score desc,
                 case when brand_raw is not null then 1 else 0 end desc,
                 case when title is not null then 1 else 0 end desc
      ) rn
    from scored s
  ) where rn = 1

),

final as (

  select
    md5(coalesce(asin,'')) as meta_pk,
    asin,
    title,
    nullif(trim(regexp_replace(coalesce(brand_raw,''), '\\s+', ' ')), '') as brand,
    value_field as raw_value,
    feature as features,
    image_url,
    rank,
    meta_date,
    current_timestamp() as loaded_at
  from deduped
  where asin is not null

)

select * from final


