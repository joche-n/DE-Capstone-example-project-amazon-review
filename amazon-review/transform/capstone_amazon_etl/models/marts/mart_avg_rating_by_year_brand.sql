{{ config(materialized='view') }}

with joined as (

  select
    t1.review_year,
    t2.brand,
    -- round to 3 decimals and cast to a DECIMAL(12,3)
    cast(round(avg(t1.overall), 3) as number(12,3)) as rating,
    count(*) as review_count
  from {{ ref('stg_reviews') }} as t1
  left join {{ ref('stg_meta') }}    as t2
    on t1.asin = t2.asin
  where t2.brand is not null
  group by 1,2

)

select * from joined
order by review_year desc, brand