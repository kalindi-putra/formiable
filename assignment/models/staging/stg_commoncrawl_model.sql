{{config(materialized="table")}}

with source_data as (
    select * from source{{'postgress','common_crawl'}}
    where 
    1=1
    {{limit_data_filter("data_load_date")}}
),
with final as
(
select 
  * from
  source_data
  where company_name is not null or company_name != "" 
  and  url_key is not null or url_key != ""
  and company_url is not null or company_url !="";
)

select * from final;