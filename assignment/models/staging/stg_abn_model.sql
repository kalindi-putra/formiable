{{config(materialized="table")}}
with source_data as (
    select * from source{{'postgress','abn_data'}}
    where 
    1=1
    {{limit_data_filter("data_load_date")}}
),
with final as
(
select 
  * from
  source_data
  where abn_id is not null or abn_id != "";
)

select * from final;