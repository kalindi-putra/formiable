{{
    config(
        enabled=true,
        materialized="incremental",
        unique_key="abn_id",
        incremental_strategy="merge",
        partition_by=["data_load_date"],
    )
}}

with common_crawl_details as (
    select *
    from {{ref("stg_commoncrawl_model")}}
),

abn_details as (

    select 
       * 
     from {{ref("stg_abn_details")}}
) 
final 
as
(
select 
    trim(b.company_name) as company_name,
    trim(b.company_url) as company_url ,
     trim(a.abnd_id) as abn_id,
        trim(a.entity_name) as entity_name,
        trim(a.entity_type) as entity_type,
        cast(trim(a.registration_date) to date) as registration_date,

         case 
            when trim(a.state) is null or trim(a.state) == "" then "unknown"
            else trim(a.state)
            end as state,
        trim(a.post_code) as post_code,
        trim(a.status) as status,
        trim(a.ACN) as acn_id ,
        trim(a.GST) as gst_id,
        from 
        abn_details a 
        join
        common_crawl_details b
        on
        a.entity_name=b.company_name

)

select * from final;

