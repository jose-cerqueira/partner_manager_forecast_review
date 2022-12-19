create temp table clean as (
select 
storekey
,case when ((ns_qty_month_plus_1>1) or (ns_qty_month_plus_1<0)) then null else ns_qty_month_plus_1 end as ns_qty_month_plus_1
,case when ((ns_qty_month_plus_2>1) or (ns_qty_month_plus_2<0)) then null else ns_qty_month_plus_2 end as ns_qty_month_plus_2
,case when ((ns_qty_month_plus_3>1) or (ns_qty_month_plus_3<0)) then null else ns_qty_month_plus_3 end as ns_qty_month_plus_3
,case when ((sos_2d_month_plus_1>1) or (sos_2d_month_plus_1<0)) then null else sos_2d_month_plus_1 end as sos_2d_month_plus_1
,case when ((sos_2d_month_plus_2>1) or (sos_2d_month_plus_2<0)) then null else sos_2d_month_plus_2 end as sos_2d_month_plus_2
,case when ((sos_2d_month_plus_3>1) or (sos_2d_month_plus_3<0)) then null else sos_2d_month_plus_3 end as sos_2d_month_plus_3
,case when ((sos_1d_month_plus_1>1) or (sos_1d_month_plus_1<0)) then null else sos_1d_month_plus_1 end as sos_1d_month_plus_1
,case when ((sos_1d_month_plus_2>1) or (sos_1d_month_plus_2<0)) then null else sos_1d_month_plus_2 end as sos_1d_month_plus_2
,case when ((sos_1d_month_plus_3>1) or (sos_1d_month_plus_3<0)) then null else sos_1d_month_plus_3 end as sos_1d_month_plus_3
,case when ((wi_month_plus_1>1) or (wi_month_plus_1<0)) then null else wi_month_plus_1 end as wi_month_plus_1
,case when ((wi_month_plus_2>1) or (wi_month_plus_2<0)) then null else wi_month_plus_2 end as wi_month_plus_2
,case when ((wi_month_plus_3>1) or (wi_month_plus_3<0)) then null else wi_month_plus_3 end as wi_month_plus_3
from `prd-data-analytics-success-1.success.success_forecast_reviewed_base`
)
;
create temp table unpivoted as (
SELECT 
storekey
,case when substr(metric_month,1,3)='sos' then substr(metric_month,1,6) else substr(metric_month,1,2) end as metric
,date_add(date_trunc(current_date(),month),interval cast(right(metric_month,1) as int64) month) as first_day_month
--,metric_month
,value
FROM clean
unpivot 
(
value for metric_month in (ns_qty_month_plus_1 ,ns_qty_month_plus_2 ,ns_qty_month_plus_3 ,sos_2d_month_plus_1 ,sos_2d_month_plus_2 ,sos_2d_month_plus_3 ,sos_1d_month_plus_1 ,sos_1d_month_plus_2 ,sos_1d_month_plus_3 ,wi_month_plus_1 ,wi_month_plus_2 ,wi_month_plus_3)
)
)
;

create temp table pivoted as (
select 
*
from unpivoted
pivot
(
    sum(value) for metric in ('ns','sos_2d','sos_1d','wi')
)
)
;
create temp table forecast as (
SELECT
forecastreleased
,date(forecastdate) as forecastdate
,storekey
,shipped
,processeditems
,returns
,below1day
,below2day
,nostock
,wrongitem 
FROM `bigquery-analytics-workbench.silver_read.sca_kpi_longrangeforecast_operational` 
where rowexcludedate is null and date_diff(date(forecastdate),date_trunc(current_date(),month),month)<=3 
and date_diff(date(forecastdate),date_trunc(current_date(),month),month)>=1
)
;

delete from success.success_forecast_review_daily_monitor where forecast_release_date=date_trunc(current_date(),month) ;
INSERT INTO success.success_forecast_review_daily_monitor (
select 
f.storekey
,ds.storename
,scf.group_
,scf.current_tier as tier
,upper(scf.office) as office
,case 
when lower(duf.name) in ('tiago neto','marixcela suarez') then 'Hybrid'
when tier='SMB' then 'SMB'
when scf.office='EU' and scf.current_tier in ("T0 - Key","T1 - Important","Tx - Standard") then concat("Boutique EMEA - ",scf.current_tier)
when scf.office='EU' and scf.current_tier in ("B0 - Key","B1 - Important","Bx - Standard") then concat("Brand EMEA - ",scf.current_tier)
when scf.office='US' then concat('US - ',scf.current_tier)
when scf.office='APAC' then 'APAC'
when scf.office='BR' then 'BR'
when scf.office='JP' then 'JP'
when scf.office='RU' then 'RU'
end as office_tier_team
,case 
when lower(duf.name) in ('tiago neto','marixcela suarez') then 'Hybrid'
when tier='SMB' then 'SMB'
when scf.office='EU' and scf.current_tier in ("T0 - Key","T1 - Important","Tx - Standard") then "EMEA Boutique"
when scf.office='EU' and scf.current_tier in ("B0 - Key","B1 - Important","Bx - Standard") then "EMEA Brand"
when scf.office='US' then 'US'
when scf.office='APAC' then 'APAC'
when scf.office='BR' then 'BR'
when scf.office='JP' then 'JP'
when scf.office='RU' then 'RU'
end as office_team
,duf.name as success_supervisor
,dufs.name as success_manager
,date_trunc(current_date(),month) as forecast_release_date
,f.forecastdate
,f.processeditems as items
,f.shipped as bo
,f.returns
,ifnull(p.ns*f.processeditems,f.nostock) as ns_qty_merge
,ifnull(p.sos_2d*f.shipped,f.below2day) as bo_2d_merge
,ifnull(p.sos_1d*f.shipped,f.below1day) as bo_1d_merge
,ifnull(p.wi*f.returns,f.wrongitem) as returned_wrong_item_merge
,f.nostock as ns_qty_forecast
,f.below2day as bo_2d_forecast
,f.below1day as bo_1d_forecast
,f.wrongitem as wi_forecast
,case when p.ns is null then 0 else 1 end as engagement_ns
,case when p.sos_2d is null then 0 else 1 end as engagement_sos_2d
,case when p.sos_1d is null then 0 else 1 end as engagement_sos_1d
,case when p.wi is null then 0 else 1 end as engagement_wi
/*,null as actual_items
,null as actual_bo
,null as actual_returns
,null as ns_qty_actual
,null as bo_2d_actual
,null as bo_1d_actual
,null as wi_actual*/
from forecast as f
left join pivoted p on p.storekey=f.storekey and p.first_day_month=f.forecastdate
left join `bigquery-analytics-workbench.gold_read.dimsupplychannelflag` scf on scf.storekey=scf.localid and scf.rowexcludedate is null and scf.storekey=f.storekey
left join `bigquery-analytics-workbench.gold_read.dimstore` ds on scf.sk_store=ds.sk_store
left join `bigquery-analytics-workbench.gold_read.dimuserfarfetch` duf on ds.sk_partnerservicesupervisor_current=duf.sk_userfarfetch and duf.department='AM'
left join `bigquery-analytics-workbench.gold_read.dimuserfarfetch` dufs on ds.sk_partnerservice_current=dufs.sk_userfarfetch and duf.department='AM'
--where f.storekey in (10372,10116,10125,10791)
)
