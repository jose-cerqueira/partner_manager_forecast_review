create temp table dates (date_ date);
insert into dates
values 
(date_add(date_trunc(current_date(), month),interval 1 month)),
(date_add(date_trunc(current_date(), month),interval 2 month)),
(date_add(date_trunc(current_date(), month),interval 3 month));

create temp table stores as (
SELECT distinct
f.storekey
FROM `bigquery-analytics-workbench.silver_read.sca_kpi_longrangeforecast_operational` f
where 
forecastreleased=case
when date_trunc(current_date(),month)='2022-01-01' then 'JAN22'
when date_trunc(current_date(),month)='2022-02-01' then 'FEB22'
when date_trunc(current_date(),month)='2022-03-01' then 'MAR22'
when date_trunc(current_date(),month)='2022-04-01' then 'APR22'
when date_trunc(current_date(),month)='2022-05-01' then 'MAY22'
when date_trunc(current_date(),month)='2022-06-01' then 'JUN22'
when date_trunc(current_date(),month)='2022-07-01' then 'JUL22'
when date_trunc(current_date(),month)='2022-08-01' then 'AUG22'
when date_trunc(current_date(),month)='2022-09-01' then 'SEP22'
when date_trunc(current_date(),month)='2022-10-01' then 'OCT22'
when date_trunc(current_date(),month)='2022-11-01' then 'NOV22'
when date_trunc(current_date(),month)='2022-12-01' then 'DEC22'
when date_trunc(current_date(),month)='2023-01-01' then 'JAN23'
when date_trunc(current_date(),month)='2023-02-01' then 'FEB23'
when date_trunc(current_date(),month)='2023-03-01' then 'MAR23'
when date_trunc(current_date(),month)='2023-04-01' then 'APR23'
when date_trunc(current_date(),month)='2023-05-01' then 'MAY23'
when date_trunc(current_date(),month)='2023-06-01' then 'JUN23'
when date_trunc(current_date(),month)='2023-07-01' then 'JUL23'
when date_trunc(current_date(),month)='2023-08-01' then 'AUG23'
when date_trunc(current_date(),month)='2023-09-01' then 'SEP23'
when date_trunc(current_date(),month)='2023-10-01' then 'OCT23'
when date_trunc(current_date(),month)='2023-11-01' then 'NOV23'
when date_trunc(current_date(),month)='2023-12-01' then 'DEC23'
when date_trunc(current_date(),month)='2024-01-01' then 'JAN24'
when date_trunc(current_date(),month)='2024-02-01' then 'FEB24'
when date_trunc(current_date(),month)='2024-03-01' then 'MAR24'
when date_trunc(current_date(),month)='2024-04-01' then 'APR24'
when date_trunc(current_date(),month)='2024-05-01' then 'MAY24'
when date_trunc(current_date(),month)='2024-06-01' then 'JUN24'
when date_trunc(current_date(),month)='2024-07-01' then 'JUL24'
when date_trunc(current_date(),month)='2024-08-01' then 'AUG24'
when date_trunc(current_date(),month)='2024-09-01' then 'SEP24'
when date_trunc(current_date(),month)='2024-10-01' then 'OCT24'
when date_trunc(current_date(),month)='2024-11-01' then 'NOV24'
when date_trunc(current_date(),month)='2024-12-01' then 'DEC24'
end
and date_diff(date(f.forecastdate),current_date(),month)<=3 and date_diff(date(f.forecastdate),current_date(),month)>=1
order by storekey
);

create temp table base as (
SELECT 
kpi.office_tier_team 
,' previous year' as actual_or_forecast
,kpi.storekey
,kpi.storename
,ds.office
,ds.success_supervisor
,ds.success_manager
,date_add(date(YearMonthFullDate), interval 1 year) as YearMonthFullDate
,extract (year from YearMonthFullDate) as Year
,extract (month from YearMonthFullDate) as Month
--measures
,BO
,BO_1d
,BO_2d
,Items
,Items_NoStock
,item_return
,item_wrongreturn
, null as rowexcludedate

FROM `prd-data-analytics-success-1.silver_read.success_kpi` kpi
left join (
    select distinct ds.storekey, ds.storename , scf.office , scf.current_tier , duf.name as success_supervisor, dufs.name as success_manager
    from `bigquery-analytics-workbench.gold_read.dimstore` ds 
    left join `bigquery-analytics-workbench.gold_read.dimsupplychannelflag` scf on ds.storekey=scf.storekey and scf.storekey=scf.localid and scf.rowexcludedate is null
    left join `bigquery-analytics-workbench.gold_read.dimuserfarfetch` duf on ds.sk_partnerservicesupervisor_current=duf.sk_userfarfetch and duf.department='AM'
    left join `bigquery-analytics-workbench.gold_read.dimuserfarfetch` dufs on ds.sk_partnerservice_current=dufs.sk_userfarfetch and dufs.department='AM'
    where ds.rowexcludedate is null and ds.localid=ds.storekey) ds on kpi.storekey=ds.storekey
left join stores on kpi.storekey=stores.storekey
where date_diff(current_date(),date(YearMonthFullDate),month)<=15 and date_diff(current_date(),date(YearMonthFullDate),month)>=9
and stores.storekey is not null

union all

SELECT 
kpi.office_tier_team 
,'current year' as actual_or_forecast
,kpi.storekey
,kpi.storename
,ds.office
,ds.success_supervisor
,ds.success_manager
,date(YearMonthFullDate) as YearMonthFullDate
,extract (year from YearMonthFullDate) as Year
,extract (month from YearMonthFullDate) as Month
--measures
,BO
,BO_1d
,BO_2d
,Items
,Items_NoStock
,item_return
,item_wrongreturn
, null as rowexcludedate

FROM `prd-data-analytics-success-1.silver_read.success_kpi` kpi
left join (
    select distinct ds.storekey, ds.storename , scf.office , scf.current_tier , duf.name as success_supervisor, dufs.name as success_manager
    from `bigquery-analytics-workbench.gold_read.dimstore` ds 
    left join `bigquery-analytics-workbench.gold_read.dimsupplychannelflag` scf on ds.storekey=scf.storekey and scf.storekey=scf.localid and scf.rowexcludedate is null
    left join `bigquery-analytics-workbench.gold_read.dimuserfarfetch` duf on ds.sk_partnerservicesupervisor_current=duf.sk_userfarfetch and duf.department='AM'
    left join `bigquery-analytics-workbench.gold_read.dimuserfarfetch` dufs on ds.sk_partnerservice_current=dufs.sk_userfarfetch and dufs.department='AM'
    where ds.rowexcludedate is null and ds.localid=ds.storekey) ds on kpi.storekey=ds.storekey
left join stores on kpi.storekey=stores.storekey
where date_diff(current_date(),date(YearMonthFullDate),month)<=3 and date_diff(current_date(),date(YearMonthFullDate),month)>=0
and stores.storekey is not null

union all

SELECT 
case 
when lower(success_supervisor) in ('tiago neto','marixcela suarez') then 'Hybrid'
when ds.current_tier='SMB' then 'SMB'
when ds.office='EU' and ds.current_tier in ("T0 - Key","T1 - Important","Tx - Standard") then concat("Boutique EMEA - ",ds.current_tier)
when ds.office='EU' and ds.current_tier in ("B0 - Key","B1 - Important","Bx - Standard") then concat("Brand EMEA - ",ds.current_tier)
when ds.office='US' then concat('US - ',ds.current_tier)
when ds.office='APAC' then 'APAC'
when ds.office='BR' then 'BR'
when ds.office='JP' then 'JP'
when ds.office='RU' then 'RU'
end as tier_team
,' forecast' as actual_or_forecast
--,f.forecastreleased
,f.storekey
,ds.storename
,ds.office
,ds.success_supervisor
,ds.success_manager
,date(f.forecastdate) as YearMonthFullDate
,extract(year from timestamp(f.forecastdate)) as Year
,extract (month from timestamp(f.forecastdate)) as Month
--,f.scenariorevision
,f.shipped as BO
,f.below1day as BO_1d
,f.below2day as BO_2d
,f.processeditems as Items
,f.nostock as Items_NoStock
,f.returns as item_return
,f.wrongitem as item_wrongreturn
,f.rowexcludedate

FROM `bigquery-analytics-workbench.silver_read.sca_kpi_longrangeforecast_operational` f
left join (
    select distinct ds.storekey, ds.storename , scf.office , scf.current_tier , duf.name as success_supervisor, dufs.name as success_manager
    from `bigquery-analytics-workbench.gold_read.dimstore` ds 
    left join `bigquery-analytics-workbench.gold_read.dimsupplychannelflag` scf on ds.storekey=scf.storekey and scf.storekey=scf.localid and scf.rowexcludedate is null
    left join `bigquery-analytics-workbench.gold_read.dimuserfarfetch` duf on ds.sk_partnerservicesupervisor_current=duf.sk_userfarfetch and duf.department='AM'
    left join `bigquery-analytics-workbench.gold_read.dimuserfarfetch` dufs on ds.sk_partnerservice_current=dufs.sk_userfarfetch and dufs.department='AM'
    where ds.rowexcludedate is null and ds.localid=ds.storekey) ds on f.storekey=ds.storekey
where forecastreleased=case
when date_trunc(current_date(),month)='2022-01-01' then 'JAN22'
when date_trunc(current_date(),month)='2022-02-01' then 'FEB22'
when date_trunc(current_date(),month)='2022-03-01' then 'MAR22'
when date_trunc(current_date(),month)='2022-04-01' then 'APR22'
when date_trunc(current_date(),month)='2022-05-01' then 'MAY22'
when date_trunc(current_date(),month)='2022-06-01' then 'JUN22'
when date_trunc(current_date(),month)='2022-07-01' then 'AUG22'
when date_trunc(current_date(),month)='2022-08-01' then 'JUL22'
when date_trunc(current_date(),month)='2022-09-01' then 'SEP22'
when date_trunc(current_date(),month)='2022-10-01' then 'OCT22'
when date_trunc(current_date(),month)='2022-11-01' then 'NOV22'
when date_trunc(current_date(),month)='2022-12-01' then 'DEC22'
when date_trunc(current_date(),month)='2023-01-01' then 'JAN23'
when date_trunc(current_date(),month)='2023-02-01' then 'FEB23'
when date_trunc(current_date(),month)='2023-03-01' then 'MAR23'
when date_trunc(current_date(),month)='2023-04-01' then 'APR23'
when date_trunc(current_date(),month)='2023-05-01' then 'MAY23'
when date_trunc(current_date(),month)='2023-06-01' then 'JUN23'
when date_trunc(current_date(),month)='2023-07-01' then 'AUG23'
when date_trunc(current_date(),month)='2023-08-01' then 'JUL23'
when date_trunc(current_date(),month)='2023-09-01' then 'SEP23'
when date_trunc(current_date(),month)='2023-10-01' then 'OCT23'
when date_trunc(current_date(),month)='2023-11-01' then 'NOV23'
when date_trunc(current_date(),month)='2023-12-01' then 'DEC23'
when date_trunc(current_date(),month)='2024-01-01' then 'JAN24'
when date_trunc(current_date(),month)='2024-02-01' then 'FEB24'
when date_trunc(current_date(),month)='2024-03-01' then 'MAR24'
when date_trunc(current_date(),month)='2024-04-01' then 'APR24'
when date_trunc(current_date(),month)='2024-05-01' then 'MAY24'
when date_trunc(current_date(),month)='2024-06-01' then 'JUN24'
when date_trunc(current_date(),month)='2024-07-01' then 'AUG24'
when date_trunc(current_date(),month)='2024-08-01' then 'JUL24'
when date_trunc(current_date(),month)='2024-09-01' then 'SEP24'
when date_trunc(current_date(),month)='2024-10-01' then 'OCT24'
when date_trunc(current_date(),month)='2024-11-01' then 'NOV24'
when date_trunc(current_date(),month)='2024-12-01' then 'DEC24'
end  
and storename is not null
and date_diff(date(f.forecastdate),current_date(),month)<=3 and date_diff(date(f.forecastdate),current_date(),month)>=1

union all 

SELECT 
case 
when lower(ds.success_supervisor) in ('tiago neto','marixcela suarez') then 'Hybrid'
when ds.current_tier='SMB' then 'SMB'
when ds.office='EU' and ds.current_tier in ("T0 - Key","T1 - Important","Tx - Standard") then concat("Boutique EMEA - ",ds.current_tier)
when ds.office='EU' and ds.current_tier in ("B0 - Key","B1 - Important","Bx - Standard") then concat("Brand EMEA - ",ds.current_tier)
when ds.office='US' then concat('US - ',ds.current_tier)
when ds.office='APAC' then 'APAC'
when ds.office='BR' then 'BR'
when ds.office='JP' then 'JP'
when ds.office='RU' then 'RU'
end as tier_team
,' budget' as actual_or_forecast
--,s.forecastreleased
,s.storekey
,ds.storename
,ds.office
,ds.success_supervisor
,ds.success_manager
,date(s.first_day_of_month) as YearMonthFullDate
,extract(year from timestamp(s.first_day_of_month)) as Year
,extract (month from timestamp(s.first_day_of_month)) as Month
--,s.scenariorevision
,s.bo_b as BO
,s.bo_1d_b as BO_1d
,s.bo_2d_b as BO_2d
,s.items_b as Items
,s.items_no_stock_b as Items_NoStock
,s.return_items_b as item_return
,s.return_wrongitem_b as item_wrongreturn
,null as rowexcludedate

FROM `prd-data-analytics-success-1.silver_read.success_budget` s
left join (
    select distinct ds.storekey, ds.storename , scf.office , scf.current_tier , duf.name as success_supervisor, dufs.name as success_manager
    from `bigquery-analytics-workbench.gold_read.dimstore` ds 
    left join `bigquery-analytics-workbench.gold_read.dimsupplychannelflag` scf on ds.storekey=scf.storekey and scf.storekey=scf.localid and scf.rowexcludedate is null
    left join `bigquery-analytics-workbench.gold_read.dimuserfarfetch` duf on ds.sk_partnerservicesupervisor_current=duf.sk_userfarfetch and duf.department='AM'
    left join `bigquery-analytics-workbench.gold_read.dimuserfarfetch` dufs on ds.sk_partnerservice_current=dufs.sk_userfarfetch and dufs.department='AM'
    where ds.rowexcludedate is null and ds.localid=ds.storekey) ds on s.storekey=ds.storekey
left join stores on s.storekey=stores.storekey
where date_diff(date(s.first_day_of_month),current_date(),month)<=3 and date_diff(date(s.first_day_of_month),current_date(),month)>=-3
and stores.storekey is not null

union all 

select 
case 
when lower(ds.success_supervisor) in ('tiago neto','marixcela suarez') then 'Hybrid'
when ds.current_tier='SMB' then 'SMB'
when ds.office='EU' and ds.current_tier in ("T0 - Key","T1 - Important","Tx - Standard") then concat("Boutique EMEA - ",ds.current_tier)
when ds.office='EU' and ds.current_tier in ("B0 - Key","B1 - Important","Bx - Standard") then concat("Brand EMEA - ",ds.current_tier)
when ds.office='US' then concat('US - ',ds.current_tier)
when ds.office='APAC' then 'APAC'
when ds.office='BR' then 'BR'
when ds.office='JP' then 'JP'
when ds.office='RU' then 'RU'
end as tier_team
,'success manager review' as actual_or_forecast
--,s.forecastreleased
,s.storekey
,ds.storename
,ds.office
,ds.success_supervisor
,ds.success_manager
,d.date_ as YearMonthFullDate
,extract(year from timestamp(d.date_)) as Year
,extract (month from timestamp(d.date_)) as Month
,case when (engagement_sos_2d+engagement_sos_1d)>=1 then bo else null end as BO
,case when engagement_sos_1d=1 then bo_1d_merge else null end as BO_1d
,case when engagement_sos_2d=1 then bo_2d_merge else null end as BO_2d
,case when engagement_ns=1 then items else null end as Items
,case when engagement_ns=1 then ns_qty_merge else null end as Items_NoStock
,case when engagement_wi=1 then returns else null end as item_return
,case when engagement_wi=1 then returned_wrong_item_merge else null end as item_wrongreturn
,null as rowexcludedate

from stores s
cross join dates d
left join (
    select distinct ds.storekey, ds.storename , scf.office , scf.current_tier , duf.name as success_supervisor, dufs.name as success_manager
    from `bigquery-analytics-workbench.gold_read.dimstore` ds 
    left join `bigquery-analytics-workbench.gold_read.dimsupplychannelflag` scf on ds.storekey=scf.storekey and scf.storekey=scf.localid and scf.rowexcludedate is null
    left join `bigquery-analytics-workbench.gold_read.dimuserfarfetch` duf on ds.sk_partnerservicesupervisor_current=duf.sk_userfarfetch and duf.department='AM'
    left join `bigquery-analytics-workbench.gold_read.dimuserfarfetch` dufs on ds.sk_partnerservice_current=dufs.sk_userfarfetch and dufs.department='AM'
    where ds.rowexcludedate is null and ds.localid=ds.storekey) ds on s.storekey=ds.storekey
left join `prd-data-analytics-success-1.success.success_forecast_reviewed` sfr on s.storekey=sfr.storekey and d.date_=sfr.forecastdate

order by success_supervisor,success_manager,storename,YearMonthFullDate asc --tier_team

);

create temp table calculated as (
select
office_tier_team
,actual_or_forecast
,storekey
,storename
,office
,success_supervisor
,success_manager
,YearMonthFullDate
,Year
,month
,ifnull(BO_1d/nullif(BO,0),999) as sos_1d
,ifnull(BO_2d/nullif(BO,0),999) as sos_2d
,ifnull(Items_NoStock/nullif(Items,0),999) as ns_qty
,ifnull(item_wrongreturn/nullif(item_return,0),999) as wi
from base --where success_manager='Gonzalo Perea'
);

create temp table unpivoted as (
select 
office_tier_team
,actual_or_forecast
,storekey
,storename
,office
,success_supervisor
,success_manager
,Year
,month
,metric
,case
when date(YearMonthFullDate)<date_trunc(current_date(),month) then concat('minus_',date_diff(current_date(),date(YearMonthFullDate),month),'_month_',metric)
when date(YearMonthFullDate)=date_trunc(current_date(),month) then concat('current_month_',metric)
when date(YearMonthFullDate)>date_trunc(current_date(),month) then concat('plus_',date_diff(date(YearMonthFullDate),current_date(),month),'_month_',metric)
end as month_diff_metric
,case when value=999 then null else value end as value
from calculated
unpivot 
(
value for metric in (sos_1d,sos_2d,ns_qty,wi)
)
)
;
create temp table pivoted_grouped as
(
select
office_tier_team
,actual_or_forecast
,storekey
,storename
,office
,success_supervisor
,success_manager
,sum(minus_3_month_ns_qty) as minus_3_month_ns_qty
,sum(minus_2_month_ns_qty) as minus_2_month_ns_qty
,sum(minus_1_month_ns_qty) as minus_1_month_ns_qty
,sum(current_month_ns_qty) as current_month_ns_qty
,sum(plus_1_month_ns_qty) as plus_1_month_ns_qty
,sum(plus_2_month_ns_qty) as plus_2_month_ns_qty
,sum(plus_3_month_ns_qty) as plus_3_month_ns_qty
,sum(minus_3_month_sos_2d) as minus_3_month_sos_2d
,sum(minus_2_month_sos_2d) as minus_2_month_sos_2d
,sum(minus_1_month_sos_2d) as minus_1_month_sos_2d
,sum(current_month_sos_2d) as current_month_sos_2d
,sum(plus_1_month_sos_2d) as plus_1_month_sos_2d
,sum(plus_2_month_sos_2d) as plus_2_month_sos_2d
,sum(plus_3_month_sos_2d) as plus_3_month_sos_2d
,sum(minus_3_month_sos_1d) as minus_3_month_sos_1d
,sum(minus_2_month_sos_1d) as minus_2_month_sos_1d
,sum(minus_1_month_sos_1d) as minus_1_month_sos_1d
,sum(current_month_sos_1d) as current_month_sos_1d
,sum(plus_1_month_sos_1d) as plus_1_month_sos_1d
,sum(plus_2_month_sos_1d) as plus_2_month_sos_1d
,sum(plus_3_month_sos_1d) as plus_3_month_sos_1d
,sum(minus_3_month_wi) as minus_3_month_wi
,sum(minus_2_month_wi) as minus_2_month_wi
,sum(minus_1_month_wi) as minus_1_month_wi
,sum(current_month_wi) as current_month_wi
,sum(plus_1_month_wi) as plus_1_month_wi
,sum(plus_2_month_wi) as plus_2_month_wi
,sum(plus_3_month_wi) as plus_3_month_wi
from 
unpivoted
pivot
(
sum(value)
for month_diff_metric in ('minus_3_month_ns_qty','minus_2_month_ns_qty','minus_1_month_ns_qty','current_month_ns_qty','plus_1_month_ns_qty','plus_2_month_ns_qty','plus_3_month_ns_qty','minus_3_month_sos_2d','minus_2_month_sos_2d','minus_1_month_sos_2d','current_month_sos_2d','plus_1_month_sos_2d','plus_2_month_sos_2d','plus_3_month_sos_2d','minus_3_month_sos_1d','minus_2_month_sos_1d','minus_1_month_sos_1d','current_month_sos_1d','plus_1_month_sos_1d','plus_2_month_sos_1d','plus_3_month_sos_1d','minus_3_month_wi','minus_2_month_wi','minus_1_month_wi','current_month_wi','plus_1_month_wi','plus_2_month_wi','plus_3_month_wi')
) 
group by 1,2,3,4,5,6,7
)
;

drop table if exists success.success_forecast_for_review;
create table success.success_forecast_for_review as (

select
office_tier_team
,actual_or_forecast
,storekey
,storename
,office
,success_supervisor
,success_manager
,minus_3_month_ns_qty 
,minus_2_month_ns_qty 
,minus_1_month_ns_qty 
,current_month_ns_qty 
,plus_1_month_ns_qty 
,plus_2_month_ns_qty 
,plus_3_month_ns_qty 
,minus_3_month_sos_2d 
,minus_2_month_sos_2d 
,minus_1_month_sos_2d 
,current_month_sos_2d 
,plus_1_month_sos_2d 
,plus_2_month_sos_2d 
,plus_3_month_sos_2d 
,minus_3_month_sos_1d 
,minus_2_month_sos_1d 
,minus_1_month_sos_1d 
,current_month_sos_1d 
,plus_1_month_sos_1d 
,plus_2_month_sos_1d 
,plus_3_month_sos_1d 
,minus_3_month_wi  
,minus_2_month_wi  
,minus_1_month_wi  
,current_month_wi  
,plus_1_month_wi 
,plus_2_month_wi 
,plus_3_month_wi 

from pivoted_grouped
where office_tier_team is not null
)
