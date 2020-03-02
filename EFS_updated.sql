
with monthly as
(
select f.snapshot_date
,forecast_item
,forecast_area
,vendor_number
,Commercial_Forecast
from

(SELECT 
forecast_snapshot_date AS snapshot_date,
forecast_item,
--v.vendor_number,
forecast_area,
--SUM(forecast_demand_qty+forecast_ex_qty) AS Actuals,
--SUM(forecast_dailyfrc_qty)AS S099_Forecast,
SUM(forecast_coitardy_coqty)AS Commercial_Forecast
FROM chewybi.forecast_demand_master_report f

WHERE 
f.forecast_snapshot_date=current_date
--and v.snapshot_date=current_date
AND forecast_monthly_date BETWEEN current_date AND current_date+90
GROUP BY 1,2,3)f
left join chewybi.item_location_vendor v on (f.forecast_item=v.product_part_number and f.forecast_area=v.location_code and v.snapshot_date=f.snapshot_date)
where 1=1
) 

,vendor_min as (
select   v.snapshot_date
,v.location_code
, v.vendor_number
,v.vendor_name
,case when v.vendor_number in ('B000016','P000627','P000588','P000648','P000723','B000138','B000139','P000720','P000737','P000703','P000645','P000683','P000738','5086') then 'LTL (Less Truck Load)'
      when v.vendor_number in ('9080','2822','B000087','B000088','2845','1277','1196','1195','B000128') then 'LTL (Less Truck Load)'
      when v.vendor_number in ('P000647','P000612','P000675','P000696','P000701','P000716','P000636','B000136','P000712','B000137','P000643','P000649','P000728','P000708','P000717','P000667') then 'Small Parcel'
      when v.vendor_number in ('P000714','P000731','P000732','P000733','P000736','P000750') then 'Shipping Container'
      else v.vendor_delivery_type end as vendor_delivery_type  
,v.vendor_purchaser_code
,v.vendor_shipment_method_code
,v.vendor_distribution_method
,v.vendor_lead_time_business_days
,case   when vendor_planning_minimum_case is not null then 'case'
        when vendor_planning_minimum_cube is not null then 'cube' 
        when vendor_planning_minimum_dollar_amount is not null then 'dollar'
        when vendor_planning_minimum_pallet is not null then 'pallet' 
        when vendor_planning_minimum_units is not null then 'unit' 
        when vendor_planning_minimum_weight is not null then 'weight' 
        else 'undefined' end as vendor_min_type
,case   when vendor_planning_minimum_case is not null then vendor_planning_minimum_case 
        when vendor_planning_minimum_cube is not null then vendor_planning_minimum_cube
        when vendor_planning_minimum_dollar_amount is not null then vendor_planning_minimum_dollar_amount
        when vendor_planning_minimum_pallet is not null then vendor_planning_minimum_pallet 
        when vendor_planning_minimum_units is not null then vendor_planning_minimum_units 
        when vendor_planning_minimum_weight is not null then vendor_planning_minimum_weight 
        when vendor_number in ('P000612','P000675','P000737') then 25
        when vendor_number in ('B000016') then 20000
        else 123456789 end as min_planning_amount
        from chewybi.vendor_location v
        where v.snapshot_date = current_date
        --and     v.location_code in ('AVP1','CFC1','EFC3')
        and v.vendor_Status = 'Enabled'
        --and v.vendor_automatic_proposal_approval = false
        --and v.vendor_approval = 'Manual'
        order by min_planning_amount
 )
 
 , vendor_min2 as (
 select v.snapshot_date
 ,v.location_code
 , v.vendor_number
 ,v.vendor_name
 ,v.vendor_delivery_type
 ,v.vendor_purchaser_code
 ,v.vendor_shipment_method_code
 ,v.vendor_distribution_method
 ,v.vendor_lead_time_business_days
 ,case when v.vendor_delivery_type = 'Shipping Container' then 'cube'
       else v.vendor_min_type end as vendor_min_type
,v.min_planning_amount
from vendor_min v
)

,pallets as
(
select products.product_part_number
,products.product_packaged_height
,products.product_packaged_length
,products.product_packaged_width
,products.product_unit_of_measure
,products.product_weight
,products.product_weight_uom
,greatest(products.product_packaged_height/40,products.product_packaged_length/59,products.product_packaged_width/48) as eq_pallets

from chewybi.products
where products.product_setup_completed_flag is true
and products.product_discontinued_flag is false

)

,week_avg as (                    
select distinct f.forecast_item 
,f.forecast_area
,f.vendor_number
,v.vendor_name
,v.vendor_min_type
,v.vendor_delivery_type
,v.vendor_purchaser_code
,v.vendor_shipment_method_code
,v.vendor_distribution_method
,v.vendor_lead_time_business_days
,p.product_weight
,p.product_weight_uom,case when v.vendor_delivery_type = 'Shipping Container' and v.min_planning_amount < 4e6 then 4.15e6
      when v.vendor_delivery_type = 'FTL (Full Truck Load)' and v.vendor_min_type = 'weight' and v.min_planning_amount < 38000 then 42000   
      else v.min_planning_amount end as MOQ
,case when v.vendor_min_type='cube' then m.UNITCUBE
      else 0 end as UNITCUBE  
,case when v.vendor_min_type='dollar' then m.UNITCOST
      else 0 end as UNITCOST 
,case when v.vendor_min_type='pallet' then m.PALLETQTY
      else 0 end as PALLETQTY  
,case when v.vendor_min_type='weight' then m.UNITWEIGHT
      else 0 end as UNITWEIGHT  
,case when v.vendor_min_type = 'case' then ilv.vendor_uom_qty
      else 0 end as vendor_uom_qty  
--,f.S099_Forecast
,f.Commercial_Forecast
,f.Commercial_Forecast/12.86 as weekly_avg_forecast_in_QTY
,(f.Commercial_Forecast/12.86)*m.UNITWEIGHT as weekly_fcst_wt
/*,case when f.Commercial_Forecast/12 < l.MINRESLOT/2 then 0
      when f.Commercial_Forecast/12 between l.MINRESLOT/2 and l.MINRESLOT then l.MINRESLOT
      else f.Commercial_Forecast/12 end as weekly_avg_forecast_in_QTY*/
, case when v.vendor_min_type = 'cube' then (f.Commercial_Forecast/12.86)*m.UNITCUBE
       when v.vendor_min_type = 'dollar' then (f.Commercial_Forecast/12.86)*m.UNITCOST
       when v.vendor_min_type = 'pallet' then (f.Commercial_Forecast/12.86)/m.PALLETQTY
       when v.vendor_min_type = 'unit' then (f.Commercial_Forecast/12.86)
       when v.vendor_min_type = 'weight' then (f.Commercial_Forecast/12.86)*m.UNITWEIGHT
       when v.vendor_min_type = 'case' then (f.Commercial_Forecast/12.86)/ilv.vendor_uom_qty 
       else null end as weekly_avg_forecast
,((f.Commercial_Forecast/12.86)*m.UNITWEIGHT)/43000 as Truckloads_wt
,((f.Commercial_Forecast/12.86)*p.eq_pallets)/26 as Truckloads_pl
from monthly f
join vendor_min2 v on f.vendor_number=v.vendor_number and f.forecast_area=v.location_code and f.snapshot_date=v.snapshot_date
join chewybi.item_location_vendor ilv on f.forecast_item = ilv.product_part_number and  f.forecast_area = ilv.location_code and f.snapshot_date=ilv.snapshot_date and f.vendor_number=ilv.vendor_number
join chewy_prod_740.C_ITEMLOCATION l on  f.forecast_item = l.ITEM and f.forecast_area=l.LOCATION                
join chewy_prod_740.C_MASTERFILE m on f.forecast_item = m.ITEM
join pallets p on f.forecast_item=p.product_part_number 
)

--,dummy as (
select w.vendor_number
,w.vendor_name
,w.forecast_area
,w.vendor_min_type
,w.MOQ
,w.vendor_lead_time_business_days
,w.vendor_delivery_type
,w.vendor_purchaser_code
,w.vendor_shipment_method_code
,w.vendor_distribution_method
,sum(w.weekly_avg_forecast_in_QTY) as avg_per_week_in_QTY
,sum(w.weekly_avg_forecast) as avg_per_week_in_UOM
,sum(w.weekly_fcst_wt) as avg_fcst_wt
,sum(w.Truckloads_wt) as Truckloads_wt_per_week
,sum(w.Truckloads_pl) as Truckloads_pl_per_week
,case when w.vendor_shipment_method_code in ('CHEWY','CHEWY.COM') then 1
     when w.vendor_delivery_type in ('LTL (Less Truck Load)','Small Parcel') then 1
     when sum(w.weekly_avg_forecast)/w.MOQ <= 1 then 1
     when sum(w.weekly_avg_forecast)/w.MOQ >= 5 then 5
     else floor(sum(w.weekly_avg_forecast)/w.MOQ) end as num_orders_per_week
from week_avg w
where w.vendor_number in ('2833')
group by        1,2,3,4,5,6,7,8,9,10
 
--)

/*
--,final as
--(
select d.vendor_number
,d.vendor_name
,d.forecast_area
,d.vendor_min_type
,d.vendor_lead_time_business_days
,d.vendor_delivery_type
,d.vendor_purchaser_code
,d.vendor_shipment_method_code
,d.vendor_distribution_method
,d.MOQ
,d.num_orders_per_week
,case when d.avg_per_week_in_UOM/d.MOQ <= 1 then 1
     else (d.avg_per_week_in_UOM/d.num_orders_per_week)/d.MOQ end as MOQs_per_order
,d.avg_per_week_in_QTY/d.num_orders_per_week as forecast_qty_per_order
,d.avg_fcst_wt/d.num_orders_per_week as fcst_per_ord_wt
,case when d.vendor_delivery_type in ('FTL (Full Truck Load)') then ROUND((d.avg_per_week_in_UOM/d.MOQ)/d.num_orders_per_week)
      else (d.Truckloads_wt_per_week/d.num_orders_per_week) end as Truckloads_wt_per_order 
,case when d.vendor_delivery_type in ('FTL (Full Truck Load)') then ROUND((d.avg_per_week_in_UOM/d.MOQ)/d.num_orders_per_week)
      else (d.Truckloads_pl_per_week/d.num_orders_per_week) end as Truckloads_pl_per_order 
                 
 from dummy d
 where d.vendor_number in ('2833')
 )   */             
--where
--ilv.vendor_uom_qty is not null
/*
ilv.product_discontinued_flag = false
--and ilv.product_published_flag = true
and ilv.primary_vendor_flag = true
and ilv.product_purchase_source_location is null
--where*/
/*) a
group by 1
having count(forecast_item)>1*/

