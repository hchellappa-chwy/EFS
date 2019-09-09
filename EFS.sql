WITH vendor_min as (
select   v.location_code, v.vendor_number,v.vendor_name,v.vendor_delivery_type,
                v.vendor_purchaser_code,
                v.vendor_shipment_method_code,
                v.vendor_distribution_method,
                --ifnull(ilv.product_vendor_order_multiple_calculated,1) as order_multiple,
                case    when vendor_planning_minimum_case is not null then 'case' 
                        when vendor_planning_minimum_cube is not null then 'cube' 
                        when vendor_planning_minimum_dollar_amount is not null then 'dollar'
                        when vendor_planning_minimum_pallet is not null then 'pallet' 
                        when vendor_planning_minimum_units is not null then 'unit' 
                        when vendor_planning_minimum_weight is not null then 'weight' 
                        else 'undefined' end as vendor_min_type,
                        
                case when vendor_planning_minimum_case is not null then vendor_planning_minimum_case 
                     when vendor_planning_minimum_cube is not null then vendor_planning_minimum_cube
                     when vendor_planning_minimum_dollar_amount is not null then vendor_planning_minimum_dollar_amount
                     when vendor_planning_minimum_pallet is not null then vendor_planning_minimum_pallet 
                     when vendor_planning_minimum_units is not null then vendor_planning_minimum_units 
                     when vendor_planning_minimum_weight is not null then vendor_planning_minimum_weight else 1 end as min_planning_amount
            
        from    chewybi.vendor_location v
        where  v.snapshot_date = current_date-5
        and     v.location_code = 'AVP1'
        --and v.vendor_automatic_proposal_approval = false
        --and v.vendor_approval = 'Manual'
        order by min_planning_amount
 )
 , vendor_min2 as (
 select v.location_code, v.vendor_number,v.vendor_name,v.vendor_delivery_type,
                v.vendor_purchaser_code,
                v.vendor_shipment_method_code,
                v.vendor_distribution_method,
                case when v.vendor_delivery_type = 'Shipping Container' then 'cube'
                     else v.vendor_min_type end as vendor_min_type,
                     v.min_planning_amount
                     from vendor_min v
                     )
, week_avg  as (
                select i.product_part_number,
                v.vendor_number,
                v.vendor_name,
                v.location_code,
                v.vendor_min_type,
                case when v.vendor_delivery_type = 'Shipping Container' and v.min_planning_amount < 4e6 then 4.15e6
                     when v.vendor_delivery_type = 'FTL (Full Truck Load)' and v.vendor_min_type = 'weight' and v.min_planning_amount < 38000 then 42000   
                else v.min_planning_amount end as MOQ,
                v.vendor_delivery_type,
                v.vendor_purchaser_code,
                v.vendor_shipment_method_code,
                v.vendor_distribution_method,
                ilv.vendor_lead_time_business_days,
              
                 case when v.vendor_min_type = 'cube' then (isnull(i.inventory_snapshot_forecast_quantity_leading_90,0)*7*m.UNITCUBE)
                     when v.vendor_min_type = 'dollar' then (isnull(i.inventory_snapshot_forecast_quantity_leading_90,0)*7*m.UNITCOST)
                     when v.vendor_min_type = 'pallet' then (isnull(i.inventory_snapshot_forecast_quantity_leading_90,0)*7/m.PALLETQTY)
                     when v.vendor_min_type = 'unit' then (isnull(i.inventory_snapshot_forecast_quantity_leading_90,0)*7)
                     when v.vendor_min_type = 'weight' then (isnull(i.inventory_snapshot_forecast_quantity_leading_90,0)*7*m.UNITWEIGHT)
                     when v.vendor_min_type = 'case' then (isnull(i.inventory_snapshot_forecast_quantity_leading_90,0)*7/ilv.product_part_number_base_uom_quantity) else null end as weekly_avg_forecast,
                     
             case when v.vendor_min_type = 'cube' then (isnull(i.inventory_snapshot_forecast_quantity_leading_90,0)*7)
                     when v.vendor_min_type = 'dollar' then (isnull(i.inventory_snapshot_forecast_quantity_leading_90,0)*7)
                     when v.vendor_min_type = 'pallet' then (isnull(i.inventory_snapshot_forecast_quantity_leading_90,0)*7)
                     when v.vendor_min_type = 'unit' then (isnull(i.inventory_snapshot_forecast_quantity_leading_90,0)*7)
                     when v.vendor_min_type = 'weight' then (isnull(i.inventory_snapshot_forecast_quantity_leading_90,0)*7)
                     when v.vendor_min_type = 'case' then (isnull(i.inventory_snapshot_forecast_quantity_leading_90,0)*7) else null end as weekly_avg_forecast_in_QTY,
                     
             case when vendor_min_type = 'cube' 
                        then (isnull(i.inventory_snapshot_forecast_quantity_leading_90,0)*7*m.UNITCUBE)/4150000
                when vendor_min_type = 'pallet'
                        then (isnull(i.inventory_snapshot_forecast_quantity_leading_90,0)*7/m.PALLETQTY)/26
                when vendor_min_type = 'weight'
                        then (isnull(i.inventory_snapshot_forecast_quantity_leading_90,0)*7*m.UNITWEIGHT)/42000
                else least((isnull(i.inventory_snapshot_forecast_quantity_leading_90,0)*7*m.UNITCUBE)/4150000,(isnull(i.inventory_snapshot_forecast_quantity_leading_90,0)*7/m.PALLETQTY)/26,
                                (isnull(i.inventory_snapshot_forecast_quantity_leading_90,0)*7*m.UNITWEIGHT)/42000) end as Truckloads
                                
                          
                from chewybi.inventory_snapshot i
                
                join chewybi.item_location_vendor ilv on
                i.product_part_number = ilv.product_part_number
                --and  i.item_location_primary_vendor_number = ilv.vendor_number
                and  i.location_code = ilv.location_code
                
                join       chewy_prod_740.C_MASTERFILE m        on              ilv.product_part_number = m.ITEM
           
        join vendor_min2 v on v.vendor_number = ilv.vendor_number and v.location_code = ilv.location_code
                where           ilv.snapshot_date = current_date-5 and i.inventory_snapshot_snapshot_dt = current_date-5
        and                     ilv.product_discontinued_flag = false
        and                     ilv.product_published_flag = true
        and                     ilv.primary_vendor_flag = true
        and                     ilv.location_code = 'AVP1'
        and                     ilv.product_purchase_source_location is null
        and                     i.inventory_snapshot_managed_flag = true
        and                     i.item_location_product_discontinued_flag = false
     )
 
--select * from week_avg;
, dummy as (

     select     w.vendor_number,
                w.vendor_name,
                w.location_code,
                w.vendor_min_type,
                w.MOQ,
                w.vendor_lead_time_business_days,
                w.vendor_delivery_type,
                w.vendor_purchaser_code,
                w.vendor_shipment_method_code,
                w.vendor_distribution_method,
               sum(w.weekly_avg_forecast_in_QTY) as avg_per_week_in_QTY,
               sum(w.weekly_avg_forecast) as avg_per_week_in_UOM,
               sum(w.Truckloads) as Truckloads_per_week,
                --sum(weekly_avg_forecast)/min_planning_amount as num_times,
                case    when sum(w.weekly_avg_forecast)/w.MOQ <= 1 then 1
                        when sum(w.weekly_avg_forecast)/w.MOQ >= 5 then 5
                        else floor(sum(w.weekly_avg_forecast)/w.MOQ) end as num_orders_per_weeks
                        
                
                from week_avg w
                group by        1,2,3,4,5,6,7,8,9,10
                
               )
              
, upt_1 as
(
select v.vendor_number,v.vendor_name, ttl.wh_id,
ttl.control_number,
--ttl.end_tran_date,
--count( distinct tpd.po_number) as num_POs, 
count(distinct ttl.item_number) as num_unique_SKUs, 
sum(ttl.tran_qty) as num_units,
sum(ttl.tran_qty)/(count(distinct ttl.item_number)) as UPT
from aad.t_tran_log ttl
join chewybi.procurement_document_product_measures pdpm
on ttl.control_number = pdpm.document_number
join chewybi.vendors v
on pdpm.vendor_key=v.vendor_key
where ttl.wh_id='AVP1' and 
ttl.tran_type = 151
--delivery_date<current_date and
--delivery_date between '01-01-2018' and current_date and 
and ttl.tran_qty <> 0
and pdpm.document_type <> 'Transfer'
group by 1,2,3,4
)

, upt_2 as
(
select vendor_number, vendor_name, wh_id, avg(UPT) as avg_UPT
from upt_1
group by 1,2,3
)
               
, vend_ship_code as (
 
 select         d.vendor_number,
                d.vendor_name,
                d.location_code,
                d.vendor_min_type,
                d.MOQ,
                d.vendor_lead_time_business_days,
                d.vendor_delivery_type,
                d.vendor_purchaser_code,
                d.vendor_shipment_method_code,
                d.vendor_distribution_method,
                --d.avg_per_week_in_QTY as avg_weekly_forecast_QTY,
                --d.avg_per_week_in_UOM as avg_weekly_forecast_UOM,
                case when d.vendor_shipment_method_code in ('CHEWY','CHEWY.COM') then 1
                        else d.num_orders_per_weeks end as num_orders_per_week,
                u.avg_UPT
               /*
                case    when d.avg_per_week_in_UOM/MOQ <= 1 then 1
                        else (d.avg_per_week_in_UOM/d.num_orders_per_weeks)/MOQ end as MOQs_per_order,
               -- case    when d.avg_per_week_in_UOM/MOQ <= 1 then ((MOQ/d.avg_per_week_in_UOM)*d.avg_per_week_in_QTY)
                        --else 
                        d.avg_per_week_in_QTY/d.num_orders_per_weeks as forecast_qty_per_order,
                         d.Truckloads_per_week/d.num_orders_per_weeks as Truckloads_per_order*/
                
                from dummy d
                left join upt_2 u 
                on d.vendor_number = u.vendor_number
                --group by 1,2,3,4,5,6,7,8,9,10,11,12,13
                               
                )
,final as
        (
        select v.*,
        case    when d.avg_per_week_in_UOM/d.MOQ <= 1 then 1
                     else (d.avg_per_week_in_UOM/v.num_orders_per_week)/d.MOQ end as MOQs_per_order,
        d.avg_per_week_in_QTY/v.num_orders_per_week as forecast_qty_per_order,
        case when d.vendor_delivery_type in ('FTL (Full Truck Load)') then ROUND((avg_per_week_in_UOM/d.MOQ)/v.num_orders_per_week)
                 else (d.Truckloads_per_week/v.num_orders_per_week) end as Truckloads_per_order 
                         
         from vend_ship_code v
         join dummy d
         on v.vendor_number = d.vendor_number
         )                
                
                select *
                from final 
                where Truckloads_per_order = Truckloads_per_order
                and MOQs_per_order <> 'Infinity'
                

