# -*- coding: utf-8 -*-
"""
Created on Fri Jul  5 09:16:49 2019

@author: AA-VManohar
"""

import datetime as dt
from vertica_python import connect
import pandas as pd
import numpy as np
from gurobipy import *

conn_info = {'host' : 'bidb.chewy.local',
 'port': 5433,
 'user': 'vmanohar',
 'password':'Venkat1992',
 'database':'bidb'
}
connection = connect(**conn_info)
cur = connection.cursor()

query = """
with monthly as
(
SELECT 
forecast_snapshot_date AS snapshot_date,
forecast_item,
v.vendor_number,
forecast_area,
--SUM(forecast_demand_qty+forecast_ex_qty) AS Actuals,
SUM(forecast_dailyfrc_qty)AS S099_Forecast,
SUM(forecast_coitardy_coqty)AS Commercial_Forecast
FROM chewybi.forecast_demand_master_report f
join chewybi.item_location_vendor v on f.forecast_item=v.product_part_number and f.forecast_area=v.location_code
WHERE 
f.forecast_snapshot_date=current_date-1
and v.snapshot_date=current_date-1
AND forecast_monthly_date BETWEEN current_date-1 AND current_date+89
and forecast_area = 'AVP1'
and v.primary_vendor_flag='true'
and v.vendor_number is not null
 
GROUP BY 1,2,3,4
) 

,vendor_min as (
select   v.snapshot_date
,v.location_code
, v.vendor_number
,v.vendor_name
--,v.vendor_delivery_type
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
        where v.snapshot_date = current_date-1
        and     v.location_code = 'AVP1'
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
,case when v.vendor_delivery_type = 'Shipping Container' and v.min_planning_amount < 4e6 then 4.15e6
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
,f.S099_Forecast
,f.Commercial_Forecast
,f.Commercial_Forecast/12 as weekly_avg_forecast_in_QTY
/*,case when f.Commercial_Forecast/12 < l.MINRESLOT/2 then 0
      when f.Commercial_Forecast/12 between l.MINRESLOT/2 and l.MINRESLOT then l.MINRESLOT
      else f.Commercial_Forecast/12 end as weekly_avg_forecast_in_QTY*/
, case when v.vendor_min_type = 'cube' then (f.Commercial_Forecast/12)*m.UNITCUBE
       when v.vendor_min_type = 'dollar' then (f.Commercial_Forecast/12)*m.UNITCOST
       when v.vendor_min_type = 'pallet' then (f.Commercial_Forecast/12)/m.PALLETQTY
       when v.vendor_min_type = 'unit' then (f.Commercial_Forecast/12)
       when v.vendor_min_type = 'weight' then (f.Commercial_Forecast/12)*m.UNITWEIGHT
       when v.vendor_min_type = 'case' then (f.Commercial_Forecast/12)/ilv.vendor_uom_qty 
       else null end as weekly_avg_forecast
, case when v.vendor_min_type = 'cube' then ((f.Commercial_Forecast/12)*m.UNITCUBE)/4150000
       when v.vendor_min_type = 'pallet' then ((f.Commercial_Forecast/12)/m.PALLETQTY)/26
       when v.vendor_min_type = 'weight' then ((f.Commercial_Forecast/12)*m.UNITWEIGHT)/42000
       else least(((f.Commercial_Forecast/12)*m.UNITCUBE)/4150000,((f.Commercial_Forecast/12)/m.PALLETQTY)/26,
       ((f.Commercial_Forecast/12)*m.UNITWEIGHT)/42000) end as Truckloads
from monthly f
join vendor_min2 v on f.vendor_number=v.vendor_number and f.forecast_area=v.location_code and f.snapshot_date=v.snapshot_date
join chewybi.item_location_vendor ilv on f.forecast_item = ilv.product_part_number and  f.forecast_area = ilv.location_code 
and f.snapshot_date=ilv.snapshot_date and f.vendor_number=ilv.vendor_number
join chewy_prod_740.C_ITEMLOCATION l on  f.forecast_item = l.ITEM and f.forecast_area=l.LOCATION                
join chewy_prod_740.C_MASTERFILE m on f.forecast_item = m.ITEM
)

,dummy as (
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
,sum(w.Truckloads) as Truckloads_per_week
,case when w.vendor_shipment_method_code in ('CHEWY','CHEWY.COM') then 1
     when w.vendor_delivery_type in ('LTL (Less Truck Load)','Small Parcel') then 1
     when sum(w.weekly_avg_forecast)/w.MOQ <= 1 then 1
     when sum(w.weekly_avg_forecast)/w.MOQ >= 5 then 5
     else floor(sum(w.weekly_avg_forecast)/w.MOQ) end as num_orders_per_week
from week_avg w
group by        1,2,3,4,5,6,7,8,9,10
)

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
,case when d.vendor_delivery_type in ('FTL (Full Truck Load)') then ROUND((d.avg_per_week_in_UOM/d.MOQ)/d.num_orders_per_week)
      else (d.Truckloads_per_week/d.num_orders_per_week) end as Truckloads_per_order 
                 
 from dummy d
"""
cur.execute(query)
result = cur.fetchall()
df = pd.DataFrame(data = result)
df.columns = ['vn','vn_name','loc_code','v_min_type','MOQ','lead_time','dl_type','purchaser_code','ship_code','dist_code','units','sales','orders']
lt_dict = dict([str(i),int(j)] for i,j in zip(df.vn,df.lead_time))
v_dict = dict([str(i),[int(j),int(k)]] for i,j,k in zip(df.vn,df.sales,df.orders))
other_dict = dict([str(i),[str(j),int(k),str(p),str(l),str(m),str(n),str(o)]] for i,j,k,p,l,m,n,o in zip(df.vn,df.vn_name,df.MOQ,df.unit,df.dl_type,df.purchaser_code,df.ship_code,df.dist_code))

a = {}
for i in v_dict.keys():
    a[i] = v_dict[i][0]/v_dict[i][1]
date = [dt.datetime(2019,8,12,0,0,0).date(),dt.datetime(2019,8,13,0,0,0).date(),dt.datetime(2019,8,14,0,0,0).date(),dt.datetime(2019,8,15,0,0,0).date(),dt.datetime(2019,8,16,0,0,0).date()]
#Building LP Model
m = Model()
x = {}
y = {}
#declaring variables:
for i in v_dict.keys():
    for j in range(1,6):
        x[i,j] = m.addVar(lb=0,ub=1,vtype=GRB.BINARY,name = 'x[%s;%d]' %(i,j))
        y[i,j] = m.addVar(lb=0,ub=GRB.INFINITY,vtype= GRB.INTEGER,name = 'y[%s;%d]'%(i,j))
U = m.addVar(lb=0,ub=GRB.INFINITY,vtype = GRB.CONTINUOUS,obj=1,name ='U')
#declaring model sense
m.modelSense = GRB.MINIMIZE

#adding constraints
unit = {}
order = {}
cap = {}
rel = {}
unit_out = {}
lb = {}
for j in range(1,6):
    unit[j] = m.addConstr(U,GRB.GREATER_EQUAL,quicksum(y[i,j] for i in v_dict.keys()),name = 'unit[%d]' %(j))

for i in v_dict.keys():
    order[i] = m.addConstr(quicksum(x[i,j] for j in range(1,6)),GRB.EQUAL,v_dict[i][1],name = 'order[%s]'%(i))
    cap[i] = m.addConstr(quicksum(y[i,j] for j in range(1,6)),GRB.GREATER_EQUAL,v_dict[i][0],name='order_cap[%s]'%(i))

for i,j in x.keys():
    rel[i,j] = m.addConstr(y[i,j] <= 1500000*x[i,j])
    lb[i,j] = m.addConstr(y[i,j] >= a[i] * x[i,j])     


m.write('OR_model.lp')
m.optimize()

out_1 ={}
if m.status == GRB.OPTIMAL:
    for i,j in x.keys():
        if x[i,j].x > 0:
            if j in out_1:
                out_1[j].append(i)
            else:
                out_1[j] = [i]
            if (j,i) in unit_out:
                unit_out[(j,i)].append(y[i,j].x)
            else:
                unit_out[(j,i)] = y[i,j].x
else:
    m.computeIIS()
    m.write('OR.ilp')

out_2 = {}
dt_tm = {}
for i in out_1.keys():
    for j in out_1[i]:
        a = date[i-1]
        b = np.busday_offset(a,-lt_dict[j])
        out_2[j,i] = b
     
        

outfile = open('OR_output.csv','w')
outfile.write('S.no'+','+'vendor_number'+','+'order_date'+','+'recieve_date'+','+'lead_time'+','+'units'+','+'vendor_name'+','+'MOQ'+','+'unit'+','+'delivery_type'+','+'purchaser_code'+','+'ship_code'+','+'dist_code')
outfile.write('\n')

k=1
for i in out_1.keys():
    for j in out_1[i]:
        outfile.write(str(k)+','+str(j)+','+str(out_2[j,i])+','+str(date[i-1])+','+str(lt_dict[j])+','+str(unit_out[i,j])+','+str(other_dict[j][0])+','+str(other_dict[j][1])+','+str(other_dict[j][2])+','+str(other_dict[j][3])+','+str(other_dict[j][4])+','+str(other_dict[j][5])+','+str(other_dict[j][6]))
        outfile.write('\n')
        k = k+1
outfile.close()

   
            
            
    
    

