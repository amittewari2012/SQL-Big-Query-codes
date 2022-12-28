--need to put array selection for created-date as it is partition ;
DECLARE dates ARRAY<DATE>;

SET dates = (
    SELECT ARRAY_AGG(DISTINCT DATE(Start_ts))
    FROM 
    (select ds.simulation_run_id,min(ord.created_at) Start_ts,max(ord.created_at) End_ts, ord.fleet_id  from `fulfillment-dwh-production.dl.simulator_delivery_statistics` ds
inner join
(select o.created_date,d.id,o.fleet_id,d.created_at, d.city_id from
`fulfillment-dwh-production.cl.orders` o  , UNNEST(deliveries) AS d
where o.created_date>='2022-11-01' AND o.created_date<='2022-11-07')ord
on ds.delivery_id=ord.id
--where ds.simulation_run_id=66657 and 
where ds.created_date='2022-12-12'
group by simulation_run_id,fleet_id

));

WITH timings AS (
  select sim_stats.experiment_id,sim_stats.Threshold,Start_ts, End_ts, DATE(Start_ts) ord_date , fleet_id
from
(select ds.simulation_run_id,min(ord.created_at) Start_ts,max(ord.created_at) End_ts, ord.fleet_id  from `fulfillment-dwh-production.dl.simulator_delivery_statistics` ds
inner join
(select o.created_date,d.id,o.fleet_id,d.created_at, d.city_id from
`fulfillment-dwh-production.cl.orders` o  , UNNEST(deliveries) AS d
where o.created_date>='2022-11-01' AND o.created_date<='2022-11-07')ord
on ds.delivery_id=ord.id
--where ds.simulation_run_id=66657 and 
where ds.created_date='2022-12-12'
group by simulation_run_id,fleet_id) time_stamp

inner join 



--for metrics JOIN:
(SELECT sr.experiment_id,  
se.name, IFNULL(REGEXP_SUBSTR(REGEXP_SUBSTR(se.name,"time_slicing_no_oslo_hourly_slicing_(.*)"), '[^_no-oslo]+'),"100%") Threshold,ss.*
from
`fulfillment-dwh-production.dl.simulator_simulation_statistics` ss
left join `fulfillment-dwh-production.dl.simulator_simulation_run` sr on sr.id=ss.simulation_run_id
left join `fulfillment-dwh-production.dl.simulator_simulation_experiments` se on se.id=sr.experiment_id

where se.name like 'time_slicing_no_oslo%' AND
ss.created_date='2022-12-12') sim_stats
on time_stamp.simulation_run_id = sim_stats.simulation_run_id
),


dataset1 AS (
    SELECT o.order_id
    ,o.fleet_id fleet
    , o.created_date
    , o.created_at
    ,ARRAY(SELECT COUNT(d.id) FROM UNNEST(deliveries)d WHERE d.delivery_status='completed') as deliveries
    ,(SELECT COUNT(d.id) FROM UNNEST (deliveries) d WHERE stacked_deliveries >= 1) stack_del
    ,(o.timings.actual_delivery_time)/60 delivery_time
    ,(select avg(d.timings.bag_time)/60 from UNNEST(deliveries)d where d.timings.bag_time IS NOT NULL) bag_time2
    
    ,(o.timings.order_delay / 60) AS del_late
    ,EXTRACT(minute FROM (d.rider_picked_up_at-d.rider_near_restaurant_at))+EXTRACT(second FROM (d.rider_picked_up_at-d.rider_near_restaurant_at))/60 pickup_waiting 
    ,o.timings.to_vendor_time/60 as time_to_pickup 
    ,IF(is_outlier_pickup_distance_manhattan IS FALSE, od.pickup_distance_manhattan, NULL) AS pickup_distance_manhattan_km
    ,IF(is_outlier_dropoff_distance_manhattan IS FALSE, od.dropoff_distance_manhattan, NULL) AS dropoff_distance_manhattan_km
    ,o.timings.order_delay/60 AS ord_status_min
FROM `fulfillment-dwh-production.cl.orders` o
LEFT JOIN UNNEST(deliveries) d on d.is_primary
LEFT JOIN (SELECT * 
    FROM `fulfillment-dwh-production.cl._outlier_deliveries` 
    ) AS od on
    d.id=od.delivery_id 
    WHERE o.created_date in UNNEST(dates)
    AND o.fleet_id='no-oslo'
--WHERE o.created_date = '2022-11-03'
--AND o.created_at BETWEEN '2022-11-03 02:02:46.580387 UTC' AND '2022-11-03 18:59:49.092026 UTC'
--AND fleet_id='no-oslo'
),

--select count(*) from
  --  timings t left join dataset1 d1
    --on d1.created_date=t.ord_date
    --WHERE d1.created_at<=t.End_ts
    --AND d1.created_at>=t.Start_ts
    --GROUP BY t.experiment_id, t.Threshold
    --AND 
    --d1.created_at<=t.End_ts
    --AND d1.created_at>=t.Start_ts
    --WHERE d1.created_date='2022-11-03'
--join dataset1 and timings on fleet_id,date concat :
-- ONLY SELECTED DATES ARE BEING DISPLAYED IN 'JOINED_DATA' TABLE :
joined_data AS (
    select * from
    timings t inner join dataset1 d1 --left
    --on t.ord_date=d1.created_date
    --AND 
    ON d1.created_at<=t.End_ts --Where
    AND d1.created_at>=t.Start_ts
    --GROUP BY t.experiment_id, t.Threshold
    --on t.ord_date=d1.created_date
    --where d1.created_date in UNNEST(dates)
    --AND d1.created_at<=t.End_ts
    --AND d1.created_at>=t.Start_ts
    --makes sense to filter here on partition
---------SAB CHANGA SI---------    

),



working_minute AS (
    SELECT w.fleet_id
    ,t.experiment_id
    ,created_date
    ,created_at
    ,working_time
    ,t.Start_ts
    ,t.End_ts
    ,t.Threshold
    FROM `fulfillment-dwh-production.cl._working_time_over_minute` w
    RIGHT JOIN timings t
    ON w.created_at>=t.Start_ts
    And w.created_at<=t.End_ts
    WHERE w.fleet_id='no-oslo'
    AND w.created_date in UNNEST(dates)
    

),

final AS (
    SELECT jd.order_id
    ,jd.fleet_id
    ,jd.experiment_id
    ,jd.created_date
    ,jd.created_at
    ,jd.Threshold
    --,jd.Start_ts
    --,jd.End_ts
    ,jd.deliveries
    ,jd.stack_del
    ,jd.delivery_time
    ,jd.bag_time2
    ,jd.del_late
    ,jd.pickup_waiting
    ,jd.time_to_pickup
    ,jd.pickup_distance_manhattan_km
    ,jd.dropoff_distance_manhattan_km
    --,IF(ord_status_min >= 0, jd.order_id, NULL) AS order_on_time_n
    ,NULL AS working_hrs

FROM joined_data jd

UNION ALL

SELECT NULL AS order_id
        ,fleet_id
        ,working_minute.experiment_id
        ,created_date
        ,created_at
        ,working_minute.threshold
        --,jd.Start_ts
        --,jd.End_ts
        ,NULL AS deliveries
        ,NULL AS stack_del
        ,NULL AS delivery_time
        ,NULL AS bag_time2
        ,NULL AS del_late
        ,NULL AS pickup_waiting
        ,NULL AS time_to_pickup
        ,NULL AS pickup_distance_manhattan_km
        ,NULL AS dropoff_distance_manhattan_km
        --,NULL AS order_on_time_n
        ,working_time/60 working_hrs
        FROM working_minute
        WHERE created_date in UNNEST(dates)
        --AND created_at<=timings.End_ts
        --AND created_at>=timings.Start_ts --BETWEEN '2022-11-03 02:02:46.580387 UTC' AND '2022-11-03 18:59:49.092026 UTC'
        --AND fleet_id='no-oslo'
)


SELECT final.experiment_id
    ,fleet_id
    , created_date
    ,final.Threshold
    ,COUNT(distinct order_id) AS orders
    --,COUNT(deliveries) AS deliveries
    ,SUM(stack_del)/COUNT(deliveries)*100 AS stack_rate
    ,AVG(delivery_time) AS avg_delivery_time
    
    ,AVG(bag_time2) AS avg_bag_time
    ,AVG(del_late) AS avg_del_late
    --avg pickup late
    --pct_customer late

    ,AVG(pickup_waiting) AS avg_pickup_waiting
    ,AVG(time_to_pickup) AS avg_time_to_pickup
    ,AVG(pickup_distance_manhattan_km)*1000 AS pickup_distance_manhattan_mtrs
    ,SUM(working_hrs) as working_hours
    --,COUNT(order_on_time_n)*100--/COUNT(deliveries) order_delayed

FROM final
    
GROUP BY 1,2,3,4
ORDER BY created_date

