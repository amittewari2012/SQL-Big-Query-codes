WITH dataset1 AS (
    SELECT o.order_id
    ,o.fleet_id
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
    WHERE created_date = '2022-11-03'
    
    
    ) AS od on
    d.id=od.delivery_id 

WHERE o.created_date = '2022-11-03'
AND o.created_at BETWEEN '2022-11-03 02:02:46.580387 UTC' AND '2022-11-03 18:59:49.092026 UTC'
AND fleet_id='no-oslo'
),

final AS (
    SELECT d.order_id
    ,d.fleet_id
    ,d.created_date
    ,d.created_at
    ,d.deliveries
    ,d.stack_del
    ,d.delivery_time
    ,d.bag_time2
    ,d.del_late
    ,d.pickup_waiting
    ,d.time_to_pickup
    ,d.pickup_distance_manhattan_km
    ,d.dropoff_distance_manhattan_km
    ,IF(ord_status_min >= 10, d.order_id, NULL) AS order_on_time_n
    ,NULL AS working_hrs

FROM dataset1 d

UNION ALL

SELECT NULL AS order_id
        ,fleet_id
        ,created_date
        ,created_at
        ,NULL AS deliveries
        ,NULL AS stack_del
        ,NULL AS delivery_time
        ,NULL AS bag_time2
        ,NULL AS del_late
        ,NULL AS pickup_waiting
        ,NULL AS time_to_pickup
        ,NULL AS pickup_distance_manhattan_km
        ,NULL AS dropoff_distance_manhattan_km
        ,NULL AS order_on_time_n
        ,working_time/60 working_hrs
        FROM `fulfillment-dwh-production.cl._working_time_over_minute`
        WHERE created_date='2022-11-03'
        AND created_at BETWEEN '2022-11-03 02:02:46.580387 UTC' AND '2022-11-03 18:59:49.092026 UTC'
        AND fleet_id='no-oslo'
)


SELECT fleet_id
    , created_date
    ,COUNT(deliveries) AS deliveries
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
    ,COUNT(order_on_time_n)*100/COUNT(deliveries) order_delayed

FROM final
    WHERE created_date='2022-11-03' 
    AND fleet_id='no-oslo'

    AND created_at BETWEEN '2022-11-03 02:02:46.580387 UTC' AND '2022-11-03 18:59:49.092026 UTC'
GROUP BY 1,2
