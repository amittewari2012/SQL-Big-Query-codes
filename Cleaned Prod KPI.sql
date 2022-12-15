WITH dataset1 AS (
    SELECT o.fleet_id
    , o.created_date
    , o.created_at
    ,ARRAY(SELECT COUNT(d.id) FROM UNNEST(deliveries)d WHERE d.delivery_status='completed') as deliveries
    ,(SELECT COUNT(d.id) FROM UNNEST (deliveries) d WHERE stacked_deliveries >= 1) stack_del
    ,(o.timings.actual_delivery_time)/60 delivery_time
    ,(select avg(d.timings.bag_time)/60 from UNNEST(deliveries)d where d.timings.bag_time IS NOT NULL) bag_time2
    
    ,(o.timings.order_delay / 60) AS del_late
    ,EXTRACT(minute FROM (d.rider_picked_up_at-d.rider_near_restaurant_at))+EXTRACT(second FROM (d.rider_picked_up_at-d.        rider_near_restaurant_at))/60 pickup_waiting 
    ,o.timings.to_vendor_time/60 as time_to_pickup 
    ,IF(is_outlier_pickup_distance_manhattan IS FALSE, od.pickup_distance_manhattan, NULL) AS pickup_distance_manhattan_km
    , IF(is_outlier_dropoff_distance_manhattan IS FALSE, od.dropoff_distance_manhattan, NULL) AS dropoff_distance_manhattan_km
FROM `fulfillment-dwh-production.cl.orders` o
LEFT JOIN UNNEST(deliveries) d on d.is_primary
LEFT JOIN (SELECT * 
    FROM `fulfillment-dwh-production.cl._outlier_deliveries` 
    WHERE created_date='2022-11-03'
    ) AS od on
    d.id=od.delivery_id 


)


SELECT fleet_id
    , dataset1.created_date
    ,COUNT(deliveries) AS deliveries
    ,SUM(stack_del)/COUNT(deliveries)*100 AS stack_rate
    ,AVG(delivery_time) AS avg_delivery_time
    
    ,AVG(bag_time2) AS avg_bag_time
    ,AVG(dataset1.del_late) AS avg_del_late
    ,AVG(pickup_waiting) AS avg_pickup_waiting
    ,AVG(time_to_pickup) AS avg_time_to_pickup
    ,AVG(pickup_distance_manhattan_km)*1000 AS pickup_distance_manhattan_mtrs

FROM dataset1
    WHERE created_date='2022-11-03' 
    AND fleet_id='no-oslo'
    AND dataset1.del_late IS NOT NULL
    AND created_at BETWEEN '2022-11-03 02:02:46.580387 UTC' AND '2022-11-03 18:59:49.092026 UTC'
GROUP BY 1,2






--d2.working_hours working_hours_prd,

--d5.avg_pickup_distance_mtrs avg_distance_to_pickup_prd,
--d6.pickup_late avg_pickup_late_prd,

--d7.delayed_orders/d.deliveries*100 pct_customer_late_prd,
