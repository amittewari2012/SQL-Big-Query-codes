DECLARE start_date DATE;
DECLARE end_date DATE;
DECLARE fleet ARRAY<STRING>;

SET start_date = (
    SELECT DATE('2022-12-01')
);

SET end_date = (
    SELECT DATE('2022-12-03')
);

SET fleet = ARRAY(
    SELECT ('no-oslo')
);

WITH sim_data AS
(SELECT JSON_EXTRACT_SCALAR(se.tags,'$.fleet_id') AS fleet_id
    , (JSON_EXTRACT_SCALAR(se.tags, '$.date')) AS experiment_date
    ,CAST(SUBSTR(JSON_EXTRACT_SCALAR(se.tags, '$.date'),1,4) AS INT64) year
    ,orders_delivered
    ,utr
    ,avg_delivery_time AS avg_delivery_time
    ,avg_bag_time AS avg_bag_time
    ,avg_time_to_pickup avg_time_to_pickup
    ,rider_hours
    ,avg_delivery_late
    ,rate_stack
    ,avg_distance_to_pickup
    ,avg_pickup_late
    ,avg_pickup_waiting
    ,pct_customer_late
    ,logistics_index
    
FROM `fulfillment-dwh-production.dl.simulator_simulation_statistics` ss
LEFT JOIN `fulfillment-dwh-production.dl.simulator_simulation_run` sr 
ON sr.id=ss.simulation_run_id
LEFT JOIN `fulfillment-dwh-production.dl.simulator_simulation_experiments` se 
ON se.id=sr.experiment_id
WHERE sr.status_name='completed' 
AND se.status_name='completed'
AND CAST(SUBSTR(JSON_EXTRACT_SCALAR(se.tags, '$.date'),1,4) AS INT64)>=2022
),
----------------------------------------------------------
dataset1 AS (
    SELECT o.order_id
        ,o.fleet_id
        , o.created_date
        , o.created_at
        ,ARRAY(SELECT COUNT(d.id) FROM UNNEST(deliveries)d WHERE d.delivery_status='completed') AS deliveries
        ,(SELECT COUNT(d.id) FROM UNNEST (deliveries) d WHERE stacked_deliveries >= 1) stack_del
        ,(o.timings.actual_delivery_time)/60 delivery_time
        ,(SELECT AVG(d.timings.bag_time)/60 FROM UNNEST(deliveries)d WHERE d.timings.bag_time IS NOT NULL) bag_time2
        ,o.original_scheduled_pickup_at-(SELECT MAX(d.rider_picked_up_at) FROM UNNEST(deliveries)d) pickup_time_delta
        ,(o.timings.order_delay / 60) AS del_late
        ,EXTRACT(minute FROM (d.rider_picked_up_at-d.rider_near_restaurant_at))+EXTRACT(second FROM (d.rider_picked_up_at-d.rider_near_restaurant_at))/60 pickup_waiting 
        ,o.timings.to_vendor_time/60 AS time_to_pickup 
        ,o.timings.order_delay/60 AS ord_status_min
        ,IF(is_outlier_pickup_distance_manhattan IS FALSE, od.pickup_distance_manhattan, NULL) AS pickup_distance_manhattan_km
        ,IF(is_outlier_dropoff_distance_manhattan IS FALSE, od.dropoff_distance_manhattan, NULL) AS dropoff_distance_manhattan_km

    FROM `fulfillment-dwh-production.cl.orders` o
    LEFT JOIN UNNEST(deliveries) d on d.is_primary    
    LEFT JOIN (SELECT * 
    FROM `fulfillment-dwh-production.cl._outlier_deliveries` 
    WHERE  created_date=DATE(start_date)
    
    
    ) AS od ON
    d.id=od.delivery_id 

    WHERE o.created_date=start_date 
    AND fleet_id IN UNNEST(fleet)
    
),

final AS (
    SELECT d.order_id
    ,d.fleet_id
    ,d.created_date
    ,d.created_at
    ,d.deliveries
    ,d.stack_del
    ,d.delivery_time
    ,IF(EXTRACT(MINUTE FROM d.pickup_time_delta)+EXTRACT(SECOND FROM d.pickup_time_delta)>0,EXTRACT(MINUTE FROM d.pickup_time_delta)+EXTRACT(SECOND FROM d.pickup_time_delta),NULL) pickup_time_delta
    ,d.bag_time2
    ,d.del_late
    ,d.pickup_waiting
    ,d.time_to_pickup
    ,d.pickup_distance_manhattan_km
    ,d.dropoff_distance_manhattan_km
    ,IF(ord_status_min >= 0, d.order_id, NULL) AS order_on_time_n
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
        ,NULL AS pickup_time_delta
        ,NULL AS bag_time2
        ,NULL AS del_late
        ,NULL AS pickup_waiting
        ,NULL AS time_to_pickup
        ,NULL AS pickup_distance_manhattan_km
        ,NULL AS dropoff_distance_manhattan_km
        ,NULL AS order_on_time_n
        ,working_time/60 working_hrs
        FROM `fulfillment-dwh-production.cl._working_time_over_minute`
        WHERE created_date=DATE(start_date) 

),

prod_data AS (
    SELECT fleet_id
        ,created_date
        ,COUNT(deliveries) AS deliveries
        ,SUM(stack_del)/COUNT(deliveries)*100 AS stack_rate
        ,AVG(delivery_time) AS avg_delivery_time
        ,COUNT(deliveries)/SUM(final.working_hrs) AS UTR
        ,AVG(final.delivery_time)*SUM(final.working_hrs)/COUNT(final.deliveries) AS logisitcs_index
        ,AVG(bag_time2) AS avg_bag_time
        ,AVG(del_late) AS avg_del_late
        ,AVG(pickup_waiting) AS avg_pickup_waiting
        ,AVG(final.pickup_time_delta) AS pickup_late
        ,AVG(time_to_pickup) AS avg_time_to_pickup
        ,AVG(pickup_distance_manhattan_km)*1000 AS pickup_distance_manhattan_mtrs
        ,SUM(working_hrs) as working_hours
        ,COUNT(order_on_time_n)/COUNT(deliveries) order_delayed

    FROM final
    WHERE created_date=start_date 
    AND fleet_id IN UNNEST(fleet)

        GROUP BY 1,2
),
combined AS
(SELECT * 
FROM prod_data
INNER JOIN sim_data
ON prod_data.fleet_id=sim_data.fleet_id
AND created_date=DATE(experiment_date))

SELECT * 
FROM combined 
WHERE created_date=start_date 
