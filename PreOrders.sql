SELECT p.created_date,p.Count_PreOrders/t.Count_Orders*100 Perc_PreOrders FROM
(SELECT created_date ,count(is_preorder) Count_PreOrders
FROM `fulfillment-dwh-production.cl.orders`
WHERE created_date <='2022-11-07' and created_date>='2022-11-01'
and is_preorder=true
GROUP BY created_date) p
INNER JOIN 
(SELECT created_date ,count(is_preorder) Count_Orders
FROM `fulfillment-dwh-production.cl.orders`
WHERE created_date <='2022-11-07' and created_date>='2022-11-01'
GROUP BY created_date) t
on p.created_date=t.created_date
ORDER BY created_date ASC
