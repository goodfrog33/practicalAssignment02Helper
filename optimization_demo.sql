SELECT p.product_name, c.name, COUNT(o.order_id) AS order_count
FROM opt_orders o
JOIN opt_clients c ON o.client_id = c.id
JOIN opt_products p ON o.product_id = p.product_id
WHERE o.order_date > '2023-01-01'
GROUP BY p.product_name, c.name
ORDER BY order_count DESC;

CREATE INDEX idx_opt_orders_order_date
    ON opt_orders(order_date);

CREATE INDEX idx_opt_orders_client_id
    ON opt_orders(client_id);

CREATE INDEX idx_opt_orders_product_id
    ON opt_orders(product_id);

WITH recent_orders AS (
    SELECT order_id, client_id, product_id
    FROM opt_orders
    WHERE order_date > '2023-01-01'
)
SELECT p.product_name, c.name, COUNT(ro.order_id) AS order_count
FROM recent_orders ro
JOIN opt_clients c ON ro.client_id = c.id
JOIN opt_products p ON ro.product_id = p.product_id
GROUP BY p.product_name, c.name
ORDER BY order_count DESC;