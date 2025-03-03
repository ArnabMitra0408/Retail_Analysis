CREATE VIEW retail_data AS
SELECT 
    c."name",
    c.email,
    c.address,
    c.age,
    g.gender_val,
    l.city,
    l.state,
    l.country,
    l.zipcode,
    i.income_category,
    cs.segment_name, 
    s.trasact_date AS "Transaction Date",
    s.total_purchases,
    s.amount,
    s.total_amount,
    p.prod_name,
    p.prod_brand,
    p.prod_category,
    p.prod_type,
    f.feeback_value,
    sm.shipping_method_name AS "Shipping Method",
    pm."method" AS "Payment Method",
    os.status AS "Order Status",
    s.ratings
FROM 
    sales s
INNER JOIN 
    customer c ON s.cust_id = c.cust_id
INNER JOIN 
    gender g ON s.gender_id = g.gender_id
INNER JOIN 
    "location" l ON s.loc_id = l.location_id
INNER JOIN 
    income i ON s.income_id = i.income_type
INNER JOIN 
    customer_segment cs ON s.segment_id = cs.segment_id
INNER JOIN 
    product p ON s.prod_id = p.product_id
INNER JOIN 
    feedback f ON s.feedback_id = f.feedback_id
INNER JOIN 
    shipping_method sm ON s.method_id = sm.shipping_method_id
INNER JOIN 
    payment_method pm ON s.payment_method_id = pm.method_id
INNER JOIN 
    order_status os ON s.status_id = os.status_id;