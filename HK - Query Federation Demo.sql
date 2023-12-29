-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Create a connection

-- COMMAND ----------

-- Create a snowflake connection
CREATE CONNECTION IF NOT EXISTS snowflake_hk
  TYPE snowflake
  OPTIONS (
    host 'tpb53051.us-east-1.snowflakecomputing.com',
    port '443',
    user 'hkidane',
    password secret("hosea_snowflake_pass", "snow_password"),
    sfWarehouse 'HKWAREHOUSE'
  );

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create a foreign catalog referencing the connection

-- COMMAND ----------

DROP CATALOG if exists hk_snowflake_catalog;

-- COMMAND ----------

 CREATE FOREIGN CATALOG IF NOT EXISTS hk_snowflake_catalog
  USING CONNECTION snowflake_hk
  OPTIONS (
    database 'SNOWFLAKE_SAMPLE_DATA'
  );


-- COMMAND ----------

SHOW SCHEMAS IN hk_snowflake_catalog;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Query a snowflake table

-- COMMAND ----------

-- Query pushdown in snowflake with results displayed here

SELECT
l_returnflag,
l_linestatus,
sum(l_quantity) as sum_qty,
sum(l_extendedprice) as sum_base_price,
sum(l_extendedprice * (1-l_discount)) 
  as sum_disc_price,
sum(l_extendedprice * (1-l_discount) * 
  (1+l_tax)) as sum_charge,
avg(l_quantity) as avg_qty,
avg(l_extendedprice) as avg_price,
avg(l_discount) as avg_disc,
count(*) as count_order
FROM
hk_snowflake_catalog.tpch_sf1.lineitem
WHERE
l_shipdate <= dateadd(day, -90, to_date('1998-12-01'))
GROUP BY
l_returnflag,
l_linestatus
ORDER BY
l_returnflag,
l_linestatus;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Join snowflake tables with databricks tables

-- COMMAND ----------

-- Create a synthetic customer data table for join

CREATE TABLE IF NOT EXISTS hosea.default.DatabirkcsCustomersTable (
    O_CUSTKEY DECIMAL(38,0),
    O_NAME VARCHAR(255),
    O_ADDRESS VARCHAR(255),
    O_CITY VARCHAR(255),
    O_STATE VARCHAR(255),
    O_ZIP VARCHAR(10)
);

-- Insert some sample data into the table
INSERT INTO hosea.default.DatabirkcsCustomersTable (O_CUSTKEY, O_NAME, O_ADDRESS, O_CITY, O_STATE, O_ZIP)
VALUES
    (75148,'John Doe', '123 Main St', 'Anytown', 'CA', '12345'),
    (8056, 'Jane Smith', '456 Elm St', 'Other City', 'NY', '54321'),
    (96104, 'Bob Johnson', '789 Oak St', 'Another City', 'TX', '98765'),
    (7693, 'Alice Brown', '101 Pine St', 'Someplace', 'FL', '56789'),
    (125657, 'Charlie Wilson', '222 Maple St', 'New Town', 'IL', '34567');


-- COMMAND ----------

-- join snowflake and databricks tables

CREATE TABLE IF NOT EXISTS hosea.default.snowbrick_orders_customers AS SELECT `O_CUSTKEY`, `O_NAME`, sum(O_TOTALPRICE) as sum_qty
FROM hosea.default.DatabirkcsCustomersTable -- databricks customers table 
JOIN hk_snowflake_catalog.tpch_sf1.orders -- snowflake orders table
USING (`O_CUSTKEY`)
GROUP BY `O_NAME`, `O_CUSTKEY`
ORDER BY `O_CUSTKEY`;

SELECT * FROM hosea.default.snowbrick_orders_customers
LIMIT 5;

-- COMMAND ----------

select * from hosea.default.mv1;
