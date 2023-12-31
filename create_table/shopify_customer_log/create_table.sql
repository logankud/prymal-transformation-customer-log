CREATE EXTERNAL TABLE IF NOT EXISTS shopify_customer_log(
email STRING
, first_order_date DATE
, latest_order_date DATE
, lifetime_spend FLOAT
, lifetime_order_cnt INT
, lifetime_aov FLOAT
, spend_30_days FLOAT
, freq_30_days INT
, aov_30_days FLOAT
, spend_60_days FLOAT
, freq_60_days INT
, aov_60_days FLOAT
, spend_90_days FLOAT
, freq_90_days INT
, aov_90_days FLOAT
, spend_120_days FLOAT
, freq_120_days INT
, aov_120_days FLOAT
, spend_150_days FLOAT
, freq_150_days INT
, aov_150_days FLOAT
, spend_180_days FLOAT
, freq_180_days INT
, aov_180_days FLOAT
, spend_365_days FLOAT
, freq_365_days INT
, aov_365_days FLOAT

)
PARTITIONED BY 
(
partition_date DATE 
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
LOCATION 's3://prymal-analytics/shopify/customer_log/' 
TBLPROPERTIES ("skip.header.line.count"="1")