-- Stock Market Lakehouse: Snowflake setup
-- Run these statements in a Snowflake worksheet before running the Databricks notebook.

-- 1. Database and schema, mirroring the Gold layer of the Medallion architecture
CREATE DATABASE STOCK_MARKET_DB;
CREATE SCHEMA STOCK_MARKET_DB.GOLD;

-- 2. Warehouse (compute) — XSMALL is intentionally right-sized for this workload,
--    not a "starter" setting to outgrow. Auto-suspend avoids burning credits when idle.
CREATE WAREHOUSE STOCK_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;

-- 3. Target table — receives the finished Gold layer output from Spark
CREATE TABLE STOCK_MARKET_DB.GOLD.STOCK_MOVING_AVG (
    TICKER STRING,
    TRADE_DATE DATE,
    CLOSE_PRICE FLOAT,
    MOVING_AVG_7DAY FLOAT
);

-- 4. Verification query — confirms all tickers landed correctly after the Spark write
SELECT TICKER, COUNT(*) FROM STOCK_MARKET_DB.GOLD.STOCK_MOVING_AVG GROUP BY TICKER;
