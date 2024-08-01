-- Databricks notebook source
-- MAGIC %md
-- MAGIC This is a SQL notebook to create schema and the table objects
-- MAGIC 1. Create the schema/database
-- MAGIC 2. Create tables inside the schema

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS TFL_DATA;
USE SCHEMA TFL_DATA;

-- COMMAND ----------

CREATE TABLE tfl_tube_status (status_timestamp timestamp, line string, status string, disruption_reason string)
USING HIVE 
comment "This table holds the latest status of the TFL Tube lines";

CREATE TABLE tfl_tube_status_h (status_timestamp timestamp, line string, status string, disruption_reason string)
USING HIVE 
comment "This table holds the histrical status of the TFL Tube lines whenever the notebook is run";
