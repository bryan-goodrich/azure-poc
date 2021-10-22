-- https://docs.microsoft.com/en-us/azure/synapse-analytics/sql/create-use-external-tables

-- CREATE DATABASE DWH
-- COLLATE Latin1_General_100_BIN2_UTF8;

-- CREATE MASTER KEY ENCRYPTION BY PASSWORD = xxxx


-- CREATE EXTERNAL FILE FORMAT ParquetFormat
-- WITH (
--     FORMAT_TYPE = PARQUET,
--     DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
-- )


-- CREATE DATABASE SCOPED CREDENTIAL WorkspaceIdentity
-- WITH IDENTITY = 'Managed Identity';
-- GO

-- CREATE EXTERNAL DATA SOURCE DWH
-- WITH (
--     LOCATION = 'https://smuduw2bianpadl02.dfs.core.windows.net/stg',
--     CREDENTIAL = WorkspaceIdentity
-- )


DROP EXTERNAL TABLE reading_spc;
GO

CREATE EXTERNAL TABLE reading_spc (
    read_id int,
    servicepoint_id varchar(25),
    dln int,
    channel smallint,
    premise_id int,
    rate_category varchar(20),
    read_starttime varchar(19),
    read_endtime varchar(19),
    read_date int,
    read_hour tinyint,
    read_offset varchar(5),
    utc_endtime varchar(19),
    config_date_from varchar(19),
    config_date_to varchar(19),
    read_length int,
    read_status smallint,
    read_group bigint,
    read_value float,
    channel_unit varchar(20),
    meter_number varchar(20),
    multiplier float,
    kwh_multiplier float,
    recording_device varchar(25),
    billing_system_cycle varchar(5),
    meter_program varchar(20),
    workbin int,
    channel_type varchar(1),
    channel_set_node_group varchar(50),
    dw_created_dt varchar(19),
    dw_modified_dt varchar(27),
    read_month int
)
WITH (
    LOCATION = 'reading_spc/read_month=*/*.parquet',
    DATA_SOURCE = DWH,
    FILE_FORMAT = ParquetFormat
)

GO

-- CREATE EXTERNAL TABLE reading_spc_daily (
    
-- )
-- WITH (
--     LOCATION = '/path/to/file',
--     DATA_SOURCE = CDH,
--     FILE_FORMAT = ParquetFormat
-- )


-- CREATE EXTERNAL TABLE reading_spc_hourly (
    
-- )
-- WITH (
--     LOCATION = '/path/to/file',
--     DATA_SOURCE = CDH,
--     FILE_FORMAT = ParquetFormat
-- )



-- Reading TOP 100 used ~60 MB
ALTER VIEW v_reading_spc
AS
SELECT
    T.*
FROM
    OPENROWSET(BULK '/reading_spc/read_month=*/*.parquet',
        DATA_SOURCE = 'DWH',
        FORMAT ='parquet',
        DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
    ) AS T

GO




ALTER VIEW v_reading_spc2
AS
SELECT
    T.*
FROM
    OPENROWSET(BULK '/reading_spc/read_month=201611/*.parquet',
        DATA_SOURCE = 'DWH',
        FORMAT ='parquet',
        DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
    )
    WITH (
        read_id int,
        servicepoint_id varchar(25),
        dln int,
        channel smallint,
        premise_id int,
        rate_category varchar(20),
        read_starttime varchar(19),
        read_endtime varchar(19),
        read_date int,
        read_hour tinyint,
        read_offset varchar(5),
        utc_endtime varchar(19),
        config_date_from varchar(19),
        config_date_to varchar(19),
        read_length int,
        read_status smallint,
        read_group bigint,
        read_value float,
        channel_unit varchar(20),
        meter_number varchar(20),
        multiplier float,
        kwh_multiplier float,
        recording_device varchar(25),
        billing_system_cycle varchar(5),
        meter_program varchar(20),
        workbin int,
        channel_type varchar(1),
        channel_set_node_group varchar(50),
        dw_created_dt varchar(19),
        dw_modified_dt varchar(27),
        read_month int
    ) as T

GO


CREATE VIEW v_reading_interval_long
AS
SELECT
    read_id,
    servicepoint_id,
    dln,
    premise_id,
    channel,
    read_date,
    read_length,
    read_interval = 'kw' + CONVERT(varchar(5), (read_hour*60 + DATEPART(minute, read_starttime)) / 15 + 1),
    read_value = read_value * kwh_multiplier,
    read_month
FROM
    OPENROWSET(BULK '/reading_spc/read_month=201611/*.parquet',
        DATA_SOURCE = 'DWH',
        FORMAT ='parquet'
    ) AS T

GO

