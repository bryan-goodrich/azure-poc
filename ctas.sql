
CREATE EXTERNAL TABLE reading_interval
WITH (
    LOCATION = '/reading_interval/read_month=201611/',
    DATA_SOURCE = DWH,
    FILE_FORMAT = ParquetFormat
)
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
