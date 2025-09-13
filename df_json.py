import pandas,time
from pyflink.table import (TableEnvironment, EnvironmentSettings, Schema,
                           DataTypes, FormatDescriptor, TableDescriptor)
from pyflink.table.expressions import (col,lit)
"""
Demo này sẽ chuyển đổi một file .csv của 3 ba cổ phiếu khác nhau trên YahooFinance trong 1 tháng sang file .json.
Trong file input, mỗi row là một ngày chứa tất cả thông tin của ba cổ phiếu, nên mục đích của demo sẽ chuyển sang
file .json thành các thông tin riêng của từng chứng khoán trong từng ngày.
Demo này sử dụng Flink.
"""

# Khởi tạo môi trường làm việc
env_setting = EnvironmentSettings.in_batch_mode()
table_env = TableEnvironment.create(env_setting)


# Tạo source 
source_descriptor = TableDescriptor.for_connector("filesystem") \
    .schema(Schema.new_builder()
        .column("Datetime", DataTypes.STRING())
        .column("Close_AAL", DataTypes.FLOAT())
            .column("Close_NVDA", DataTypes.FLOAT())
            .column("Close_OPEN", DataTypes.FLOAT())
            .column("High_AAL", DataTypes.FLOAT())
            .column("High_NVDA", DataTypes.FLOAT())
            .column("High_OPEN", DataTypes.FLOAT())
            .column("Low_AAL", DataTypes.FLOAT())
            .column("Low_NVDA", DataTypes.FLOAT())
            .column("Low_OPEN", DataTypes.FLOAT())
            .column("Open_AAL", DataTypes.FLOAT())
            .column("Open_NVDA", DataTypes.FLOAT())
            .column("Open_OPEN", DataTypes.FLOAT())
            .column("Volume_AAL", DataTypes.BIGINT())
            .column("Volume_NVDA", DataTypes.BIGINT())
            .column("Volume_OPEN", DataTypes.BIGINT())
            .build()) \
    .format(FormatDescriptor.for_format('json')
        .option('field-delimiter',',')
        .option('ignore-parse-errors',"true")
        .option("csv.ignore-first-line","true")
        .build()) \
    .option('path','/tmp/pyflink_build/stock_1mo.csv') \
    .build()

table_env.create_temporary_table("stock_source",source_descriptor)

# Tạo sink
schema = Schema.new_builder() \
    .column("datetime",DataTypes.STRING())\
    .column("ticker_name", DataTypes.STRING())\
    .column("close",DataTypes.FLOAT())\
    .column("high", DataTypes.FLOAT())\
    .column("low", DataTypes.FLOAT())\
    .column("open", DataTypes.FLOAT())\
    .column("volume", DataTypes.BIGINT())\
    .build()

sink_description = TableDescriptor.for_connector("filesystem")\
    .schema(schema=schema)\
    .format(FormatDescriptor.for_format('json').build())\
    .option('path','stock_output.json')\
    .build()
table_env.create_temporary_table("stock_sink",sink_description)

table = table_env.from_path("stock_source")
table = table.select(col("Datetime").alias('datetime'),
                     lit('AAL').alias('ticker_name'),
                     col("Close_AAL").alias('close'),
                     col("High_AAL").alias('high'),
                     col("Low_AAL").alias('low'),
                     col("Open_AAL").alias('open'),
                     col('Volume_AAL').alias('volume')
    ).union_all(
        table.select(col("Datetime"),
                     lit('NVDA'),
                     col("Close_NVDA"),
                     col("High_NVDA"),
                     col("Low_NVDA"),
                     col("Open_NVDA"),
                     col('Volume_NVDA'))
    ).union_all(
        table.select(col("Datetime"),
                     lit('OPEN'),
                     col("Close_OPEN"),
                     col("High_OPEN"),
                     col("Low_OPEN"),
                     col("Open_OPEN"),
                     col('Volume_OPEN'))       
    )

table.execute_insert("stock_sink").wait()
time.sleep(45)
