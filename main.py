import pyspark
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import col,lower,trim,count
import logging
import duplicates

logger = logging.getLogger('compare_df_parallel_demo')
formatter = logging.Formatter("%(asctime)s %(levelname)s \t[%(filename)s:%(lineno)s - %(funcName)s()] %(message)s")
logger.setLevel(logging.INFO)
io_log_handler = logging.StreamHandler()
logger.addHandler(io_log_handler)

#Create session
sc = SparkContext()
spark = SparkSession(sc)
sqlContext = SQLContext(sc)
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

#Read mssi table
df1 = sqlContext.read.load('/user/hive/warehouse/enr.db/mssi_td_aplicacion')
#df1 = SQLContext.sql("SELECT * FROM parquet.'/user/hive/warehouse/enr.db/mssi_td_aplicacion'")
df1.show()

#Read consume table
df2 = sqlContext.read.load('/user/hive/warehouse/con.db/dmcrp001_td_aplicacion/aud_fecha_datos=20220421')
df2.show()

grain_cols = ['cod_aplicacion']

#grainDuplicateCheck(df2, grain_cols, logger)

function.compareGrain(df1, df2, grain_cols, logger)