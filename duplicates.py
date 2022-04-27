import sys
#from prettytable import PrettyTable
from lib import prettytable

from pyspark.sql.functions import col,lower,trim,count

def pretty_print_table(list_data, header):
	"""
	Function to pretty print a table data similar to spark
	Arguments:
		list_data {[str]} -- list of string data of a table
		header {[str]} -- list of string table header
	Returns:
		[type] -- [description]
	"""
	return_dict = {
		'data' : '',
		'success' : True
	}
	try:
		t = PrettyTable(header)
		for data in list_data:
			t.add_row(data)
		return_dict['data']='\n'+str(t)
	except Exception as exc:
		return_dict['data'] = str(exc)
		return_dict['success'] = False
	finally:
		return return_dict

def compareGrain(src_df, tar_df, grain, logger, diff_limit=10): 
	"""
	Function to compare grain columns of tow dataframes
	Arguments:
		src_df {pyspark.sql.dataframe.DataFrame} -- Source Pyspark DataFrame
		tar_df {pyspark.sql.dataframe.DataFrame} -- Target Pyspark DataFrame
		grain {list} -- list of string containing grain
		logger {logger} -- logger
	Keyword Arguments:
		diff_limit {int} -- Number of diff records to be printed (default: {10})
	"""
	try:
		logger.info("Checking Grain...")
		
		df1 = src_df.select( grain )
		df2 = tar_df.select( grain )

		diff1 = df1.subtract(df2)
		diff1_count = diff1.count()

		diff2 = df2.subtract(df1)
		diff2_count = diff2.count()

		if diff1_count==0:
			logger.info("Source - Target : PASSED")

		else:
			diff_pdf = diff1.limit(diff_limit).toPandas()
			list_diff = diff_pdf.iloc[:].values.tolist()
			diff_str = pretty_print_table(list_diff, grain)['data']
			logger.error("Source - Target : FAILED Diff Count : {0}\n{1}".format(diff1_count, diff_str))
			
		if diff2_count==0:
			logger.info("Target - Source : PASSED")

		else:
			diff_pdf = diff2.limit(diff_limit).toPandas()
			list_diff = diff_pdf.iloc[:].values.tolist()
			diff_str = pretty_print_table(list_diff, grain)['data']
			logger.error("Target - Source : FAILED Diff Count : {0}\n{1}".format(diff2_count,diff_str))
		
	except Exception as e:
		exception_message = "message: {0}\nline no:{1}\n".format(str(e),sys.exc_info()[2].tb_lineno)
		logger.error(exception_message)

def grainDuplicateCheck(df, grain, logger, duplicate_limit=10):
	"""
	Function to check grain duplicates
	Arguments:
		df {pyspark.sql.dataframe.DataFrame} -- input dataframe
		grain {list} -- list of grain column names
		logger {logger} -- logger
	Keyword Arguments:
		duplicate_limit {int} -- Number of data samples to be printed (default: {10})
	"""
	try:
		logger.info("Checking for Grain Duplicates in data")

		#run a group by query
		duplicate_df = df\
					.select(grain)\
					.groupBy(grain)\
					.agg(count("*").alias("count"))\
					.filter("count>1")
		
		duplicate_count = duplicate_df.count()
		if duplicate_count > 0 :
			sample_duplicate = duplicate_df.limit(duplicate_limit)
			duplicate_pdf = sample_duplicate.toPandas()
			list_duplicate = duplicate_pdf.iloc[:].values.tolist()
			header = grain+['count']
			duplicate_str = pretty_print_table(list_duplicate, header)['data']

			logger.error("Duplicate Count: {0}\n{1}".format(duplicate_count, duplicate_str))
		
	except Exception as e:
		exception_message = "message: {0}\nline no:{1}\n".format(str(e),sys.exc_info()[2].tb_lineno)
		logger.error(exception_message)