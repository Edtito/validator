import sys
import itertools
from prettytable import PrettyTable
import multiprocessing as ms
from multiprocessing.pool import ThreadPool

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

def renameDfColumns(df, prefix):
	"""
	Function to rename Data Frame columns with given prefix
	Arguments:
		df {pyspark.sql.dataframe.DataFrame} -- Pyspark DataFrame
		prefix {str} -- prefix string
	"""
	columns = df.columns
	for column in columns:
		df = df.withColumnRenamed(column, "{0}_{1}".format(column, prefix))
	return df

def compareDf(src_df, tar_df, grain, logger, diff_limit=10,):
	"""
	Function To Compare two Data Frames based on grain
	Arguments:
		src_df {pyspark.sql.dataframe.DataFrame} -- Source Pyspark DataFrame
		tar_df {pyspark.sql.dataframe.DataFrame} -- Target Pyspark DataFrame
		grain {list} -- list of string containing grain
		logger {logger} -- logger objects
	Keyword Arguments:
		diff_limit {int} --  Number of mismatches to be returned (default: {10})
	Returns:
		{list} -- format is [column_name{str}, test_status{str}, diff_count{int}, diff_values{str}]
	"""

	try:
		
		logger.info("Checking column data...\n")
		
		columns = src_df.columns
		#rename columns with src and tar prefix
		src_df = renameDfColumns(src_df, 'src')
		tar_df = renameDfColumns(tar_df, 'tar')

		#prepare join condition on grain
		join_condition = [ trim(lower(col(x+'_src'))).eqNullSafe(trim(lower(col(x+'_tar')))) for x in grain ]

		#join src and tar dataframes
		main_df = src_df.join(tar_df, join_condition, how='inner')
		
		#for each column in dataframe comapre src and tar values
		for cols in list(set(columns)-set(grain)):
			#get diff
			if main_df.select(cols+'_src').dtypes[-1][-1] == 'double':
				diff = main_df.select([x+'_src' for x in grain]+[cols+'_src', cols+'_tar']).filter((col(cols+'_src')-col(cols+'_tar') >=1) | (col(cols+'_src')-col(cols+'_tar') <=-1) | ( ((col(cols+'_src').isNotNull()) & (col(cols+'_tar').isNull())) | ((col(cols+'_src').isNull()) & (col(cols+'_tar').isNotNull())) ) )
			else:
				diff = main_df.select([x+'_src' for x in grain]+[cols+'_src', cols+'_tar']).filter((col(cols+'_src')!=col(cols+'_tar')) | ( ((col(cols+'_src').isNotNull()) & (col(cols+'_tar').isNull())) | ((col(cols+'_src').isNull()) & (col(cols+'_tar').isNotNull())) ) )
			#get diff count
			diff_count = diff.count()
			if diff_count > 0 :
				sample_diff = diff.limit(diff_limit)
				diff_pdf = sample_diff.toPandas()
				list_diff = diff_pdf.iloc[:].values.tolist()
				header = [x+'_src' for x in grain]+[cols+'_src', cols+'_tar']
				diff_str = pretty_print_table(list_diff, header)['data']	

				logger.error("[FAILED] Source and Target Data is not matching for column : {0} count : {1}".format(cols, diff_count))
				logger.error("{0}".format(diff_str))
			
	except Exception as e:
		exception_message = "message: {0}\nline no:{1}\n".format(str(e),sys.exc_info()[2].tb_lineno)
		print(exception_message)
		logger.error(exception_message)

def compareDFColumn(column, grain, main_df, diff_limit=10):
	"""
	Lambda function to compare dataframe column
	Arguments:
		column {str} -- column name
		grain {list} -- list of string containing grain
		main_df {pyspark.sql.dataframe.DataFrame} -- Pyspark DataFrame which has both source and target df joined
	Returns:
		{list} -- format is [column_name{str}, test_status{str}, diff_count{int}, diff_values{str}]
	"""
	try:
		diff_str = ''
		if main_df.select(column+'_src').dtypes[-1][-1] == 'double':
			diff = main_df.select([x+'_src' for x in grain]+[column+'_src', column+'_tar']).filter((col(column+'_src')-col(column+'_tar') >= 1) | (col(column+'_src')-col(column+'_tar') <=-1) | ( ((col(column+'_src').isNotNull()) & (col(column+'_tar').isNull())) | ((col(column+'_src').isNull()) & (col(column+'_tar').isNotNull())) ) )
		else:
			diff = main_df.select([x+'_src' for x in grain]+[column+'_src', column+'_tar']).filter((col(column+'_src')!=col(column+'_tar')) | ( ((col(column+'_src').isNotNull()) & (col(column+'_tar').isNull())) | ((col(column+'_src').isNull()) & (col(column+'_tar').isNotNull())) ) )
		diff_count = diff.count()
		if diff_count > 0 :
			sample_diff = diff.limit(diff_limit)
			diff_pdf = sample_diff.toPandas()
			list_diff = diff_pdf.iloc[:].values.tolist()
			header = [x+'_src' for x in grain]+[column+'_src', column+'_tar']
			diff_str = pretty_print_table(list_diff, header)['data']
			return [column, 'FAILED', diff_count, diff_str, [header]+list_diff]
		else:
			return [column, 'PASSED', diff_count, diff_str, []]
	except Exception as e:
		exception_message = "message: {0}\nline no:{1}\n".format(str(e),sys.exc_info()[2].tb_lineno)
		print(exception_message)
		return [column, 'FAILED', 9999999, exception_message]

def compareDfParallel(src_df, tar_df, grain, logger, diff_limit=10, parallel_thread_count=4):
	"""
	Function To Compare two Data Frames based on grain and comparision of columns is parallel by running parallel pysparkjobs
	Arguments:
		src_df {pyspark.sql.dataframe.DataFrame} -- Source Pyspark DataFrame
		tar_df {pyspark.sql.dataframe.DataFrame} -- Target Pyspark DataFrame
		grain {list} -- list of string containing grain
		logger {logger} -- logger objects
	Keyword Arguments:
		diff_limit {int} --  Number of mismatches to be returned (default: {10})
	Returns:
		{list} -- format is [column_name{str}, test_status{str}, diff_count{int}, diff_values{str}]
	"""
	try:
		logger.info("Checking column data...\n")

		columns = src_df.columns
		#rename df columns
		src_df = renameDfColumns(src_df, 'src')
		tar_df = renameDfColumns(tar_df, 'tar')

		#prepare join condition based grain
		join_condition = [ trim(lower(col(x+'_src'))).eqNullSafe(trim(lower(col(x+'_tar')))) for x in grain ]

		#join src and tar dataframes
		main_df = src_df.join(tar_df, join_condition, how='inner')

		#get list of columns to compare
		columns_test = list(set(columns)-set(grain))
		
		#initiate thread pool based on number of cores
		#pool = ThreadPool(ms.cpu_count())
		pool = ThreadPool(parallel_thread_count)

		#start the parallel comparision jobs
		res = pool.starmap(compareDFColumn, zip(columns_test,itertools.repeat(grain),itertools.repeat(main_df),itertools.repeat(diff_limit)))
		pool.close()
		pool.join()

		#print all mismatch
		for cols,status,diff_count,mismatch, mismatch_list in res:
			if diff_count>0:
				logger.error("[FAILED] Source and Target Data is not matching for column : {0} count : {1}".format(cols, diff_count))
				logger.error("{0}".format(mismatch))

	except Exception as e:
		exception_message = "message: {0}\nline no:{1}\n".format(str(e),sys.exc_info()[2].tb_lineno)
		logger.error(exception_message)


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