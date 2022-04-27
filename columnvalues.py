import sys
from duplicate_grain_check import pretty_print_table

from pyspark.sql.functions import col,lower,trim,count

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