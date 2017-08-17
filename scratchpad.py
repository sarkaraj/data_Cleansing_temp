# # from pyspark import *
# # from pyspark.sql import *
# #
# #
# # conf = SparkConf().setMaster("local[*]").setAppName("temp")
# # sc = SparkContext(conf=conf)
#
# from subprocess import call
#
#
# def run_command(command):
#     command_array = command.split(" ")
#     call(command_array)
#
#
# def validate_virtual_env():
#     packages = ["virtualenv", "pandas"]
#
#     for package in packages:
#         try:
#             import package
#             # print "package imported successfully"
#         except ImportError, e:
#             run_command("pip install " + package)
#
#
# def make_dirs(location):
#     dirs = location.split("/")[1:]
#     current_loc = "/"
#
#     for dir in dirs:
#         try:
#             command = "mkdir " + current_loc + dir
#             run_command(command)
#         except OSError as exception:
#             print exception
#
#
# def initiate_venv(location="/tmp/cona_pyspark_venv/"):
#     # Creating virtual environments
#     current_project_venv_environ = location + "cona_suggestive_order"
#     venv_init_command = "virtualenv " + current_project_venv_environ
#     run_command(venv_init_command)
#
#     # Activate virtual environment
#     venv_actvt_command = "source " + current_project_venv_environ + "/bin/activate"
#     run_command(venv_actvt_command)
#
#     # Install required libraries
# #
# validate_virtual_env()
# #
# # blah = data_grp.limit(1)
# # blah.cache()
# #
# # # print blah.printSchema
# #
# # # print\
# # temp_df = \
# #     blah \
# #         .select(explode(col('data')).alias('data')) \
# #     # .map(lambda line : line.data.split("\t"))\
# # .withColumn('quantity', split(col('data'), "\t").getItem(0).cast(IntegerType())) \
# #     .withColumn('bill_date',
# #                 from_unixtime(unix_timestamp(split(col('data'), "\t").getItem(1), "yyyyMMdd")).cast(DateType())) \
# #     .withColumn('dlvry_lag', split(col('data'), "\t").getItem(2)) \
# #     .withColumn('city', split(col('data'), "\t").getItem(3)) \
# #     .withColumn('region', split(col('data'), "\t").getItem(4)) \
# #     .drop(col('data'))  # /
# # # .count()
# # # .collect()
# # # .printSchema
# #
# #
# # # temp_df.toPandas()
