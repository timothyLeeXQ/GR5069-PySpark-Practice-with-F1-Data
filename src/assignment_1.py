# Databricks notebook source
# MAGIC %md
# MAGIC # Assignment 1: Analysing F1 Data hosted in AWS S3 using Databricks and PySpark
# MAGIC 
# MAGIC **Objectives:**
# MAGIC 
# MAGIC * Explore and gain familiarity with F1 Dataset and PySpark
# MAGIC * Answer simple questions about the F1 Data:
# MAGIC   + What was the average time each driver spent at the pit stop for each race?
# MAGIC   + Rank the average time spent at the pit stop in order of who won each race
# MAGIC   + Insert the missing code(e.g: ALO for Alonso) for drivers based on the 'drivers' dataset
# MAGIC   + Who is the youngest and oldest driver for each race? Create a new column called “Age”
# MAGIC   + For a given race, which driver has the most wins and losses?
# MAGIC   + Continue exploring the data by answering your own question.

# COMMAND ----------

# MAGIC %md ## Mount S3 Bucket to obtain data
# MAGIC 
# MAGIC * Provide AWS Access Keys for authentication
# MAGIC * **DELETE ACCESS KEYS IMMEDIATELY AFTER MOUNTING**
# MAGIC * Provide AWS Bucket Name and mount name
# MAGIC * Expect successful mount - line 8 should produce paths to be used to get data

# COMMAND ----------

ACCESS_KEY = ''
# Encode the Secret Key as that can contain '/'
SECRET_KEY = ''.replace("/", "%2F")
AWS_BUCKET_NAME_RAW = 'ne-gr5069'
MOUNT_NAME_RAW = 'ne-gr5069'

dbutils.fs.mount('s3a://%s:%s@%s' % (ACCESS_KEY, SECRET_KEY, AWS_BUCKET_NAME_RAW),
                 '/mnt/%s' % MOUNT_NAME_RAW)
display(dbutils.fs.ls('/mnt/%s' % MOUNT_NAME_RAW))

# COMMAND ----------

# MAGIC %md ## Mount S3 Bucket to write data

# COMMAND ----------

AWS_BUCKET_NAME_PROC = 'xql2001-gr5069'
MOUNT_NAME_PROC = 'xql2001-gr5069'

dbutils.fs.mount('s3a://%s:%s@%s' % (ACCESS_KEY, SECRET_KEY, AWS_BUCKET_NAME_PROC),
                 '/mnt/%s' % MOUNT_NAME_PROC)
display(dbutils.fs.ls('/mnt/%s' % MOUNT_NAME_PROC))

# COMMAND ----------

# MAGIC %md ## Load relevant packages

# COMMAND ----------

from pyspark.sql.types import DateType, IntegerType, DoubleType
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md ## Define functions to be used throughout

# COMMAND ----------

def cols_to_int_type(df, col_list):
  # :::::::::::: DESCRIPTION
  # This function is used to change a set of columns in a dataframe to an
  # integer type by looping through through columnsprovided as a list
  #
  # Functionalising this because many of the dfs in the F1 data should be
  # integers, but every variable is imported as string
  #
  # ::::::::: INPUTS
  # 1. df - the dataframe with columns to be changed to int. Should be a 
  #    pyspark.sql dataframe object
  # 2. col_list - a list of strings - each the name of a column in the df
  #    that is to be changed to an integer datatype
  #
  # ::::::::: OUTPUT
  # The dataframe entered as an argument, but with the desired columns
  # cast to the datatype Integer
  #
  for colname in col_list:
    df = df.withColumn(colname, df[colname].cast(IntegerType()))
  return(df)

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. What was the average time each driver spent at the pit stop for each race?

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explore relevant tables
# MAGIC 
# MAGIC Tables to explore:
# MAGIC * Pit stops
# MAGIC * Drivers
# MAGIC * Races
# MAGIC 
# MAGIC Check available columns and their datatypes, change datatypes where needed

# COMMAND ----------

df_pitstops = spark.read.csv('/mnt/ne-gr5069/raw/pit_stops.csv',header=True)
display(df_pitstops)

# COMMAND ----------

list_pitstop_int_colname = ['raceId', 'driverId', 'stop', 'lap', 'milliseconds']
df_pitstops = cols_to_int_type(df_pitstops, list_pitstop_int_colname)

df_pitstops = df_pitstops.withColumn('duration',
                                     df_pitstops['duration'].cast(DoubleType())
                                    )

#Leaving time is for now, can't seem to get it to handle milliseconds


display(df_pitstops)

# COMMAND ----------

df_drivers = spark.read.csv('/mnt/ne-gr5069/raw/drivers.csv',header=True)
display(df_drivers)

# COMMAND ----------

# Ignoring driver number to avoid having to deal with \N values
# Driver num can stay as strings - they're basically FIA-issued IDs anyway
df_drivers = df_drivers.withColumn('driverId',
                                   df_drivers['driverId'].cast(IntegerType())
                                  )
df_drivers = df_drivers.withColumn('dob', df_drivers['dob'].cast(DateType()))

display(df_drivers)

# COMMAND ----------

# MAGIC %md
# MAGIC # An Aside: 3. Insert the missing code(e.g: ALO for Alonso) for drivers based on the 'drivers' dataset
# MAGIC 
# MAGIC **This is being addressed here as an aside from Q1.**
# MAGIC 
# MAGIC I think it makes more sense here, as
# MAGIC  1. I am cleaning the driver data in this segment already
# MAGIC  2. Driver code is used in both questions 1 and 2, and it would be nice to have those
# MAGIC  values filled in when presenting the tables for Q1 and Q2
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ## Notes
# MAGIC 
# MAGIC * Driver code is missing for most drivers beyond current drivers
# MAGIC * With a few exceptions, driver code is the first 3 letters of the 
# MAGIC driver's last name
# MAGIC * Crucially it is important not to replcae the existing values as
# MAGIC they contain notable exceptions - MSC for Michael Schumacher, because
# MAGIC his brother is also an F1 racer
# MAGIC * Some exceptions:
# MAGIC   - Ralph Schumacher is RSC (see above)
# MAGIC   - Accented characters should not be accented
# MAGIC   - van der Garde is VDG
# MAGIC   - Rossi is RSI, since Rosberg is already ROS
# MAGIC   - Montoya (MOY) and Montagny (MOT) in older series
# MAGIC You would need to know these exceptions and handle them one by one - I
# MAGIC found them on Reddit. None of these, or any others undiscovered, have
# MAGIC been addressed
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC 1. Get the first 3 characters of drivers' last names
# MAGIC 2. Make them upper case
# MAGIC 3. Remove \N from current codes with empty string
# MAGIC 4. Merge current code and new code columns
# MAGIC 5. Keep only the first 3 characters of the new code column to ensure
# MAGIC   - Drivers with their code already present keep it
# MAGIC   - Drivers with missing codes get the replacement codes

# COMMAND ----------

df_drivers = df_drivers.withColumn('last_name_sub', F.substring(F.col('surname'),0, 3))\
  .withColumn('last_name_sub', F.upper(F.col('last_name_sub')))\
  .withColumn('code', F.regexp_replace('code', '\\\\N', ''))\
  .withColumn('code', F.concat(F.col('code'), F.col('last_name_sub')))\
  .withColumn('code', F.substring(F.col('code'),0, 3))\

  
display(df_drivers)
df_drivers.coalesce(1).write.csv('/mnt/xql2001-gr5069/processed/assignment_1/question3_df_drivers.csv')

# COMMAND ----------

# MAGIC %md 
# MAGIC # Back to Question 1

# COMMAND ----------

df_races = spark.read.csv('/mnt/ne-gr5069/raw/races.csv',header=True)
display(df_races)

# COMMAND ----------

# Round is the position of the race in that year's calendar

list_races_int_colname = ['raceId', 'year', 'round', 'circuitId']
df_races = cols_to_int_type(df_races, list_races_int_colname)

df_races = df_races.withColumn('date', df_races['date'].cast(DateType()))
#Again passing on time

display(df_races)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Answer the question (question 1):
# MAGIC 
# MAGIC 1. Group by driverId and raceId in the pitstops dataset
# MAGIC 2. Find the average pitting time in ms by averaging milliseconds
# MAGIC 3. Join with drivers and races datasets on their IDs to get details on drivers and races
# MAGIC 4. Pick only relevant columns and rename them for readability
# MAGIC 5. Present - driver info, race details, and average pit time

# COMMAND ----------

df_avg_pit_time = df_pitstops.groupby('driverId', 'raceId')\
  .agg(F.avg('milliseconds'))\
  .join(df_races.select('raceId', 'name', 'year'), on = ['raceId'])\
  .join(df_drivers.select('driverId', 'forename', 'surname', 'code'), on = ['driverId'])\
  .select(F.col('driverId'),
          F.col('raceId'),
          F.col('code').alias('driver_code'),
          F.col('forename').alias('driver_first_name'),
          F.col('surname').alias('driver_last_name'),
          F.col('name').alias('race_name'),
          F.col('year').alias('season'),
          F.col('avg(milliseconds)').alias('average_pit_time(ms)'))

display(df_avg_pit_time.select('driver_code',
                               'driver_first_name',
                               'driver_last_name',
                               'race_name',
                               'season',
                               'average_pit_time(ms)'
                              )
       )
df_avg_pit_time.coalesce(1).write.csv('/mnt/xql2001-gr5069/processed/assignment_1/question1_df_avg_pit_time.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # 2. Rank the average time spent at the pit stop in order of who won each race

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##Explore relevant tables
# MAGIC 
# MAGIC Look at the results table to get finishing positions
# MAGIC 
# MAGIC Check available columns and their datatypes, change datatypes where needed

# COMMAND ----------

df_results = spark.read.csv('/mnt/ne-gr5069/raw/results.csv',header=True)
display(df_results)

# COMMAND ----------

list_results_int_colname = ['resultId',
                            'raceId',
                            'driverId',
                            'constructorId',
                            'number',
                            'grid',
                            'position',
                            'positionOrder',
                            'points',
                            'laps',
                            'milliseconds',
                            'fastestLap',
                            'rank',
                            'statusId'
                           ]
df_results = cols_to_int_type(df_results, list_results_int_colname)
  
display(df_results)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Answer the question (question 2):
# MAGIC 
# MAGIC I assume "in order of who won each race" refers to "in order of their finishing positions".
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC 1. Join with averaged pit times per race, driver on their IDs
# MAGIC 2. Order by race and position order
# MAGIC 3. Pick only relevant columns and rename them for readability
# MAGIC 4. Present - joined df ordered by race, then position

# COMMAND ----------

# Using positionOrder as the relevant column for finishing position
# position and positionText do note rank people who dropped out of the race
# which is very common in F1 races.

df_avg_pittimes_driver_race_pos = df_avg_pit_time\
  .join(df_results.select('raceId', 'driverId', 'positionOrder'),
        on = ['driverId', 'raceId'])\
  .orderBy('raceId', 'positionOrder')

display(df_avg_pittimes_driver_race_pos.select('driver_code',
                                               'driver_first_name',
                                               'driver_last_name',
                                               'race_name',
                                               'season',
                                               'positionOrder',
                                               'average_pit_time(ms)'
                                              )
       )

df_avg_pittimes_driver_race_pos.coalesce(1).write.csv('/mnt/xql2001-gr5069/processed/assignment_1/question2_df_avg_pittimes_driver_race_pos.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. Who is the youngest and oldest driver for each race? Create a new column called “Age”

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC I assume we don't just want their current age, but their age
# MAGIC at the time of the race
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC 1. Join in driver dob from the drivers data
# MAGIC 2. Join in race date from the race data
# MAGIC 3. Calculate age at race time
# MAGIC 4. Present joined df in full, in order of race, then age

# COMMAND ----------

df_driver_race_pos_age = df_avg_pittimes_driver_race_pos\
  .join(df_drivers.select('driverId', 'dob'), on = ['driverId'])\
  .join(df_races.select('raceId', 'date'), on = ['raceId'])\
  .withColumn("age", F.datediff(F.col('date'),F.col("dob"))/365.25)

df_driver_race_pos_age = df_driver_race_pos_age\
  .withColumn("age", df_driver_race_pos_age["age"].cast(IntegerType()))\
  .orderBy('raceId', 'age')

display(df_driver_race_pos_age)
df_driver_race_pos_age.coalesce(1).write.csv('/mnt/xql2001-gr5069/processed/assignment_1/question4_df_driver_race_pos_age.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # 5. For a given race, which driver has the most wins and losses?

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ## Notes
# MAGIC 
# MAGIC ### On races: 
# MAGIC I assume "race" here refers to a particular circuit, not a specific race
# MAGIC (i.e. Most wins and losses in the Australian GP, not 2011 Australian GP).
# MAGIC Otherwise, each driver can only win each race once, since it is a unique instance
# MAGIC 
# MAGIC ### On wins:
# MAGIC Technically, winning a race could mean many things. It could mean first place,
# MAGIC beating the driver who is close to you in the championship, not crashing, or
# MAGIC just scoring points for the championship (points are awarded for the first
# MAGIC 10 finishers in each race), and aggregated over the whole season to see which
# MAGIC driver (and constructor) won.
# MAGIC 
# MAGIC If we're separating wins from losses we will need to define a win and a loss.
# MAGIC I'd say getting points for the race is a good definition
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC 1. Join circuitId data from races (don't trust race name enough)
# MAGIC 2. Join points data from results (positionOrder is already in the data but
# MAGIC might be poor indicator of winning - a race where everyone crashed should
# MAGIC have no winner but would have 10 winners if we used positionOrder)
# MAGIC 3. Create New col with wins and losses
# MAGIC 4. Groupby and count wins
# MAGIC 
# MAGIC Then: See below

# COMMAND ----------

df_drivers_results_races = df_driver_race_pos_age\
  .join(df_races.select('raceId', 'circuitId'), on = ['raceId'])\
  .join(df_results.select('raceId', 'driverId', 'points'), on = ['raceId', 'driverId'])

df_races_win_loss = df_drivers_results_races\
  .withColumn('win',
              F.when(F.col('points') > 0, 'win')\
              .otherwise('lose')
             )\
  .groupBy('driverId', 'circuitId')\
  .pivot('win')\
  .count()\
  .na.fill(0)

display(df_races_win_loss)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC The table above doesn't precisely give us who won/lost the most races at each circuit.
# MAGIC 
# MAGIC Next step: Groupby circuit and find the most wins there
# MAGIC 
# MAGIC Then: Join back other details we want, and order them nicely. Display and save.
# MAGIC 
# MAGIC Finally: Repeat for losses.

# COMMAND ----------

df_races_most_wins = df_races_win_loss\
  .groupBy('circuitId')\
  .agg(F.max(F.col('win')).alias('win'))\
  .join(df_races_win_loss, on = ['circuitId', 'win'], how = 'inner')\
  .join(df_drivers_results_races.select('driverId',
                                        'circuitId',
                                        'driver_code',
                                        'driver_first_name',
                                        'driver_last_name',
                                        'race_name'
                                       ),
        on = ['driverId', 'circuitId'],
        how = 'inner')\
  .distinct()
# distinct() used as the previous join somehow created duplicate rows

# Reorder columns
df_races_most_wins = df_races_most_wins.select('circuitId',
                                               'race_name',
                                               'driverId',
                                               'driver_code',
                                               'driver_first_name',
                                               'driver_last_name',
                                               'win'
                                              )\
  .orderBy('race_name', 'win')

display(df_races_most_wins)
df_races_most_wins.coalesce(1).write.csv('/mnt/xql2001-gr5069/processed/assignment_1/question5a_df_races_most_wins.csv')

# COMMAND ----------

df_races_most_lose = df_races_win_loss\
  .groupBy('circuitId')\
  .agg(F.max(F.col('lose')).alias('lose'))\
  .join(df_races_win_loss, on = ['circuitId', 'lose'], how = 'inner')\
  .join(df_drivers_results_races.select('driverId',
                                        'circuitId',
                                        'driver_code',
                                        'driver_first_name',
                                        'driver_last_name',
                                        'race_name'
                                       ),
        on = ['driverId', 'circuitId'],
        how = 'inner')\
  .distinct()
# distinct() used as the previous join somehow created duplicate rows

# Reorder columns
df_races_most_lose = df_races_most_lose.select('circuitId',
                                               'race_name',
                                               'driverId',
                                               'driver_code',
                                               'driver_first_name',
                                               'driver_last_name',
                                               'lose'
                                              )\
  .orderBy('race_name', 'lose')

display(df_races_most_lose)
df_races_most_lose.coalesce(1).write.csv('/mnt/xql2001-gr5069/processed/assignment_1/question5b_df_races_most_lose.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # 6. Continue exploring the data by answering your own question.
# MAGIC 
# MAGIC ## Which constructor clinched pole position most often?
# MAGIC 
# MAGIC Pole position, the first car on the starting grid is awarded to the first place in the qualifying round.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explore relevant tables
# MAGIC 
# MAGIC Tables to explore:
# MAGIC * Qualifying
# MAGIC * Constructors
# MAGIC 
# MAGIC Check available columns and their datatypes, change datatypes where needed

# COMMAND ----------

df_quals = spark.read.csv('/mnt/ne-gr5069/raw/qualifying.csv',header=True)
display(df_quals)

# COMMAND ----------

list_quals_int_colname = ['qualifyId',
                            'raceId',
                            'driverId',
                            'constructorId',
                            'number',
                            'position'
                           ]
df_quals = cols_to_int_type(df_quals, list_quals_int_colname)
  
display(df_quals)

# COMMAND ----------

df_constructors = spark.read.csv('/mnt/ne-gr5069/raw/constructors.csv',header=True)
display(df_constructors)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Answer the question (question 6):
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC 1. Filter df_quals to only keep 1st place finishes (i.e. pole position)
# MAGIC 2. Count the number of times each constructor got pole position
# MAGIC 3. Join with df_constructors for the name
# MAGIC 4. Remove Id (unpresentable) and present, in order of the number of
# MAGIC pole positions clinched

# COMMAND ----------

df_quals_constructors_pole = df_quals\
  .filter(F.col('position') == 1)\
  .groupBy('constructorId')\
  .agg(F.count('position'))\
  .join(df_constructors.select('constructorId','name'),
        on = ['constructorId'])\
  .select(F.col('name').alias('constructor_name'),
          F.col('count(position)').alias('pole_positions_won'))\
  .orderBy('pole_positions_won', ascending = False)


display(df_quals_constructors_pole)
df_quals_constructors_pole.coalesce(1).write.csv('/mnt/xql2001-gr5069/processed/assignment_1/question6_df_quals_constructors_pole.csv')

# COMMAND ----------

#Unmount bucket when done
dbutils.fs.unmount("/mnt/%s" % MOUNT_NAME_RAW)
dbutils.fs.unmount("/mnt/%s" % MOUNT_NAME_PROC)
