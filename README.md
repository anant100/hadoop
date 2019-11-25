# hadoop
Configuring Hadoop API  ||  Work with Hadoop HDFS API  ||  Data Enrichment with Streaming data  ||  Understand how to change a pipeline to work with multiple data sources

# Data set

I use STM GTFS data set. In this project, we continue the practice of enrichment (the most common data transformation task to do). To download the dataset, visit http://www.stm.info/en/about/developers

# Description 

## Preparation

Put the files of trip, route and calendar on HDFS under /user/[group name]/[your name]/stm/ where [group name] is your class group for example summer2019 and [your name] is a name of your choice all in lowercase without space or any non-alphabetic character.

## Enricher

Create project called hadoop to enrich route with trip based on route_id and then enrich the result with Calendar based on service_id. At the end, you should write the final result in a CSV file with a proper header.

Apply required changes to read files from HDFS and write the result back to the HDFS. 

## The followings are applied in project:

•	You may read all the files in memory once and do the join as these files are small. It is encouraged to use STREAMING as a bonus.

•	The destination directory on HDFS is /user/[group name]/[your name]/course3/

•	Delete the destination directory and its content if exists at the beginning of the project

•	There should not be any manual work needed and every time you run your project, you get the same result

•	You could write intermediate data back on HDFS for debugging but you shouldn’t keep them. At the end of the project, there is only file in the destination directory, and it is the final result. Hence, you will have an intermediate class as well.

a.	Trip

b.	Calendar

c.	Route

d.	EnrichedTrip (final class)

# Bonus
Enrichment with Streaming data sets

# Output 
Calendar & Route ---> Memory ---> DataStream (Trip) + Memory ---> EnrichedTrip
