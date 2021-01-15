## Create ML Models with BigQuery ML: Challenge Lab:

solutionMadeBy{
Prakash_Foundation();
Online_inter_college(); 
}


######################################################################################################
Task 1: Create a dataset to store your machine learning models

It can be called any name to pass the test but for the remaining instructions to work use `austin` as the dataset name.


bq mk austin





######################################################################################################
Task 2: Create a forecasting BigQuery machine learning model.

Cloud Shell
export PROJECT_ID=DEVSHELL_PROJECT_ID


bq query --use_legacy_sql=false "CREATE OR REPLACE MODEL austin.austin_1 OPTIONS(input_label_cols=['duration_minutes'], model_type='linear_reg') AS SELECT duration_minutes, location, start_station_name, CAST(EXTRACT(dayofweek FROM start_time) AS STRING) as dayofweek, CAST(EXTRACT(hour FROM start_time) AS STRING) AS hourofday, FROM \`bigquery-public-data.austin_bikeshare.bikeshare_trips\` AS trips JOIN \`bigquery-public-data.austin_bikeshare.bikeshare_stations\` AS stations ON trips.start_station_id = stations.station_id WHERE EXTRACT(year from start_time) = 2018"



#####################################################################################################3
## Task 3: Create the second machine learning model. 




Cloud Shell
export PROJECT_ID=DEVSHELL_PROJECT_ID


bq query --use_legacy_sql=false "CREATE OR REPLACE MODEL austin.austin_2 OPTIONS(input_label_cols=['duration_minutes'], model_type='linear_reg') AS SELECT duration_minutes, subscriber_type, start_station_name, CAST(EXTRACT(hour FROM start_time) AS STRING) AS hourofday FROM \`bigquery-public-data.austin_bikeshare.bikeshare_trips\` AS trips WHERE EXTRACT(year from start_time) = 2018"


@###################################################################################################3333

Task 4: Evaluate the two machine learning models.


Cloud Shell\


bq query --use_legacy_sql=false "SELECT SQRT(mean_squared_error)AS rmse, mean_absolute_error FROM ML.EVALUATE(MODEL austin.austin_1, (SELECT duration_minutes, location, start_station_name, CAST(EXTRACT(dayofweek FROM start_time) AS STRING) as dayofweek, CAST(EXTRACT(hour FROM start_time) AS STRING) AS hourofday, FROM \`bigquery-public-data.austin_bikeshare.bikeshare_trips\` AS trips JOIN \`bigquery-public-data.austin_bikeshare.bikeshare_stations\` AS stations ON trips.start_station_id = stations.station_id WHERE EXTRACT(year from start_time) = 2019))"


bq query --use_legacy_sql=false "SELECT SQRT(mean_squared_error)AS rmse, mean_absolute_error FROM ML.EVALUATE(MODEL austin.austin_2, (SELECT duration_minutes, subscriber_type, start_station_name, CAST(EXTRACT(hour FROM start_time) AS STRING) AS hourofday FROM \`bigquery-public-data.austin_bikeshare.bikeshare_trips\` AS trips WHERE EXTRACT(year from start_time) = 2019))"





###################################################################################################333

Task 5: Use the subscriber type machine learning model to predict average trip durations




Cloud Shell
bq query --use_legacy_sql=false "SELECT start_station_name, avg(duration_minutes) as avg_duration, count(*) as total_trips FROM \`bigquery-public-data.austin_bikeshare.bikeshare_trips\` WHERE EXTRACT(year FROM start_time) = 2019 GROUP BY start_station_name ORDER BY total_trips DESC"




Cloud Shell
bq query --use_legacy_sql=false "SELECT   avg(predicted_duration_minutes),count(duration_minutes) FROM Ml.PREDICT(MODEL austin.austin_2, (SELECT duration_minutes, subscriber_type, start_station_name,  CAST(EXTRACT(hour FROM start_time) AS STRING) AS hourofday FROM \`bigquery-public-data.austin_bikeshare.bikeshare_trips\` AS trips WHERE EXTRACT(year from start_time)=2019 and start_station_name = '21st & Speedway @PCL' and subscriber_type='Single Trip' ))"






********************************************************************************************************8


Task 1:

Create the first ML model using a JOIN between two bike share tables. Again any names will work but keep them as ‘austin_1’ and ‘austin_2’ for the remaining instructions to work. 


BigQuery Console Query Editor

CREATE OR REPLACE MODEL 

  austin.austin_1 

OPTIONS(input_label_cols=['duration_minutes'], model_type='linear_reg') 

AS SELECT

    duration_minutes, 

    location,

    start_station_name, 

    CAST(EXTRACT(dayofweek FROM start_time) AS STRING) as dayofweek, 

    CAST(EXTRACT(hour FROM start_time) AS STRING) AS hourofday,

   FROM 

    `bigquery-public-data.austin_bikeshare.bikeshare_trips` AS trips

   JOIN 

    `bigquery-public-data.austin_bikeshare.bikeshare_stations` AS stations

   ON 

    trips.start_station_id = stations.station_id

   WHERE 

    EXTRACT(year from start_time) = 2018

#############################*******************************#######################################


Task 3:

BigQuery Console Query Editor
CREATE OR REPLACE MODEL 

   austin.austin_2 

OPTIONS(input_label_cols=['duration_minutes'], model_type='linear_reg') 

AS SELECT

     duration_minutes, 

     subscriber_type,

     start_station_name, 

     CAST(EXTRACT(hour FROM start_time) AS STRING) AS hourofday

   FROM 

     `bigquery-public-data.austin_bikeshare.bikeshare_trips` AS trips

   WHERE 

     EXTRACT(year from start_time) = 2018


###############################**************************************##########################3


Task 4:


BigQuery Console Query Editor
SELECT 

  SQRT(mean_squared_error) as rmse, mean_absolute_error 

FROM 

  ML.EVALUATE(MODEL austin.austin_1)


SELECT 

  SQRT(mean_squared_error) as rmse, mean_absolute_error 

FROM 

  ML.EVALUATE(MODEL austin.austin_2)


SELECT 

   SQRT(mean_squared_error)AS rmse, mean_absolute_error 

FROM 

  ML.EVALUATE (MODEL austin.austin_1, (

    SELECT 

       duration_minutes, location, start_station_name, 

       CAST(EXTRACT(dayofweek FROM start_time) AS STRING) as dayofweek, 

       CAST(EXTRACT(hour FROM start_time) AS STRING) AS hourofday,

    FROM 

      `bigquery-public-data.austin_bikeshare.bikeshare_trips` AS trips 

    JOIN 

      `bigquery-public-data.austin_bikeshare.bikeshare_stations` AS stations 

    ON 

      trips.start_station_id = stations.station_id 

    WHERE 

       EXTRACT(year from start_time) = 2019

  ))


SELECT 

  SQRT(mean_squared_error)AS rmse, mean_absolute_error 

FROM 

  ML.EVALUATE(MODEL austin.austin_2, (

    SELECT 

       duration_minutes, subscriber_type, start_station_name, 

       CAST(EXTRACT(hour FROM start_time) AS STRING) AS hourofday 

    FROM 

       `bigquery-public-data.austin_bikeshare.bikeshare_trips` AS trips 

    WHERE 

       EXTRACT(year from start_time) = 2019

  ))
 

#################################****************************##############################33333333
Task 5:

Use the second model, that model (model austin_2 in this case) to to predict the average duration length of all trips from the busiest rental station in 2019 (based on the number of rentals per station in 2019) where the subscriber_type=’Single Trip’


The following query will list busiest stations in descending order. The busiest station for 2019 was “21st & Speedway @PCL”.


BigQuery Console Query Editor
SELECT 

  start_station_name, avg(duration_minutes) as avg_duration, count(*) as total_trips 

FROM 

  `bigquery-public-data.austin_bikeshare.bikeshare_trips` 

WHERE 

  EXTRACT(year FROM start_time) = 2019

GROUP BY 

  start_station_name

ORDER BY 

  total_trips DESC









Then predict trip length.


BigQuery Console Query Editor
SELECT 

  avg(predicted_duration_minutes),count(duration_minutes)

FROM 

  Ml.PREDICT(MODEL austin.austin_2, (

    SELECT

     duration_minutes, 

     subscriber_type,

     start_station_name, 

     CAST(EXTRACT(hour FROM start_time) AS STRING) AS hourofday

    FROM 

       `bigquery-public-data.austin_bikeshare.bikeshare_trips` AS trips

    WHERE 

       EXTRACT(year from start_time) = 2019

       and start_station_name = '21st & Speedway @PCL'

       and subscriber_type='Single Trip'

  ))










