# Create ML Models with BigQuery ML: Challenge Lab (GSP341)

![Google Cloud](https://img.shields.io/badge/Google_Cloud-4285F4?logo=googlecloud&logoColor=white)
![Lab](https://img.shields.io/badge/Lab-GSP341-34A853)
![BigQuery ML](https://img.shields.io/badge/BigQuery_ML-669DF6?logo=googlebigquery&logoColor=white)

Reference solution for the Google Cloud Skills Boost challenge lab **Create ML Models with BigQuery ML** (GSP341).

## 📖 Overview

Using the **Austin Bikeshare** public dataset, you train two linear regression models that predict trip duration:

| Model | Features |
|---|---|
| `location_model` | start station, hour, day of week, station address |
| `subscriber_model` | start station, hour, subscriber type |

You then compare them on 2019 data and use the better one to predict average trip length at the busiest station.

## 🧰 Services used

BigQuery · BigQuery ML (`CREATE MODEL`, `ML.EVALUATE`, `ML.PREDICT`) · BigQuery public datasets

## 🚀 Solution

### Task 1: Create a dataset for the models

```bash
bq mk austin
```

### Task 2: Train the location model

```sql
CREATE OR REPLACE MODEL austin.location_model
OPTIONS (model_type = 'linear_reg', input_label_cols = ['duration_minutes']) AS
SELECT
  start_station_name,
  EXTRACT(HOUR      FROM start_time) AS start_hour,
  EXTRACT(DAYOFWEEK FROM start_time) AS day_of_week,
  duration_minutes,
  address AS location
FROM `bigquery-public-data.austin_bikeshare.bikeshare_trips` AS trips
JOIN `bigquery-public-data.austin_bikeshare.bikeshare_stations` AS stations
  ON trips.start_station_name = stations.name
WHERE EXTRACT(YEAR FROM start_time) = 2018
  AND duration_minutes > 0;
```

### Task 3: Train the subscriber model

```sql
CREATE OR REPLACE MODEL austin.subscriber_model
OPTIONS (model_type = 'linear_reg', input_label_cols = ['duration_minutes']) AS
SELECT
  start_station_name,
  EXTRACT(HOUR FROM start_time) AS start_hour,
  subscriber_type,
  duration_minutes
FROM `bigquery-public-data.austin_bikeshare.bikeshare_trips`
WHERE EXTRACT(YEAR FROM start_time) = 2018;
```

### Task 4: Evaluate both models on 2019 data

```sql
-- location_model
SELECT SQRT(mean_squared_error) AS rmse, mean_absolute_error
FROM ML.EVALUATE(MODEL austin.location_model, (
  SELECT
    start_station_name,
    EXTRACT(HOUR      FROM start_time) AS start_hour,
    EXTRACT(DAYOFWEEK FROM start_time) AS day_of_week,
    duration_minutes,
    address AS location
  FROM `bigquery-public-data.austin_bikeshare.bikeshare_trips` AS trips
  JOIN `bigquery-public-data.austin_bikeshare.bikeshare_stations` AS stations
    ON trips.start_station_name = stations.name
  WHERE EXTRACT(YEAR FROM start_time) = 2019
));

-- subscriber_model
SELECT SQRT(mean_squared_error) AS rmse, mean_absolute_error
FROM ML.EVALUATE(MODEL austin.subscriber_model, (
  SELECT
    start_station_name,
    EXTRACT(HOUR FROM start_time) AS start_hour,
    subscriber_type,
    duration_minutes
  FROM `bigquery-public-data.austin_bikeshare.bikeshare_trips`
  WHERE EXTRACT(YEAR FROM start_time) = 2019
));
```

### Task 5: Predict average trip length at the busiest station

Find the busiest station in 2019:

```sql
SELECT start_station_name, COUNT(*) AS trips
FROM `bigquery-public-data.austin_bikeshare.bikeshare_trips`
WHERE EXTRACT(YEAR FROM start_time) = 2019
GROUP BY start_station_name
ORDER BY trips DESC
LIMIT 1;
```

Predict the average single-trip duration there. At the time of the lab, the busiest station was `21st & Speedway @PCL`.

```sql
SELECT AVG(predicted_duration_minutes) AS average_predicted_trip_length
FROM ML.PREDICT(MODEL austin.subscriber_model, (
  SELECT
    start_station_name,
    EXTRACT(HOUR FROM start_time) AS start_hour,
    subscriber_type,
    duration_minutes
  FROM `bigquery-public-data.austin_bikeshare.bikeshare_trips`
  WHERE EXTRACT(YEAR FROM start_time) = 2019
    AND subscriber_type = 'Single Trip'
    AND start_station_name = '21st & Speedway @PCL'
));
```

> Lab versions differ in the model names, the evaluation year and the subscriber type they ask for. Match the values in your instructions.

---

## ⚠️ Notes

- Resource names, zones, regions and ports are generated per lab session. Always use the values shown in **your** lab instructions.
- These steps were written when the lab was live. Google revises labs over time, so the current version may differ.

## 📌 Disclaimer

This repository is a study reference. Attempt the lab on your own first, then compare approaches. It is not affiliated with or endorsed by Google.

## 👤 Author

**Girish Sharma** · Cloud & DevOps Engineer · [GitHub](https://github.com/codewithgarry) · [LinkedIn](https://www.linkedin.com/in/codewithgarry/)
