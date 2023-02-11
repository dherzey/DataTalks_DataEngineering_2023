# Week 1 - Homework Part 1

## Question 1:
```bash
docker build --help
```

## Question 2:
```bash
docker run -it --entrypoint=bash python:3.9
```

## Question 3:
```sql
SELECT 
    COUNT(*)
FROM 
    green_taxi_data
WHERE 
    DATE(lpep_pickup_datetime) IN ('2019-01-15')
    AND DATE(lpep_dropoff_datetime) IN ('2019-01-15');
```

## Question 4:
```sql
SELECT 
    DATE(lpep_pickup_datetime)
    , COUNT(*)
FROM 
    green_taxi_data
WHERE 
    DATE(lpep_pickup_datetime) 
    IN ('2019-01-10','2019-01-15','2019-01-18','2019-01-28')
GROUP BY 
    DATE(lpep_pickup_datetime)
ORDER BY 
    COUNT(*);
```

## Question 5:
```sql
SELECT 
    passenger_count
    , COUNT(*)
FROM 
    green_taxi_data
WHERE 
    DATE(lpep_pickup_datetime) IN ('2019-01-01')
    AND passenger_count IN (2,3)
GROUP BY 
    passenger_count;
```

## Question 6:
```sql
SELECT 
    zDO."Zone"
    , SUM(t."tip_amount")
FROM 
    green_taxi_data t 
	JOIN zones zDO
	ON t."DOLocationID" = zDO."LocationID"
	JOIN zones zPU 
	ON t."PULocationID" = zPU."LocationID"
WHERE 
    zPU."Zone"='Astoria'
GROUP BY 
    zDO."Zone"
ORDER BY 
    SUM(t."tip_amount") DESC;
```