200 -> check json file -> continue process
401 -> Fail immediately with auth message
403 -> Fail immediately with permission message
404 -> Fail immediately with permission message
429 -> Retry through Airflow
5xx -> Retry through Airflow
timeout -> Retry through Airflow
Connection error -> Retry through Airflow
invalid JSON -> Fail with endpoint/status/body details
