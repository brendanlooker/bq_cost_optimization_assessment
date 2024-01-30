BEGIN
  DECLARE metrics ARRAY<STRUCT<project_id STRING, usage_time TIMESTAMP, avg_wait_time_min NUMERIC>>;

  SET metrics = ARRAY (
   SELECT AS STRUCT 
        project_id, 
        TIMESTAMP_SECONDS(600 * DIV(UNIX_SECONDS(creation_time) + 300, 600)) AS usage_time, 
        CAST(AVG(js.wait_ms_avg)/60000 AS NUMERIC) AS avg_wait_time_min
    FROM `{INFOSCHEMA_PROJECT_NAME}`.`region-{REGION}`.INFORMATION_SCHEMA.JOBS{INFO_TABLE_SUFFIX},
    UNNEST(job_stages) AS js 
    WHERE TIMESTAMP(creation_time) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) 
    GROUP BY project_id, usage_time 
    ORDER BY avg_wait_time_min DESC, project_id
    -- LIMIT 5
  );

  -- Iterate through the metrics array 
  FOR metric IN (SELECT * FROM unnest(metrics)) DO
    IF metric.avg_wait_time_min < {WAIT_TIME_THRESHOLD} THEN 
    
      -- Insert into log table
      INSERT INTO `{DESTINATION_PROJECT_NAME}.{DESTINATION_DATASET_NAME}`.job_metrics_alerts (project_id, log_time, message) 
      VALUES(metric.project_id, CURRENT_TIMESTAMP(),FORMAT('Average Wait Time Threshold exceeded in project: %s, Usage Time: %s, Avg Wait Time: %f', metric.project_id, STRING(metric.usage_time),metric.avg_wait_time_min)); 
    END IF;
  END FOR;
END;
