BEGIN
  DECLARE metrics ARRAY<STRUCT<project_id STRING, job_id STRING, job_start TIMESTAMP, execution_run_time INT64>>;

  SET metrics = ARRAY (
   SELECT AS STRUCT 
        project_id, 
        job_id,
        start_time as job_start,
        timestamp_diff(current_timestamp(), start_time, second) as execution_run_time
    FROM `{INFOSCHEMA_PROJECT_NAME}`.`region-{REGION}`.INFORMATION_SCHEMA.JOBS{INFO_TABLE_SUFFIX}
    WHERE timestamp_diff(current_timestamp(), start_time, second) > 3600
    AND   end_time is null
  );

  -- Iterate through the metrics array 
  FOR metric IN (SELECT * FROM unnest(metrics)) DO
    
      -- Insert into log table
      INSERT INTO `{DESTINATION_PROJECT_NAME}.{DESTINATION_DATASET_NAME}`.job_metrics_alerts (project_id, log_time, message) 
      VALUES(metric.project_id, CURRENT_TIMESTAMP(),FORMAT('Execution Run Time Threshold exceeded in project: %s, Job Id: %s, Job Start Time: %s, Execution Run Time (Seconds): %d', metric.project_id, metric.job_id, STRING(metric.job_start),metric.execution_run_time)); 

  END FOR;
END;
