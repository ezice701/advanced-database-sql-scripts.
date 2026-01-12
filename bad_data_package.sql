drop table location_bad_data;
drop table police_officer_bad_data;


-- bad data table for location
create table location_bad_data as
select* from stg_location
where 1=0;

-- bad data table for police officer
create table police_officer_bad_data as
select* 
from stg_police_officer
where 1=0;


CREATE OR REPLACE PACKAGE bad_data_pkg AS

  PROCEDURE identify_location_bad_data;

  PROCEDURE identify_officer_bad_data;

END bad_data_pkg;
/

CREATE OR REPLACE PACKAGE BODY bad_data_pkg AS
  
    --- Private Function to insert into the process log
    FUNCTION log_process_func (
        p_process_name IN VARCHAR2,
        p_target_table IN VARCHAR2,
        p_status IN VARCHAR2,
        p_rows_processed IN NUMBER DEFAULT NULL,
        p_error_message IN VARCHAR2 DEFAULT NULL,
        p_process_id IN NUMBER DEFAULT NULL
    )
    RETURN NUMBER
    IS
        v_process_id NUMBER := p_process_id;
    BEGIN
       IF p_status = 'STARTED' THEN
            INSERT INTO process_log (process_name, target_table, start_time, status)
            VALUES (p_process_name, p_target_table, CURRENT_TIMESTAMP, p_status)
            returning process_id into v_process_id;
            RETURN v_process_id;

        ELSE
            UPDATE process_log
                SET end_time = CURRENT_TIMESTAMP, rows_processed = p_rows_processed, status = p_status, error_message = p_error_message
                WHERE process_id = v_process_id;
             RETURN v_process_id;
        END IF;
    END log_process_func;

    -- Private Function to insert into the error log
    FUNCTION log_error_func (
        p_data_source IN VARCHAR2,
        p_target_table IN VARCHAR2,
         p_error_message IN VARCHAR2
    )
    RETURN  BOOLEAN
    IS
     BEGIN
        INSERT INTO error_log (error_timestamp, data_source, target_table, error_message)
        VALUES (CURRENT_TIMESTAMP, p_data_source, p_target_table, p_error_message);
        RETURN TRUE;
        EXCEPTION
            WHEN OTHERS THEN
            RETURN FALSE;
    END log_error_func;

  
  -- Procedure to identify and store bad data from stg_location
  PROCEDURE identify_location_bad_data AS
    v_process_id NUMBER;
    v_rows_processed NUMBER := 0;
    v_error_message VARCHAR2(4000);
  BEGIN
        v_process_id := log_process_func('bad_data: location_bad_data','location_bad_data', 'STARTED');
    
      
    BEGIN
      -- Insert bad data into location_bad_data
      INSERT INTO location_bad_data
      SELECT *
      FROM stg_location
      WHERE location_key IS NULL
         OR region_name IS NULL
         OR street_name IS NULL
         OR post_code IS NULL
         OR city_name IS NULL
         OR REGEXP_LIKE(region_name, '^[0-9]+$')
          OR REGEXP_LIKE(city_name, '^[0-9]+$')
         OR NOT (
            REGEXP_LIKE(TRIM(region_name), '^[a-z]+( [a-z]+)*$')           -- All lowercase
            OR REGEXP_LIKE(TRIM(region_name), '^[A-Z]+( [A-Z]+)*$')        -- All uppercase
            OR REGEXP_LIKE(TRIM(region_name), '^[A-Z][a-z]*( [A-Z][a-z]*)*(, [A-Z][a-z]*)*$') -- Initial capital
         )
         OR NOT (
            REGEXP_LIKE(TRIM(city_name), '^[a-z]+( [a-z]+)*$')           -- All lowercase
            OR REGEXP_LIKE(TRIM(city_name), '^[A-Z]+( [A-Z]+)*$')        -- All uppercase
            OR REGEXP_LIKE(TRIM(city_name), '^[A-Z][a-z]*( [A-Z][a-z]*)*$')  -- Initial capital
         );

      -- Log rows processed
      v_rows_processed := v_rows_processed + sql%rowcount;

      -- Update process log with success status
        v_process_id := log_process_func('bad_data: location_bad_data','location_bad_data', 'SUCCESS', v_rows_processed, p_process_id => v_process_id);

      
    EXCEPTION
        WHEN OTHERS THEN
        
            -- Log error
            v_error_message := 'Error Code: ' || SQLCODE || '_' || SQLERRM;

            IF NOT log_error_func('N/A', 'location_bad_data', v_error_message) THEN
                    NULL; -- Or log that error logging itself failed, if needed
            END IF;
           -- Update process log with failure status
            v_process_id := log_process_func('bad_data: location_bad_data', 'location_bad_data', 'FAILED', p_error_message=> v_error_message, p_process_id => v_process_id);


        -- Rollback and raise error
            ROLLBACK;
            RAISE;
    END;
  END identify_location_bad_data;

  
  -- Procedure to identify and store bad data from stg_police_officer
  PROCEDURE identify_officer_bad_data AS
    v_process_id NUMBER;
    v_rows_processed NUMBER := 0;
    v_error_message VARCHAR2(4000);
  BEGIN
      -- Log process start
      v_process_id := log_process_func('bad_data: police_officer_bad_data', 'police_officer_bad_data','STARTED');

    BEGIN
      -- Insert bad data into police_officer_bad_data
      INSERT INTO police_officer_bad_data
      SELECT *
      FROM stg_police_officer
      WHERE police_officer_key IS NULL
         OR full_name IS NULL
         OR department IS NULL
         OR rank IS NULL
         OR REGEXP_LIKE(full_name, '^[0-9]+$')
          OR REGEXP_LIKE(department, '^[0-9]+$')
         OR NOT (
            REGEXP_LIKE(TRIM(full_name), '^[a-z]+( [a-z]+)*$')           -- All lowercase
            OR REGEXP_LIKE(TRIM(full_name), '^[A-Z]+( [A-Z]+)*$')        -- All uppercase
            OR REGEXP_LIKE(TRIM(full_name), '^[A-Z][a-z]*( [A-Z][a-z]*)*$')  -- Initial capital
         )
         OR NOT (
             -- Department must be either all uppercase, all lowercase, or proper case
            REGEXP_LIKE(TRIM(department), '^[a-z]+( [a-z]+)*$')           -- All lowercase
             OR REGEXP_LIKE(TRIM(department), '^[A-Z]+( [A-Z]+)*$')            -- All uppercase
            OR REGEXP_LIKE(TRIM(department), '^[A-Z][a-z]*( [A-Z][a-z]*)*$')  -- Proper case (Initial capital)
         );

      -- Log rows processed
      v_rows_processed := SQL%ROWCOUNT;
      
        -- Update process log with success status
        v_process_id := log_process_func('bad_data: police_officer_bad_data', 'police_officer_bad_data', 'SUCCESS', v_rows_processed,  p_process_id => v_process_id);

      
    EXCEPTION
        WHEN OTHERS THEN
        
           -- Log error
           v_error_message := 'Error Code: ' || SQLCODE || '_' || SQLERRM;

           IF NOT log_error_func('N/A', 'police_officer_bad_data', v_error_message) THEN
                   NULL; -- Or log that error logging itself failed, if needed
                END IF;

             -- Update process log with failure status
           v_process_id := log_process_func('bad_data: police_officer_bad_data', 'police_officer_bad_data', 'FAILED', p_error_message=> v_error_message,  p_process_id => v_process_id);

           -- Rollback and raise error
            ROLLBACK;
            RAISE;
    END;
  END identify_officer_bad_data;

END bad_data_pkg;
/

EXECUTE all procedure
BEGIN
   bad_data_pkg.identify_location_bad_data;
   bad_data_pkg.identify_officer_bad_data;
END;
/

select * from location_bad_data;
select * from police_officer_bad_data;