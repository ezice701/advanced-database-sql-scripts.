-- Drop the existing table if they exist
drop table location_good_data;
drop table police_officer_good_data;

drop table location_audit;
drop table police_officer_audit;


drop sequence seq_location_audit;
drop sequence seq_police_officer_audit;


-- good data table for location
create table location_good_data as
select* from stg_location
where 1=0;

-- good data table for police officer
create table police_officer_good_data as
select* 
from stg_police_officer
where 1=0;


CREATE SEQUENCE seq_location_audit START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE seq_police_officer_audit START WITH 1 INCREMENT BY 1;

create table location_audit (
    audit_id         NUMBER NOT NULL,
    table_name       VARCHAR2(100) NOT NULL,
    location_id      NUMBER,
    column_name      VARCHAR2(100) NOT NULL,
    old_value        VARCHAR2(4000),
    new_value        VARCHAR2(4000),
    change_date      DATE DEFAULT SYSDATE NOT NULL,
    changed_by       VARCHAR2(100),
    Operation_type   VARCHAR2(10) NOT NULL,
    CONSTRAINT pk_location_audit PRIMARY KEY (audit_id)
);


create table police_officer_audit (
    audit_id         NUMBER NOT NULL,
    table_name       VARCHAR2(100) NOT NULL,
    officer_id      NUMBER,
    column_name      VARCHAR2(100) NOT NULL,
    old_value        VARCHAR2(4000),
    new_value        VARCHAR2(4000),
    change_date      DATE DEFAULT SYSDATE NOT NULL,
    changed_by       VARCHAR2(100),
    Operation_type   VARCHAR2(10) NOT NULL,
    PRIMARY KEY (audit_id)
);


-- Drop the existing trigger if it exists
DROP TRIGGER trig_location_audit_pk;
DROP TRIGGER trig_police_officer_audit_pk;

-- Create or replace the trigger for generating AUDIT_ID
CREATE OR REPLACE TRIGGER trig_location_audit_pk
BEFORE INSERT ON location_audit
FOR EACH ROW
BEGIN
    -- Use the sequence to get the next audit_id
    :new.audit_id := seq_location_audit.nextval;
END;
/

-- Create or replace the trigger for generating AUDIT_ID
CREATE OR REPLACE TRIGGER trig_police_officer_audit_pk
BEFORE INSERT ON police_officer_audit
FOR EACH ROW
BEGIN
    -- Use the sequence to get the next audit_id
    :new.audit_id := seq_police_officer_audit.nextval;
END;
/

-- Drop the existing trigger on location if it exists
DROP TRIGGER trig_location_audittrial;

-- Create or replace the trigger for audit information in care_centre_audit
CREATE OR REPLACE TRIGGER trig_location_audittrial
AFTER UPDATE ON location_good_data
FOR EACH ROW
DECLARE
    v_changed_by VARCHAR2(100);
BEGIN
    -- Assuming you capture the username or session info for the change
    v_changed_by := USER;

    IF UPDATING THEN
        
          IF :OLD.location_key != :NEW.location_key THEN
               INSERT INTO location_audit (
                  table_name, location_id, column_name, old_value, new_value, change_date, changed_by, operation_type
                )
                 VALUES (
                    'location_good_data', :NEW.location_key, 'location_key', :OLD.location_key, :NEW.location_key, SYSDATE, v_changed_by, 'UPDATE'
                );
            END IF;
            IF :OLD.region_name != :NEW.region_name THEN
                INSERT INTO location_audit (
                    table_name, location_id, column_name, old_value, new_value, change_date, changed_by, operation_type
                )
                VALUES (
                   'location_good_data', :NEW.location_key, 'region_name', :OLD.region_name, :NEW.region_name, SYSDATE, v_changed_by, 'UPDATE'
                );
            END IF;
            IF :OLD.street_name != :NEW.street_name THEN
                INSERT INTO location_audit (
                    table_name, location_id, column_name, old_value, new_value, change_date, changed_by, operation_type
                )
                VALUES (
                    'location_good_data', :NEW.location_key, 'street_name', :OLD.street_name, :NEW.street_name, SYSDATE, v_changed_by, 'UPDATE'
                );
            END IF;
             IF :OLD.post_code != :NEW.post_code THEN
                 INSERT INTO location_audit (
                    table_name, location_id, column_name, old_value, new_value, change_date, changed_by, operation_type
                )
                VALUES (
                    'location_good_data', :NEW.location_key, 'post_code', :OLD.post_code, :NEW.post_code, SYSDATE, v_changed_by, 'UPDATE'
                );
            END IF;
            IF :OLD.city_name != :NEW.city_name THEN
                 INSERT INTO location_audit (
                    table_name, location_id, column_name, old_value, new_value, change_date, changed_by, operation_type
                )
                VALUES (
                   'location_good_data', :NEW.location_key, 'city_name', :OLD.city_name, :NEW.city_name, SYSDATE, v_changed_by, 'UPDATE'
                );
            END IF;
    END IF;
    
END;
/

-- Drop the existing trigger on police_officer_good_data if it exists
DROP TRIGGER trig_police_officer_audittrial;

CREATE OR REPLACE TRIGGER trig_police_officer_audittrial
AFTER UPDATE ON police_officer_good_data
FOR EACH ROW
DECLARE
    v_changed_by VARCHAR2(100);
BEGIN
    -- Assuming you capture the username or session info for the change
    v_changed_by := USER;

    IF UPDATING THEN
         IF :OLD.police_officer_key != :NEW.police_officer_key THEN
            INSERT INTO police_officer_audit (
                table_name, officer_id, column_name, old_value, new_value, change_date, changed_by, operation_type
            )
            VALUES (
                'police_officer_good_data', :NEW.police_officer_key, 'police_officer_key', :OLD.police_officer_key, :NEW.police_officer_key, SYSDATE, v_changed_by, 'UPDATE'
            );
        END IF;
        IF :OLD.full_name != :NEW.full_name THEN
            INSERT INTO police_officer_audit (
                table_name, officer_id, column_name, old_value, new_value, change_date, changed_by, operation_type
            )
            VALUES (
                'police_officer_good_data', :NEW.police_officer_key, 'full_name', :OLD.full_name, :NEW.full_name, SYSDATE, v_changed_by, 'UPDATE'
            );
        END IF;
        IF :OLD.department != :NEW.department THEN
            INSERT INTO police_officer_audit (
                table_name, officer_id, column_name, old_value, new_value, change_date, changed_by, operation_type
            )
            VALUES (
                'police_officer_good_data',:NEW.police_officer_key, 'department', :OLD.department, :NEW.department, SYSDATE, v_changed_by, 'UPDATE'
            );
        END IF;
        IF :OLD.rank != :NEW.rank THEN
            INSERT INTO police_officer_audit (
                table_name, officer_id, column_name, old_value, new_value, change_date, changed_by, operation_type
            )
            VALUES (
                'police_officer_good_data',:NEW.police_officer_key, 'rank', :OLD.rank, :NEW.rank, SYSDATE, v_changed_by, 'UPDATE'
            );
        END IF;
    END IF;
END;
/

CREATE OR REPLACE PACKAGE good_data_pkg AS
    PROCEDURE identify_location_good_data;
    PROCEDURE identify_officer_good_data;
    PROCEDURE process_location_good_data;
    PROCEDURE process_police_officer_good_data;
    PROCEDURE update_stg_crime_type_data;
END good_data_pkg;
/

CREATE OR REPLACE PACKAGE BODY good_data_pkg AS

    -- function to log process 
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
                SET end_time = CURRENT_TIMESTAMP, rows_processed = p_rows_processed, status = p_status, error_message = SUBSTR(p_error_message, 1, 4000)
                WHERE process_id = v_process_id;
            RETURN v_process_id;
        END IF;
    END log_process_func;

    -- function to insert error log
      FUNCTION log_error_func (
        p_data_source IN VARCHAR2,
        p_target_table IN VARCHAR2,
         p_error_message IN VARCHAR2
    )
    RETURN  BOOLEAN
    IS
     BEGIN
        INSERT INTO error_log (error_timestamp, data_source, target_table, error_message)
        VALUES (CURRENT_TIMESTAMP, p_data_source, p_target_table, SUBSTR(p_error_message, 1, 4000));
        RETURN TRUE;
        EXCEPTION
            WHEN OTHERS THEN
            RETURN FALSE;
    END log_error_func;


    PROCEDURE identify_location_good_data AS
        v_process_id NUMBER;
        v_rows_processed NUMBER := 0;
        v_error_message VARCHAR2(4000);
    BEGIN
      -- Log process start
        v_process_id := log_process_func('good_data: location_good_data', 'location_good_data', 'STARTED');
    BEGIN
            INSERT INTO location_good_data
            SELECT *
            FROM stg_location
            WHERE location_key IS NOT NULL
            AND region_name IS NOT NULL
            AND street_name IS NOT NULL
            AND post_code IS NOT NULL
            AND city_name IS NOT NULL
            AND NOT REGEXP_LIKE(region_name, '^[0-9]+$')
            AND NOT REGEXP_LIKE(street_name, '^[0-9]+$')
            AND NOT REGEXP_LIKE(city_name, '^[0-9]+$')
            AND (
               REGEXP_LIKE(TRIM(region_name), '^[a-z]+( [a-z]+)*$')
               OR REGEXP_LIKE(TRIM(region_name), '^[A-Z]+( [A-Z]+)*$')
             OR REGEXP_LIKE(TRIM(region_name), '^[A-Z][a-z]*( [A-Z][a-z]*)*(, [A-Z][a-z]*)*$')
            )
            AND (
              REGEXP_LIKE(TRIM(city_name), '^[a-z]+( [a-z]+)*$')
             OR REGEXP_LIKE(TRIM(city_name), '^[A-Z]+( [A-Z]+)*$')
               OR REGEXP_LIKE(TRIM(city_name), '^[A-Z][a-z]*( [A-Z][a-z]*)*$')
            );
            
            v_rows_processed := sql%rowcount;

            UPDATE location_good_data
            SET region_name = UPPER(region_name),
                street_name = UPPER(street_name),
                city_name = UPPER(city_name)
             WHERE region_name IS NOT NULL
                   AND street_name IS NOT NULL
                AND city_name IS NOT NULL;

            v_rows_processed := v_rows_processed + sql%rowcount;

           -- Update process log with success status
           v_process_id := log_process_func('good_data: location_good_data', 'location_good_data', 'SUCCESS', v_rows_processed,  p_process_id => v_process_id);

        EXCEPTION
            WHEN OTHERS THEN
               v_error_message := 'Error Code: ' || SQLCODE || '_' || SQLERRM;
                IF NOT log_error_func('N/A', 'location_good_data', v_error_message) THEN
                   NULL; -- Or log that error logging itself failed, if needed
                END IF;
               -- Update process log with failure status
              v_process_id := log_process_func('good_data: location_good_data', 'location_good_data', 'FAILED',  p_error_message =>v_error_message, p_process_id => v_process_id);
            RAISE;

          COMMIT;
        DBMS_OUTPUT.PUT_LINE('Data inserted and uppercase transformation applied to location_good_data.');
       END;
    END identify_location_good_data;


    PROCEDURE identify_officer_good_data AS
       v_process_id NUMBER;
        v_rows_processed NUMBER := 0;
         v_error_message VARCHAR2(4000);
     BEGIN
    -- Log process start
        v_process_id := log_process_func('good_data: police_officer_good_data', 'police_officer_good_data', 'STARTED');
         BEGIN
            insert into police_officer_good_data
            select* 
            from stg_police_officer
            where police_officer_key is not null
            and full_name is not null
            and department is not null
            and rank is not null
            and not REGEXP_LIKE(full_name, '^[0-9]+$')
            and not REGEXP_LIKE(department, '^[0-9]+$')
            and (
                REGEXP_LIKE(TRIM(full_name), '^[a-z]+( [a-z]+)*$')               -- All lowercase
                or REGEXP_LIKE(TRIM(full_name), '^[A-Z]+( [A-Z]+)*$')            -- All uppercase
                or REGEXP_LIKE(TRIM(full_name), '^[A-Z][a-z]*( [A-Z][a-z]*)*$')  -- Initial capital
            )
            and (
                 -- Department must be either all uppercase, all lowercase, or proper case
                REGEXP_LIKE(TRIM(department), '^[a-z]+( [a-z]+)*$')               -- All lowercase
                or REGEXP_LIKE(TRIM(department), '^[A-Z]+( [A-Z]+)*$')            -- All uppercase
                or REGEXP_LIKE(TRIM(department), '^[A-Z][a-z]*( [A-Z][a-z]*)*$')  -- Proper case (Initial capital)
            );
            DBMS_OUTPUT.PUT_LINE('Rows Inserted' || v_rows_processed);

            UPDATE police_officer_good_data
            SET full_name = UPPER(full_name),
            department = UPPER(department)
            WHERE full_name IS NOT NULL
            AND department IS NOT NULL;

            v_rows_processed := v_rows_processed + sql%rowcount;

             -- Update process log with success status
            v_process_id := log_process_func('good_data: police_officer_good_data', 'police_officer_good_data', 'SUCCESS', v_rows_processed, p_process_id => v_process_id);

        EXCEPTION
            WHEN OTHERS THEN
              v_error_message := 'Error Code: ' || SQLCODE || '_' || SQLERRM;
                IF NOT log_error_func('N/A', 'police_officer_good_data', v_error_message) THEN
                    NULL; -- Or log that error logging itself failed, if needed
                END IF;

             -- Update process log with failure status
              v_process_id := log_process_func('good_data: police_officer_good_data', 'police_officer_good_data', 'FAILED',  p_error_message =>v_error_message, p_process_id => v_process_id);
              RAISE;

           COMMIT;
       END;
    END identify_officer_good_data;

    PROCEDURE process_location_good_data AS
       v_process_id NUMBER;
        v_rows_processed NUMBER := 0;
        v_error_message VARCHAR2(4000);
    BEGIN
    -- Log process start
        v_process_id := log_process_func('good_data: location_bad_data', 'location_bad_data', 'STARTED');
     BEGIN

            UPDATE location_bad_data
            SET region_name = 'UNKNOWN'
            WHERE region_name IS NULL;

            v_rows_processed := sql%rowcount;
           UPDATE location_bad_data
            SET street_name = 'UNKNOWN'
             WHERE street_name IS NULL;

             v_rows_processed := v_rows_processed + sql%rowcount;
             UPDATE location_bad_data
             SET post_code = 'UNKNOWN'
            WHERE post_code IS NULL;

              v_rows_processed := v_rows_processed + sql%rowcount;
             UPDATE location_bad_data
              SET city_name = 'UNKNOWN'
             WHERE city_name IS NULL;

            v_rows_processed := v_rows_processed + sql%rowcount;
            UPDATE location_bad_data
              SET region_name = 'UNKNOWN'
              WHERE REGEXP_LIKE(region_name, '^[0-9]+$');

              v_rows_processed := v_rows_processed + sql%rowcount;
           UPDATE location_bad_data
             SET city_name = 'UNKNOWN'
              WHERE REGEXP_LIKE(city_name, '^[0-9]+$');

             v_rows_processed := v_rows_processed + sql%rowcount;
            UPDATE location_bad_data
            SET region_name = UPPER(TRIM(region_name))
            WHERE region_name IS NOT NULL;

           v_rows_processed := v_rows_processed + sql%rowcount;
             UPDATE location_bad_data
             SET city_name = UPPER(TRIM(city_name))
            WHERE city_name IS NOT NULL;

            v_rows_processed := v_rows_processed + sql%rowcount;
             UPDATE location_bad_data
             SET street_name = UPPER(TRIM(street_name))
             WHERE street_name IS NOT NULL;

            v_rows_processed := v_rows_processed + sql%rowcount;

              -- Insert the cleaned data from the bad table into the good table
            INSERT INTO location_good_data
            SELECT *
            FROM location_bad_data
            WHERE location_key IS NOT NULL
            AND region_name IS NOT NULL
            AND street_name IS NOT NULL
            AND post_code IS NOT NULL
            AND city_name IS NOT NULL;

            v_rows_processed := v_rows_processed + sql%rowcount;

           -- Update process log with success status
             v_process_id := log_process_func('good_data: location_bad_data', 'location_bad_data', 'SUCCESS', v_rows_processed,  p_process_id => v_process_id);

        EXCEPTION
            WHEN OTHERS THEN
              v_error_message := 'Error Code: ' || SQLCODE || '_' || SQLERRM;
              IF NOT log_error_func('N/A', 'location_bad_data', v_error_message) THEN
                  NULL; -- Or log that error logging itself failed, if needed
                END IF;
             -- Update process log with failure status
            v_process_id := log_process_func('good_data: location_bad_data', 'location_bad_data', 'FAILED',  p_error_message =>v_error_message, p_process_id => v_process_id);

            RAISE;
          COMMIT;

        END;
     END process_location_good_data;
    
    PROCEDURE process_police_officer_good_data AS
        v_process_id NUMBER;
        v_rows_processed NUMBER := 0;
        v_error_message VARCHAR2(4000);
    BEGIN
    -- Log process start
          v_process_id := log_process_func('good_data: process_police_officer_bad_data', 'police_officer_bad_data', 'STARTED');
         BEGIN

            UPDATE police_officer_bad_data
            SET full_name = 'UNKNOWN'
            WHERE full_name IS NULL;

            v_rows_processed := sql%rowcount;

           UPDATE police_officer_bad_data
             SET department = 'UNKNOWN'
           WHERE department IS NULL;

            v_rows_processed := v_rows_processed + sql%rowcount;
          UPDATE police_officer_bad_data
            SET rank = 'UNKNOWN'
           WHERE rank IS NULL;
           
           v_rows_processed := v_rows_processed + sql%rowcount;
            UPDATE police_officer_bad_data
              SET full_name = 'UNKNOWN'
          WHERE REGEXP_LIKE(full_name, '^[0-9]+$');

            v_rows_processed := v_rows_processed + sql%rowcount;
             UPDATE police_officer_bad_data
             SET department = 'UNKNOWN'
              WHERE REGEXP_LIKE(department, '^[0-9]+$');

           v_rows_processed := v_rows_processed + sql%rowcount;
             UPDATE police_officer_bad_data
             SET full_name = UPPER(TRIM(full_name))
             WHERE full_name IS NOT NULL;

            v_rows_processed := v_rows_processed + sql%rowcount;

             UPDATE police_officer_bad_data
            SET department = UPPER(TRIM(department))
             WHERE department IS NOT NULL;

           v_rows_processed := v_rows_processed + sql%rowcount;

           
            -- Insert the cleaned data from the bad table into the good table
            INSERT INTO police_officer_good_data
            SELECT *
            FROM police_officer_bad_data
            WHERE police_officer_key IS NOT NULL
            AND full_name IS NOT NULL
            AND department IS NOT NULL
            AND rank IS NOT NULL;

            v_rows_processed := v_rows_processed + sql%rowcount;

           -- Update process log with success status
            v_process_id := log_process_func('good_data: process_police_officer_bad_data', 'police_officer_bad_data', 'SUCCESS', v_rows_processed, p_process_id => v_process_id);
        EXCEPTION
              WHEN OTHERS THEN
                v_error_message := 'Error Code: ' || SQLCODE || '_' || SQLERRM;
                IF NOT log_error_func('N/A', 'police_officer_bad_data', v_error_message) THEN
                   NULL; -- Or log that error logging itself failed, if needed
                END IF;
                -- Update process log with failure status
            v_process_id := log_process_func('good_data: process_police_officer_bad_data', 'police_officer_bad_data', 'FAILED', p_error_message => v_error_message, p_process_id => v_process_id);
            RAISE;

         COMMIT;
        DBMS_OUTPUT.PUT_LINE('Processing for police officer bad data completed.');
      END;
     END process_police_officer_good_data;
    
   PROCEDURE update_stg_crime_type_data AS
        v_process_id NUMBER;
        v_rows_processed NUMBER := 0;
       v_error_message VARCHAR2(4000);
     BEGIN
     -- Log process start
           v_process_id := log_process_func('Update data: stg_crime_type', 'stg_crime_type', 'STARTED');
      BEGIN
               UPDATE stg_crime_type
             SET closure_status = 'CLOSED'
              WHERE closure_status IS NULL;
             v_rows_processed := sql%rowcount;

           UPDATE stg_crime_type
            SET crime_type = 'UNKNOWN'
              WHERE crime_type IS NULL;

              v_rows_processed := v_rows_processed + sql%rowcount;
               -- Update process log with success status
               v_process_id := log_process_func('Update data: stg_crime_type', 'stg_crime_type', 'SUCCESS', v_rows_processed,  p_process_id => v_process_id);
         EXCEPTION
                WHEN OTHERS THEN
                  v_error_message := 'Error Code: ' || SQLCODE || '_' || SQLERRM;
                  IF NOT log_error_func('N/A', 'stg_crime_type', v_error_message) THEN
                       NULL; -- Or log that error logging itself failed, if needed
                   END IF;
                   -- Update process log with failure status
             v_process_id := log_process_func('Update data: stg_crime_type', 'stg_crime_type', 'FAILED',  p_error_message => v_error_message, p_process_id => v_process_id);
                RAISE;

             COMMIT;
          DBMS_OUTPUT.PUT_LINE('Update data for stg_crime_type has been completed successfully.');

      END;
   END update_stg_crime_type_data;

END good_data_pkg;
/
--EXECUTE the procedure one by one
BEGIN
    good_data_pkg.identify_location_good_data;
    good_data_pkg.identify_officer_good_data;
    good_data_pkg.process_location_good_data;
    good_data_pkg.process_police_officer_good_data;
    good_data_pkg.update_stg_crime_type_data;
END;
/



select* from location_good_data order by location_id;
select* from police_officer_good_data order by officer_id;