drop sequence trans_location_seq;
drop sequence trans_crime_type_seq;
drop sequence trans_police_officer_seq;
drop sequence trans_crime_register_seq;


drop trigger trans_crime_type_trig;
drop trigger trans_location_trig;
drop trigger trans_police_officer_trig;
drop trigger trans_crime_register_trig;


drop table trans_location;
drop table trans_crime_type;
drop table trans_police_officer;
drop table trans_crime_register;


---------------------------create SEQ------------------------
create sequence trans_location_seq start with 1 increment by 1;
create sequence trans_crime_type_seq start with 1 increment by 1;
create sequence trans_police_officer_seq start with 1 increment by 1;
create sequence trans_crime_register_seq start with 1 increment by 1;




-- Create a Database table to represent the "dim_location" entity.
CREATE TABLE trans_location(
    location_id	INTEGER NOT NULL,
    location_key	INTEGER,
    region_name	VARCHAR(20),
    street_name	VARCHAR(20),
    post_code	VARCHAR(20),
    city_name	VARCHAR(20),
    data_source	VARCHAR(40),
    -- Specify the PRIMARY KEY constraint for table "dim_location".
    -- This indicates which attribute(s) uniquely identify each row of data.
    PRIMARY KEY (location_id)
);

-- Create a Database table to represent the "dim_crime_type" entity.
CREATE TABLE trans_crime_type(
    crimetype_id	INTEGER NOT NULL,
    crime_type_key	INTEGER,
    closure_status	VARCHAR(20),
    crime_type	VARCHAR(40),
    fk_location INTEGER,

    data_source	VARCHAR(40),
    -- Specify the PRIMARY KEY constraint for table "dim_crime_type".
    -- This indicates which attribute(s) uniquely identify each row of data.
    PRIMARY KEY (crimetype_id)
);

-- Create a Database table to represent the "dim_police_officer" entity.
CREATE TABLE trans_police_officer(
    officer_id	INTEGER NOT NULL,
    police_officer_key	INTEGER,
    full_name	VARCHAR(40),
    department	VARCHAR(20),
    rank	INTEGER,
    fk_location INTEGER,
    data_source	VARCHAR(40),
    -- Specify the PRIMARY KEY constraint for table "dim_police_officer".
    -- This indicates which attribute(s) uniquely identify each row of data.
    PRIMARY KEY (officer_id)
);

CREATE TABLE trans_crime_register(
    crime_register_id	INTEGER NOT NULL,
    fk_location INTEGER,
    fk_police_officer INTEGER,
    fk_crime_type INTEGER,
    closed_date VARCHAR(50),
    data_source	VARCHAR(40),

    PRIMARY KEY (crime_register_id)
);



------------------- triggers for uniques id's---------------------

create or replace trigger trans_crime_type_trig
before insert on trans_crime_type
for each row
begin
    if :new.crimetype_id is null then
        select trans_crime_type_seq.nextval into :new.crimetype_id from SYS.DUAL;
    end if;
end;
/

create or replace trigger trans_location_trig
before insert on trans_location
for each row
begin
    if :new.location_id is null then
        select trans_location_seq.nextval into :new.location_id from SYS.DUAL;
    end if;
end;
/

create or replace trigger trans_police_officer_trig
before insert on trans_police_officer
for each row
begin
    if :new.officer_id is null then
        select trans_police_officer_seq.nextval into :new.officer_id from SYS.DUAL;
    end if;
end;
/

create or replace trigger trans_crime_register_trig
before insert on trans_crime_register
for each row
begin
    if :new.crime_register_id is null then
        select trans_crime_register_seq.nextval into :new.crime_register_id from SYS.DUAL;
    end if;
end;
/

--------------------------------------------------------------------
CREATE OR REPLACE PACKAGE transformation_pkg AS

    PROCEDURE load_trans_location;
    PROCEDURE load_trans_police_officer;
    PROCEDURE load_trans_crime_type;
    PROCEDURE load_trans_crime_register;
END transformation_pkg;
/

CREATE OR REPLACE PACKAGE BODY transformation_pkg AS
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

   -- Procedure to load data into trans_location
    PROCEDURE load_trans_location AS
        v_process_id NUMBER;
        v_rows_processed NUMBER := 0;
        v_error_message VARCHAR2(4000);
    BEGIN
        -- Log process start
        v_process_id := log_process_func(
            'Transformation: trans_location', 
            'trans_location', 
            'STARTED'
        );    

    BEGIN
           -- Insert data from good data table
            INSERT INTO trans_location (location_key, region_name, street_name, post_code, city_name, data_source)
            SELECT DISTINCT location_key, region_name, street_name, post_code, city_name, data_source
            FROM location_good_data;

            -- Update rows processed
            v_rows_processed := sql%rowcount;

           -- Update process log with success status
           v_process_id := log_process_func(
            'Transformation: trans_location',
            'trans_location',
            'SUCCESS',
            v_rows_processed,
            NULL,
            v_process_id
        );

    EXCEPTION
        WHEN OTHERS THEN
            -- Log error
            v_error_message := 'Error Code: ' || SQLCODE || '_' || SQLERRM;
           IF NOT log_error_func('N/A', 'trans_location', v_error_message) THEN
                   NULL; -- Or log that error logging itself failed, if needed
                END IF;
            -- Update process log with failure status
            v_process_id := log_process_func(
                'Transformation: trans_location',
                'trans_location',
                'FAILED',
                NULL,
                v_error_message,
                v_process_id
            );           
            -- Rollback and raise error
            ROLLBACK;
           RAISE;
    END;
    END load_trans_location;


    -- Procedure to load data into trans_police_officer
    PROCEDURE load_trans_police_officer AS
        v_process_id NUMBER;
        v_rows_processed NUMBER := 0;
        v_error_message VARCHAR2(4000);
    BEGIN
        -- Log process start
        v_process_id := log_process_func(
            'Transformation: trans_location', 
            'trans_location', 
            'STARTED'
        );
    BEGIN
           -- Insert data from good data table
             INSERT INTO trans_police_officer (police_officer_key, full_name, department, rank, fk_location, data_source)
            SELECT DISTINCT police_officer_key, full_name, department, rank, fk_location, data_source
             FROM police_officer_good_data;

           -- Update rows processed
            v_rows_processed := sql%rowcount;
            
             -- Update process log with success status
        v_process_id := log_process_func(
            'Transformation: trans_location',
            'trans_location',
            'SUCCESS',
            v_rows_processed,
            NULL,
            v_process_id
        );
        
        EXCEPTION
        WHEN OTHERS THEN
           -- Log error
           v_error_message := 'Error Code: ' || SQLCODE || '_' || SQLERRM;
           IF NOT log_error_func('N/A', 'trans_police_officer', v_error_message) THEN
                   NULL; -- Or log that error logging itself failed, if needed
                END IF;
           -- Update process log with failure status
            v_process_id := log_process_func(
                'Transformation: trans_location',
                'trans_location',
                'FAILED',
                NULL,
                v_error_message,
                v_process_id
            );            
            -- Rollback and raise error
            ROLLBACK;
           RAISE;
        END;
    END load_trans_police_officer;


   -- Procedure to load data into trans_crime_type
    PROCEDURE load_trans_crime_type AS
        v_process_id NUMBER;
        v_rows_processed NUMBER := 0;
        v_error_message VARCHAR2(4000);
    BEGIN
        -- Log process start
        v_process_id := log_process_func(
            'Transformation: trans_location', 
            'trans_location', 
            'STARTED'
        );    
            
    BEGIN
           -- Insert data from staging table
            INSERT INTO trans_crime_type(crime_type_key, closure_status, crime_type, fk_location, data_source)
            SELECT DISTINCT
                crime_type_key,
                closure_status,
                upper(crime_type),
                fk_location,
                data_source
            FROM stg_crime_type;
            
             -- Update rows processed
              v_rows_processed := sql%rowcount;
              
            -- Update process log with success status
        v_process_id := log_process_func(
            'Transformation: trans_location',
            'trans_location',
            'SUCCESS',
            v_rows_processed,
            NULL,
            v_process_id
        );    

        EXCEPTION
        WHEN OTHERS THEN
           -- Log error
           v_error_message := 'Error Code: ' || SQLCODE || '_' || SQLERRM;
           IF NOT log_error_func('N/A', 'trans_crime_type', v_error_message) THEN
                   NULL; -- Or log that error logging itself failed, if needed
            END IF;
           -- Update process log with failure status
            v_process_id := log_process_func(
                'Transformation: trans_location',
                'trans_location',
                'FAILED',
                NULL,
                v_error_message,
                v_process_id
            );          
        -- Rollback and raise error
        ROLLBACK;
        RAISE;
    END;
   END load_trans_crime_type;
   
   -- Procedure to load data into trans_crime_register
   PROCEDURE load_trans_crime_register AS
        v_process_id NUMBER;
        v_rows_processed NUMBER := 0;
        v_error_message VARCHAR2(4000);
    BEGIN
        -- Log process start
        v_process_id := log_process_func(
            'Transformation: trans_location', 
            'trans_location', 
            'STARTED'
        );    
    BEGIN
           -- Insert data from staging table
            INSERT INTO trans_crime_register(fk_location, fk_police_officer, fk_crime_type, closed_date, data_source)
            SELECT DISTINCT
                fk_location,
                fk_police_officer,
                fk_crime_type,
                closed_date,
                data_source
            FROM stg_crime_register;
            
           -- Update rows processed
            v_rows_processed := sql%rowcount;
            
            -- Update process log with success status
            v_process_id := log_process_func(
                'Transformation: trans_location',
                'trans_location',
                'SUCCESS',
                v_rows_processed,
                NULL,
                v_process_id
            );

      EXCEPTION
        WHEN OTHERS THEN
           -- Log error
           v_error_message := 'Error Code: ' || SQLCODE || '_' || SQLERRM;
             IF NOT log_error_func('N/A', 'trans_crime_register', v_error_message) THEN
                    NULL; -- Or log that error logging itself failed, if needed
                END IF;
            -- Update process log with failure status
            v_process_id := log_process_func(
                'Transformation: trans_location',
                'trans_location',
                'FAILED',
                NULL,
                v_error_message,
                v_process_id
            );          
        -- Rollback and raise error
        ROLLBACK;
        RAISE;
    END;
    END load_trans_crime_register;
END transformation_pkg;
/

-- Execute the transformation procedures
BEGIN
  transformation_pkg.load_trans_location;
  transformation_pkg.load_trans_police_officer;
  transformation_pkg.load_trans_crime_type;
  transformation_pkg.load_trans_crime_register;
END;
/

select* from trans_location order by location_id;
select* from trans_police_officer order by officer_id;
select* from trans_crime_type order by crimetype_id;
select* from trans_crime_register order by crime_register_id;