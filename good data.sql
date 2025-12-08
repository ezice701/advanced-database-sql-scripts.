drop table location_good_data;
drop table police_officer_good_data;

-- good data table for location
create table location_good_data as
select* from stg_location
where 1=0;

-- good data table for police officer
create table police_officer_good_data as
select* 
from stg_police_officer
where 1=0;

-- insert good data to good tables from stg
create or replace procedure identify_location_good_data as
    v_process_id number;
    v_rows_processed number := 0;
    v_error_message varchar2(4000);  -- Declare a variable to hold error message
begin

    begin 
        insert into process_log(process_name, target_table, start_time, status)
        values('good_data: location_good_data', 'location_good_data', current_timestamp, 'STARTED')
        returning process_id into v_process_id;

          -- Fetch the last inserted process_id using ROWNUM and store it in v_process_id
        select process_id into v_process_id  -- INTO clause added
        from process_log
        where process_name = 'good_data: location_good_data'
        and target_table = 'location_good_data'
        and rownum = 1
        order by process_id desc;

        insert into location_good_data
        select* 
        from stg_location
        where location_key is not null
            and region_name is not null
            and street_name is not null
            and post_code is not null
            and city_name is not null
            and not REGEXP_LIKE(region_name, '^[0-9]+$')
            and not REGEXP_LIKE(street_name, '^[0-9]+$')
            and not REGEXP_LIKE(city_name, '^[0-9]+$')
            and (
                    REGEXP_LIKE(TRIM(region_name), '^[a-z]+( [a-z]+)*$')               -- All lowercase
                    or REGEXP_LIKE(TRIM(region_name), '^[A-Z]+( [A-Z]+)*$')            -- All uppercase
                    or REGEXP_LIKE(TRIM(region_name), '^[A-Z][a-z]*( [A-Z][a-z]*)*(, [A-Z][a-z]*)*$')  -- Initial capital
                )
            and (
                REGEXP_LIKE(TRIM(city_name), '^[a-z]+( [a-z]+)*$')               -- All lowercase
                or REGEXP_LIKE(TRIM(city_name), '^[A-Z]+( [A-Z]+)*$')            -- All uppercase
                or REGEXP_LIKE(TRIM(city_name), '^[A-Z][a-z]*( [A-Z][a-z]*)*$')  -- Initial capital
            );

         -- log the rows processed for this step
        v_rows_processed := sql%rowcount;

             -- Now, apply uppercase transformation after data is inserted into the good table
        update location_good_data
        set region_name = UPPER(region_name),
            street_name = UPPER(street_name),
            city_name = UPPER(city_name)
        where region_name is not null
            and street_name is not null
            and city_name is not null;

        -- add the rows processed in this step
        v_rows_processed := v_rows_processed + sql%rowcount;

        update process_log
        set end_time =current_timestamp, rows_processed = v_rows_processed, status='SUCCESS'
        where process_id= v_process_id;
    
    exception
        when others then
            --capture the error message using SQLERRM
            v_error_message:= 'Error Code: ' || SQLCODE || '_' || SQLERRM;
            
            insert into error_log (error_timestamp, data_source, target_table, error_message)
            values (current_timestamp, 'N/A', 'location_good_data', v_error_message);

            update process_log
            set end_time=current_timestamp, status='FAILED', error_message=v_error_message
            where process_id=v_process_id;
            raise;
        
        commit;
        -- Log completion message
        DBMS_OUTPUT.PUT_LINE('Data inserted and uppercase transformation applied to location_good_data.');
            
    end;
end;
/

begin
    identify_location_good_data;
end;
/

select * from location_good_data;

create or replace procedure identity_officer_good_data as
    v_process_id number;
    v_rows_processed number := 0;
    v_error_message varchar2(4000);  -- Declare a variable to hold error message
begin

    begin 
        insert into process_log(process_name, target_table, start_time, status)
        values('good_data: police_officer_good_data', 'police_officer_good_data', current_timestamp, 'STARTED')
        returning process_id into v_process_id;

          -- Fetch the last inserted process_id using ROWNUM and store it in v_process_id
        select process_id into v_process_id  -- INTO clause added
        from process_log
        where process_name = 'good_data: police_officer_good_data'
        and target_table = 'police_officer_good_data'
        and rownum = 1
        order by process_id desc;

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

    -- Now, apply uppercase transformation after data is inserted into the good table
    update police_officer_good_data
    set full_name = UPPER(full_name),
        department = UPPER(department)
    where full_name is not null
        and department is not null;

        -- add the rows processed in this step
        v_rows_processed := v_rows_processed + sql%rowcount;

            
        update process_log
        set end_time =current_timestamp, rows_processed = v_rows_processed, status='SUCCESS'
        where process_id= v_process_id;
        
    exception
        when others then
            --capture the error message using SQLERRM
            v_error_message:= 'Error Code: ' || SQLCODE || '_' || SQLERRM;
            
            insert into error_log (error_timestamp, data_source, target_table, error_message)
            values (current_timestamp, 'N/A', 'police_officer_good_data', v_error_message);

            update process_log
            set end_time=current_timestamp, status='FAILED', error_message=v_error_message
            where process_id=v_process_id;
            raise;

    -- Commit the changes
    commit;
    -- Log completion message
    DBMS_OUTPUT.PUT_LINE('Data inserted and uppercase transformation applied to police_officer_good_data.');
    end;
end;
/

begin
    identity_officer_good_data;
end;
/

select * from police_officer_good_data;


CREATE OR REPLACE PROCEDURE process_location_good_data AS
    v_process_id number;
    v_rows_processed number := 0;
    v_error_message varchar2(4000);  -- Declare a variable to hold error message
begin

    begin 
        insert into process_log(process_name, target_table, start_time, status)
        values('good_data: location_bad_data', 'location_bad_data', current_timestamp, 'STARTED')
        returning process_id into v_process_id;

          -- Fetch the last inserted process_id using ROWNUM and store it in v_process_id
        select process_id into v_process_id  -- INTO clause added
        from process_log
        where process_name = 'good_data: location_bad_data'
        and target_table = 'location_bad_data'
        and rownum = 1
        order by process_id desc;

        -- Update bad data table (location_bad_data) for region_name, city_name, and other fields
            -- Replace NULL values for region_name
        UPDATE location_bad_data
        SET region_name = 'UNKNOWN'
        WHERE region_name IS NULL;

        -- log the rows processed for this step
        v_rows_processed := sql%rowcount;

        -- Replace NULL values for street_name
        UPDATE location_bad_data
        SET street_name = 'UNKNOWN'
        WHERE street_name IS NULL;

        -- add the rows processed in this step
        v_rows_processed := v_rows_processed + sql%rowcount;

        -- Replace NULL values for post_code
        UPDATE location_bad_data
        SET post_code = 'UNKNOWN'
        WHERE post_code IS NULL;

        -- add the rows processed in this step
        v_rows_processed := v_rows_processed + sql%rowcount;

        -- Replace NULL values for city_name
        UPDATE location_bad_data
        SET city_name = 'UNKNOWN'
        WHERE city_name IS NULL;

        -- add the rows processed in this step
        v_rows_processed := v_rows_processed + sql%rowcount;

        -- Ensure that region_name and city_name are not numeric (replace with 'Unknown' if numeric)
        UPDATE location_bad_data
        SET region_name = 'UNKNOWN'
        WHERE REGEXP_LIKE(region_name, '^[0-9]+$');

        -- add the rows processed in this step
        v_rows_processed := v_rows_processed + sql%rowcount;

        UPDATE location_bad_data
        SET city_name = 'UNKNOWN'
        WHERE REGEXP_LIKE(city_name, '^[0-9]+$');

        -- add the rows processed in this step
        v_rows_processed := v_rows_processed + sql%rowcount;

        -- Capitalize all letters in region_name
        UPDATE location_bad_data
        SET region_name = UPPER(TRIM(region_name))
        WHERE region_name IS NOT NULL;

        -- add the rows processed in this step
        v_rows_processed := v_rows_processed + sql%rowcount;

        -- Capitalize all letters in city_name
        UPDATE location_bad_data
        SET city_name = UPPER(TRIM(city_name))
        WHERE city_name IS NOT NULL;

        -- add the rows processed in this step
        v_rows_processed := v_rows_processed + sql%rowcount;

        -- Capitalize all letters in street_name
        UPDATE location_bad_data
        SET street_name = UPPER(TRIM(street_name))
        WHERE street_name IS NOT NULL;

        -- add the rows processed in this step
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

        -- add the rows processed in this step
        v_rows_processed := v_rows_processed + sql%rowcount;

        update process_log
        set end_time =current_timestamp, rows_processed = v_rows_processed, status='SUCCESS'
        where process_id= v_process_id;
    
    exception
        when others then
            --capture the error message using SQLERRM
            v_error_message:= 'Error Code: ' || SQLCODE || '_' || SQLERRM;
            
            insert into error_log (error_timestamp, data_source, target_table, error_message)
            values (current_timestamp, 'N/A', 'location_bad_data', v_error_message);

            update process_log
            set end_time=current_timestamp, status='FAILED', error_message=v_error_message
            where process_id=v_process_id;
            raise;

        -- Commit the changes
        COMMIT;

        -- Log completion message
        DBMS_OUTPUT.PUT_LINE('Processing for location bad data completed and data inserted into good table.');
    end;
END;
/   

begin
    process_location_good_data;
end;
/

CREATE OR REPLACE PROCEDURE process_police_officer_good_data AS
    v_process_id number;
    v_rows_processed number := 0;
    v_error_message varchar2(4000);  -- Declare a variable to hold error message
begin

    begin 
        insert into process_log(process_name, target_table, start_time, status)
        values('good_data: process_stg_police_officer', 'process_stg_police_officer', current_timestamp, 'STARTED')
        returning process_id into v_process_id;

        
        -- Fetch the last inserted process_id using ROWNUM and store it in v_process_id
        select process_id into v_process_id  -- INTO clause added
        from process_log
        where process_name = 'good_data: process_stg_police_officer'
        and target_table = 'process_stg_police_officer'
        and rownum = 1
        order by process_id desc;

        -- log the rows processed for this step
        v_rows_processed := sql%rowcount;

        -- Replace NULL values for full_name
        UPDATE police_officer_bad_data
        SET full_name = 'UNKNOWN'
        WHERE full_name IS NULL;

        -- add the rows processed in this step
        v_rows_processed := v_rows_processed + sql%rowcount;

        -- Replace NULL values for department
        UPDATE police_officer_bad_data
        SET department = 'UNKNOWN'
        WHERE department IS NULL;

        -- add the rows processed in this step
        v_rows_processed := v_rows_processed + sql%rowcount;

        -- Replace NULL values for rank
        UPDATE police_officer_bad_data
        SET rank = 'UNKNOWN'
        WHERE rank IS NULL;

        -- add the rows processed in this step
        v_rows_processed := v_rows_processed + sql%rowcount;

        -- Ensure that full_name is not numeric (replace with 'Unknown' if numeric)
        UPDATE police_officer_bad_data
        SET full_name = 'UNKNOWN'
        WHERE REGEXP_LIKE(full_name, '^[0-9]+$');

        -- add the rows processed in this step
        v_rows_processed := v_rows_processed + sql%rowcount;

        -- Ensure that department is not numeric (replace with 'Unknown' if numeric)
        UPDATE police_officer_bad_data
        SET department = 'UNKNOWN'
        WHERE REGEXP_LIKE(department, '^[0-9]+$');

        -- add the rows processed in this step
        v_rows_processed := v_rows_processed + sql%rowcount;

            -- Capitalize all letters in full_name
        UPDATE police_officer_bad_data
        SET full_name = UPPER(TRIM(full_name))
        WHERE full_name IS NOT NULL;

        -- add the rows processed in this step
        v_rows_processed := v_rows_processed + sql%rowcount;

        -- Capitalize all letters in department
        UPDATE police_officer_bad_data
        SET department = UPPER(TRIM(department))
        WHERE department IS NOT NULL;

        -- add the rows processed in this step
        v_rows_processed := v_rows_processed + sql%rowcount;


        -- Insert the cleaned data from the bad table into the good table
        INSERT INTO police_officer_good_data
        SELECT *
        FROM police_officer_bad_data
        WHERE police_officer_key IS NOT NULL
        AND full_name IS NOT NULL
        AND department IS NOT NULL
        AND rank IS NOT NULL;

        -- add the rows processed in this step
        v_rows_processed := v_rows_processed + sql%rowcount;
        update process_log
        set end_time =current_timestamp, rows_processed = v_rows_processed, status='SUCCESS'
        where process_id= v_process_id;
    

    exception
        when others then
            --capture the error message using SQLERRM
            v_error_message:= 'Error Code: ' || SQLCODE || '_' || SQLERRM;
            
            insert into error_log (error_timestamp, data_source, target_table, error_message)
            values (current_timestamp, 'N/A', 'process_stg_police_officer', v_error_message);

            update process_log
            set end_time=current_timestamp, status='FAILED', error_message=v_error_message
            where process_id=v_process_id;
            raise;
    

        -- Commit the changes
        COMMIT;

        -- Log completion message
        DBMS_OUTPUT.PUT_LINE('Processing for police officer bad data completed and data inserted into good table.');
    end;
END;
/

begin
    process_police_officer_good_data;
end;
/

select* from location_good_data order by location_id;
select* from police_officer_good_data order by officer_id;

-- update stg_crime_type table
update stg_crime_type
set closure_status = 'CLOSED'
where closure_status is null;

update stg_crime_type
set crime_type = 'UNKNOWN'
where crime_type is null;    


