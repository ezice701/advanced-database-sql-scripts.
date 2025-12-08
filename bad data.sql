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


-- procedure for location
create or replace procedure identify_location_bad_data as
 v_process_id number;
    v_rows_processed number := 0;
    v_error_message varchar2(4000);  -- Declare a variable to hold error message
begin

    begin 
        insert into process_log(process_name, target_table, start_time, status)
        values('bad_data: location_bad_data', 'location_bad_data', current_timestamp, 'STARTED')
        returning process_id into v_process_id;

        -- Fetch the last inserted process_id using ROWNUM and store it in v_process_id
        select process_id into v_process_id  -- INTO clause added
        from process_log
        where process_name = 'bad_data: location_bad_data'
        and target_table = 'location_bad_data'
        and rownum = 1
        order by process_id desc;

        insert into location_bad_data
        select* 
        from stg_location
        where location_key is null
            or region_name is null
            or street_name is null
            or post_code is null
            or city_name is null
            or REGEXP_LIKE(region_name, '^[0-9]+$')
            or REGEXP_LIKE(city_name, '^[0-9]+$')
            or not (
                    REGEXP_LIKE(TRIM(region_name), '^[a-z]+( [a-z]+)*$')               -- All lowercase
                    or REGEXP_LIKE(TRIM(region_name), '^[A-Z]+( [A-Z]+)*$')            -- All uppercase
                    or REGEXP_LIKE(TRIM(region_name), '^[A-Z][a-z]*( [A-Z][a-z]*)*(, [A-Z][a-z]*)*$')  -- Initial capital
                )
            or not (
                REGEXP_LIKE(TRIM(city_name), '^[a-z]+( [a-z]+)*$')               -- All lowercase
                or REGEXP_LIKE(TRIM(city_name), '^[A-Z]+( [A-Z]+)*$')            -- All uppercase
                or REGEXP_LIKE(TRIM(city_name), '^[A-Z][a-z]*( [A-Z][a-z]*)*$')  -- Initial capital
            );

        -- log the rows processed for this step
        v_rows_processed := sql%rowcount;

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
    end;
end;
/

begin
    identify_location_bad_data;
end;
/


-- procedure for police officer
create or replace procedure identity_officer_bad_data as
 v_process_id number;
    v_rows_processed number := 0;
    v_error_message varchar2(4000);  -- Declare a variable to hold error message
begin

    begin 
        insert into process_log(process_name, target_table, start_time, status)
        values('bad_data: police_officer_bad_data', 'police_officer_bad_data', current_timestamp, 'STARTED')
        returning process_id into v_process_id;

          -- Fetch the last inserted process_id using ROWNUM and store it in v_process_id
        select process_id into v_process_id  -- INTO clause added
        from process_log
        where process_name = 'bad_data: police_officer_bad_data'
        and target_table = 'police_officer_bad_data'
        and rownum = 1
        order by process_id desc;

        insert into police_officer_bad_data
        select* 
        from stg_police_officer
        where police_officer_key is null
            or full_name is null
            or department is null
            or rank is null
            or REGEXP_LIKE(full_name, '^[0-9]+$')
            or REGEXP_LIKE(department, '^[0-9]+$')
            or not (
                REGEXP_LIKE(TRIM(full_name), '^[a-z]+( [a-z]+)*$')               -- All lowercase
                or REGEXP_LIKE(TRIM(full_name), '^[A-Z]+( [A-Z]+)*$')            -- All uppercase
                or REGEXP_LIKE(TRIM(full_name), '^[A-Z][a-z]*( [A-Z][a-z]*)*$')  -- Initial capital
            )
            or not (
                 -- Department must be either all uppercase, all lowercase, or proper case
                REGEXP_LIKE(TRIM(department), '^[a-z]+( [a-z]+)*$')               -- All lowercase
                or REGEXP_LIKE(TRIM(department), '^[A-Z]+( [A-Z]+)*$')            -- All uppercase
                or REGEXP_LIKE(TRIM(department), '^[A-Z][a-z]*( [A-Z][a-z]*)*$')  -- Proper case (Initial capital)
            );

         -- log the rows processed for this step
        v_rows_processed := sql%rowcount;

        update process_log
        set end_time =current_timestamp, rows_processed = v_rows_processed, status='SUCCESS'
        where process_id= v_process_id;
    exception
        when others then
            --capture the error message using SQLERRM
            v_error_message:= 'Error Code: ' || SQLCODE || '_' || SQLERRM;
            
            insert into error_log (error_timestamp, data_source, target_table, error_message)
            values (current_timestamp, 'N/A', 'police_officer_bad_data', v_error_message);

            update process_log
            set end_time=current_timestamp, status='FAILED', error_message=v_error_message
            where process_id=v_process_id;
            raise;
    end;
end;
/

begin
    identity_officer_bad_data;
end;
/


