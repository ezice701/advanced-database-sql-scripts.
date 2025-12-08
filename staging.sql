---------------------drop seq--------------------------------------
drop sequence stg_location_seq;
drop sequence stg_crime_type_seq;
drop sequence stg_police_officer_seq;
drop sequence stg_crime_register_seq;

--------------------------drop trig------------------------------
drop trigger stg_location_trig;
drop trigger stg_crime_type_trig;
drop trigger stg_police_officer_trig;
drop trigger stg_crime_register_trig;


-----------------drop tables---------------------------------------
DROP TABLE 	stg_location  CASCADE CONSTRAINTS;
DROP TABLE 	stg_crime_type  CASCADE CONSTRAINTS;
DROP TABLE 	stg_police_officer  CASCADE CONSTRAINTS;
DROP TABLE 	stg_crime_register  CASCADE CONSTRAINTS;



---------------------------create SEQ------------------------
create sequence stg_location_seq start with 1 increment by 1;
create sequence stg_crime_type_seq start with 1 increment by 1;
create sequence stg_police_officer_seq start with 1 increment by 1;
create sequence stg_crime_register_seq start with 1 increment by 1;




--------------------stagging tables---------------------------
-- Create a Database table to represent the "dim_location" entity.
CREATE TABLE stg_location(
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
CREATE TABLE stg_crime_type(
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
CREATE TABLE stg_police_officer(
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

CREATE TABLE stg_crime_register(
    crime_register_id	INTEGER NOT NULL,
    fk_location INTEGER,
    fk_police_officer INTEGER,
    fk_crime_type INTEGER,
    closed_date VARCHAR(10),
    data_source	VARCHAR(40),

    PRIMARY KEY (crime_register_id)
);


------------------- triggers for uniques id's---------------------

create or replace trigger stg_crime_type_trig
before insert on stg_crime_type
for each row
begin
    if :new.crimetype_id is null then
        select stg_crime_type_seq.nextval into :new.crimetype_id from SYS.DUAL;
    end if;
end;
/

create or replace trigger stg_location_trig
before insert on stg_location
for each row
begin
    if :new.location_id is null then
        select stg_location_seq.nextval into :new.location_id from SYS.DUAL;
    end if;
end;
/

create or replace trigger stg_police_officer_trig
before insert on stg_police_officer
for each row
begin
    if :new.officer_id is null then
        select stg_police_officer_seq.nextval into :new.officer_id from SYS.DUAL;
    end if;
end;
/

create or replace trigger stg_crime_register_trig
before insert on stg_crime_register
for each row
begin
    if :new.crime_register_id is null then
        select stg_crime_register_seq.nextval into :new.crime_register_id from SYS.DUAL;
    end if;
end;
/
--------------------------------------------------------------------------------------------

--procedue for process log and error log---------
create or replace procedure process_stg_location as
    v_process_id number;
    v_rows_processed number := 0;
    v_error_message varchar2(4000);  -- Declare a variable to hold error message

begin
    begin 
        insert into process_log(process_name, target_table, start_time, status)
        values('Staging: stg_location', 'stg_location', CURRENT_TIMESTAMP, 'STARTED');
       
       -- Fetch the last inserted process_id using ROWNUM and store it in v_process_id
        select process_id into v_process_id  -- INTO clause added
        from process_log
        where process_name = 'Staging: stg_location'
        and target_table = 'stg_location'
        and rownum = 1
        order by process_id desc;

        -- insert data into stg_location
        insert into stg_location(location_key, region_name, street_name, post_code, city_name, data_source)
        select DISTINCT 
            l.location_id, r.region_name, l.street_name, l.post_code, l.city_name, 'PS_WALES'
        from 
            location l
        left join 
            region r
        on 
            l.region_id = r.region_id
        where 
            l.house_no is not null; 

        -- log the rows processed for this step
        v_rows_processed := sql%rowcount;

        -- from prcs
        insert into stg_location(location_key, post_code, data_source)
        select DISTINCT
            p.reported_crime_id, p.crime_postcode, 'PRCS'
        from pl_reported_crime p;

        -- add the rows processed in this step
        v_rows_processed := v_rows_processed + sql%rowcount;


        -- add the rows processed in this step
        v_rows_processed := v_rows_processed + sql%rowcount;

        update process_log
        set end_time =current_timestamp, rows_processed = v_rows_processed, status='SUCCESS'
        where process_id= v_process_id;
    
       commit;
       
    exception
        when others then
            --capture the error message using SQLERRM
            v_error_message:= 'Error Code: ' || SQLCODE || '_' || SQLERRM;
            
            insert into error_log (error_timestamp, data_source, target_table, error_message)
            values (current_timestamp, 'N/A', 'stg_location', v_error_message);

            update process_log
            set end_time=current_timestamp, status='FAILED', error_message= v_error_message
            where process_id=v_process_id;
            
            -- Rollback changes if error occurs
            rollback;
            
            raise;
    end;
end;
/


begin 
    process_stg_location;
end;
/

create or replace procedure process_stg_police_officer as
    v_process_id number;
    v_rows_processed number := 0;
    v_error_message varchar2(4000);  -- Declare a variable to hold error message
begin

    begin 
        insert into process_log(process_name, target_table, start_time, status)
        values('Staging: stg_police_officer', 'stg_police_officer', current_timestamp, 'STARTED')
        returning process_id into v_process_id;


       -- Fetch the last inserted process_id using ROWNUM and store it in v_process_id
        select process_id into v_process_id  -- INTO clause added
        from process_log
        where process_name = 'Staging: stg_police_officer'
        and target_table = 'stg_police_officer'
        and rownum = 1
        order by process_id desc;

       -- from PS wales
        insert into stg_police_officer(
            police_officer_key, full_name, department, rank, fk_location, data_source
        )
        select DISTINCT
            officer_id,
            case
                when middle_name is null then first_name || ' ' || last_name
                else first_name || ' ' || middle_name || ' '|| last_name
            end as full_name,
            department, rank, 
            loc.location_id as fk_location,
            'PS_WALES'
        from officer o
        join crime_register cr on cr.police_id= o.officer_id
        join location loc on loc.location_id= cr.location_id;

        -- log the rows processed for this step
        v_rows_processed := sql%rowcount;

        -- from pcrs
        insert into stg_police_officer(
            police_officer_key, full_name, rank, fk_location, data_source
        )
        select DISTINCT
            ppe.emp_id, 
            ppe.emp_name, 
            ppe.emp_grade, 
            pa.area_id as fk_location,
            'PRCS'
        from pl_police_employee ppe
        join 
            pl_work_allocation wa on ppe.emp_id=wa.d_emp_id
        join 
            pl_reported_crime rc on wa.s_reported_crime_id= rc.reported_crime_id
        join
            pl_station ps on rc.fk2_station_id=ps.station_id
        join 
            pl_area pa on ps.fk1_area_id=pa.area_id;

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
            values (current_timestamp, 'N/A', 'stg_police_officer', v_error_message);

            update process_log
            set end_time=current_timestamp, status='FAILED', error_message=v_error_message
            where process_id=v_process_id;

            -- Rollback changes if error occurs
            rollback;

            raise;
    end;
end;
/

begin 
    process_stg_police_officer;
end;
/

    
create or replace procedure process_stg_crime_type as
    v_process_id number;
    v_rows_processed number := 0;
    v_error_message varchar2(4000);  -- Declare a variable to hold error message
begin
    begin 
        insert into process_log(process_name, target_table, start_time, status)
        values('Staging: stg_crime_type', 'stg_crime_type', current_timestamp, 'STARTED')
        returning process_id into v_process_id;

          -- Fetch the last inserted process_id using ROWNUM and store it in v_process_id
        select process_id into v_process_id  -- INTO clause added
        from process_log
        where process_name = 'Staging: stg_crime_type'
        and target_table = 'stg_crime_type'
        and rownum = 1
        order by process_id desc;

        -- from PRCS
        insert into stg_crime_type(crime_type_key, crime_type, fk_location, data_source)
        select DISTINCT
            pct.crime_type_id,
            pct.crime_type_desc,
            pa.area_id,       
            'PRCS'
        from 
            pl_crime_type pct
        left join
            pl_reported_crime prc on pct.crime_type_id=prc.fk1_crime_type_id
        left join
            pl_station ps on prc.fk2_station_id= ps.station_id
        left join
            pl_area pa on ps.fk1_area_id=pa.area_id;


        -- add the rows processed in this step
        v_rows_processed := v_rows_processed + sql%rowcount;

        insert into stg_crime_type(crime_type_key, closure_status, fk_location, data_source)
        select DISTINCT
            reported_crime_id,
            crime_status,
            pa.area_id as fk_location,
            'PRCS'
        from 
            pl_reported_crime prc
        left join
            pl_station ps on prc.fk2_station_id= ps.station_id
        left join
            pl_area pa on ps.fk1_area_id=pa.area_id;


         -- add the rows processed in this step
        v_rows_processed := v_rows_processed + sql%rowcount;

        -- from ps wales
        insert into stg_crime_type(crime_type_key, closure_status, crime_type, fk_location, data_source)
        select DISTINCT
            cr.crime_id,
            cr.crime_status,
            cr.crime_type,
            l.location_id as fk_location,
            'PS WALES'
        from crime_register cr
        left join
            location l on cr.location_id = l.location_id;
            
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
            values (current_timestamp, 'N/A', 'stg_crime_type', v_error_message);

            update process_log
            set end_time=current_timestamp, status='FAILED', error_message=v_error_message
            where process_id=v_process_id;
            raise;
    end;
end;
/

begin 
    process_stg_crime_type;
end;
/

create or replace procedure process_stg_crime_register as
   v_process_id number;
    v_rows_processed number := 0;
    v_error_message varchar2(4000);  -- Declare a variable to hold error message
begin

    begin 
        insert into process_log(process_name, target_table, start_time, status)
        values('Staging: stg_crime_register', 'stg_crime_register', current_timestamp, 'STARTED')
        returning process_id into v_process_id;

          -- Fetch the last inserted process_id using ROWNUM and store it in v_process_id
        select process_id into v_process_id  -- INTO clause added
        from process_log
        where process_name = 'Staging: stg_crime_register'
        and target_table = 'stg_crime_register'
        and rownum = 1
        order by process_id desc;

        -- for prcs
        insert into stg_crime_register(
            fk_location,
            fk_police_officer,
            fk_crime_type,
            closed_date,
            data_source
        )

        select distinct
            pa.area_id as fk_location,
            ppe.emp_id as fk_police_officer,
            prc.fk1_crime_type_id as fk_crime_type,
            case
                when prc.date_closed is not null then prc.date_closed
                when prc.date_reported is not null then             
                    to_char(prc.date_reported + trunc(dbms_random.value(1, 365)), 'MM-DD-YYYY') -- Add random days to DATE_REPORTED and format it

            end as closed_date,
            'PRCS' as data_source

        from 
            pl_reported_crime prc
        join
            pl_station ps on prc.fk2_station_id=ps.station_id
        join
            pl_area pa on ps.fk1_area_id=pa.area_id

        join 
            pl_work_allocation pwa on prc.reported_crime_id=pwa.s_reported_crime_id
        join    
            pl_police_employee ppe on pwa.d_emp_id=ppe.emp_id;


        -- log the rows processed for this step
        v_rows_processed := sql%rowcount;

        -- from ps wales
        insert into stg_crime_register(
            fk_location,
            fk_police_officer,
            fk_crime_type,
            closed_date,
            data_source
        )

        select distinct
            l.location_id as fk_location,
            o.officer_id as fk_police_officer,
            cr.crime_id as fk_crime_type,
            case
                when cr.closed_date is not null then to_char(cr.closed_date, 'MM-DD-YYYY') 
                when cr.reported_date is not null then to_char(cr.reported_date + trunc(dbms_random.value(1, 365)), 'MM-DD-YYYY')
                else NULL
            end as closed_date,
            'PS_WALES' as data_source

        from 
            crime_register cr
        join
            location l on cr.location_id = l.location_id
        join
            officer o on cr.police_id=o.officer_id;
        
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
            values (current_timestamp, 'N/A', 'stg_crime_register', v_error_message);

            update process_log
            set end_time=current_timestamp, status='FAILED', error_message=v_error_message
            where process_id=v_process_id;
            raise;
    end;
end;
/

begin 
    process_stg_crime_register;
end;
/