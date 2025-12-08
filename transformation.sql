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

----------------------------------------------------------

create or replace procedure load_trans_location as
    v_process_id number;
    v_rows_processed number := 0;
    v_error_message varchar2(4000);  -- Declare a variable to hold error message
begin

    begin 
        insert into process_log(process_name, target_table, start_time, status)
        values('Transformation: trans_location', 'trans_location', current_timestamp, 'STARTED')
        returning process_id into v_process_id;

        -- Fetch the last inserted process_id using ROWNUM and store it in v_process_id
        select process_id into v_process_id  -- INTO clause added
        from process_log
        where process_name = 'Transformation: trans_location'
        and target_table = 'trans_location'
        and rownum = 1
        order by process_id desc;

        insert into trans_location ( location_key, region_name, street_name, post_code, city_name, data_source)
        select distinct  location_key, region_name, street_name, post_code, city_name, data_source
        from location_good_data;

        -- add the rows processed in this step
        v_rows_processed := sql%rowcount;

        update process_log
        set end_time =current_timestamp, rows_processed = v_rows_processed, status='SUCCESS'
        where process_id= v_process_id;


    exception
        when others then
            --capture the error message using SQLERRM
            v_error_message:= 'Error Code: ' || SQLCODE || '_' || SQLERRM;
            
            insert into error_log (error_timestamp, data_source, target_table, error_message)
            values (current_timestamp, 'N/A', 'trans_location', v_error_message);

            update process_log
            set end_time=current_timestamp, status='FAILED', error_message=v_error_message
            where process_id=v_process_id;
            raise;
    end;
end;
/

begin
    load_trans_location;
end;
/

create or replace procedure load_trans_police_officer as
    v_process_id number;
    v_rows_processed number := 0;
    v_error_message varchar2(4000);  -- Declare a variable to hold error message
begin

    begin 
        insert into process_log(process_name, target_table, start_time, status)
        values('Transformation: trans_police_officer', 'trans_police_officer', current_timestamp, 'STARTED')
        returning process_id into v_process_id;

        -- Fetch the last inserted process_id using ROWNUM and store it in v_process_id
        select process_id into v_process_id  -- INTO clause added
        from process_log
        where process_name = 'Transformation: trans_police_officer'
        and target_table = 'trans_police_officer'
        and rownum = 1
        order by process_id desc;
        
        insert into trans_police_officer ( police_officer_key, full_name, department, rank, fk_location, data_source)
        select distinct police_officer_key, full_name, department, rank, fk_location, data_source
        from police_officer_good_data;

    -- add the rows processed in this step
        v_rows_processed := sql%rowcount;

        update process_log
        set end_time =current_timestamp, rows_processed = v_rows_processed, status='SUCCESS'
        where process_id= v_process_id;


    exception
        when others then
            --capture the error message using SQLERRM
            v_error_message:= 'Error Code: ' || SQLCODE || '_' || SQLERRM;
            
            insert into error_log (error_timestamp, data_source, target_table, error_message)
            values (current_timestamp, 'N/A', 'trans_police_officer', v_error_message);

            update process_log
            set end_time=current_timestamp, status='FAILED', error_message=v_error_message
            where process_id=v_process_id;
            raise;
    end;
end;
/

begin
    load_trans_police_officer;
end;
/

create or replace procedure load_trans_crime_type as
    v_process_id number;
    v_rows_processed number := 0;
    v_error_message varchar2(4000);  -- Declare a variable to hold error message
begin

    begin 
        insert into process_log(process_name, target_table, start_time, status)
        values('Transformation: trans_crime_type', 'trans_crime_type', current_timestamp, 'STARTED')
        returning process_id into v_process_id;

        -- Fetch the last inserted process_id using ROWNUM and store it in v_process_id
        select process_id into v_process_id  -- INTO clause added
        from process_log
        where process_name = 'Transformation: trans_crime_type'
        and target_table = 'trans_crime_type'
        and rownum = 1
        order by process_id desc; 

        insert into trans_crime_type(crime_type_key, closure_status, crime_type, fk_location, data_source)
        select distinct
            crime_type_key,
            closure_status,
            upper(crime_type),
            fk_location,
            data_source
        from stg_crime_type sct;

        -- add the rows processed in this step
        v_rows_processed := sql%rowcount;

        update process_log
        set end_time =current_timestamp, rows_processed = v_rows_processed, status='SUCCESS'
        where process_id= v_process_id;


    exception
        when others then
            --capture the error message using SQLERRM
            v_error_message:= 'Error Code: ' || SQLCODE || '_' || SQLERRM;
            
            insert into error_log (error_timestamp, data_source, target_table, error_message)
            values (current_timestamp, 'N/A', 'trans_crime_type', v_error_message);

            update process_log
            set end_time=current_timestamp, status='FAILED', error_message=v_error_message
            where process_id=v_process_id;
            raise;
    end;
end;
/

begin
    load_trans_crime_type;
end;
/

create or replace procedure load_trans_crime_register as
    v_process_id number;
    v_rows_processed number := 0;
    v_error_message varchar2(4000);  -- Declare a variable to hold error message
begin

    begin 
        insert into process_log(process_name, target_table, start_time, status)
        values('Transformation: trans_crime_register', 'trans_crime_register', current_timestamp, 'STARTED')
        returning process_id into v_process_id;

        -- Fetch the last inserted process_id using ROWNUM and store it in v_process_id
        select process_id into v_process_id  -- INTO clause added
        from process_log
        where process_name = 'Transformation: trans_crime_register'
        and target_table = 'trans_crime_register'
        and rownum = 1
        order by process_id desc; 
    
        insert into trans_crime_register(fk_location, fk_police_officer, fk_crime_type, closed_date, data_source)
        select distinct
            fk_location,
            fk_police_officer,
            fk_crime_type,
            closed_date,
            data_source
        from stg_crime_register;

    -- add the rows processed in this step
        v_rows_processed := sql%rowcount;

        update process_log
        set end_time =current_timestamp, rows_processed = v_rows_processed, status='SUCCESS'
        where process_id= v_process_id;


    exception
        when others then
            --capture the error message using SQLERRM
            v_error_message:= 'Error Code: ' || SQLCODE || '_' || SQLERRM;
            
            insert into error_log (error_timestamp, data_source, target_table, error_message)
            values (current_timestamp, 'N/A', 'trans_crime_register', v_error_message);

            update process_log
            set end_time=current_timestamp, status='FAILED', error_message=v_error_message
            where process_id=v_process_id;
            raise;
    end;
end;
/

begin
    load_trans_crime_register;
end;
/



select* from trans_location order by location_id;
select* from trans_police_officer order by officer_id;
select* from trans_crime_type order by crimetype_id;
