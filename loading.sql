create or replace procedure load_dim_location as
    v_process_id number;
    v_rows_processed number := 0;
    v_error_message varchar2(4000);  -- Declare a variable to hold error message
begin
    begin 
        insert into process_log(process_name, target_table, start_time, status)
        values('Loading: dim_location', 'dim_location', current_timestamp, 'STARTED')
        returning process_id into v_process_id;

          -- Fetch the last inserted process_id using ROWNUM and store it in v_process_id
        select process_id into v_process_id  -- INTO clause added
        from process_log
        where process_name = 'Loading: dim_location'
        and target_table = 'dim_location'
        and rownum = 1
        order by process_id desc;

        insert into dim_location(location_key, region_name, street_name, post_code, city_name, data_source)
        select 
            location_key,
            region_name,
            street_name,
            post_code,
            city_name,
            data_source
        from 
            trans_location;

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
            values (current_timestamp, 'N/A', 'dim_location', v_error_message);

            update process_log
            set end_time=current_timestamp, status='FAILED', error_message=v_error_message
            where process_id=v_process_id;
            raise;
    end;
end;
/

begin
    load_dim_location;
end;
/

select * from dim_location;

create or replace procedure load_dim_police_officer as
    v_process_id number;
    v_rows_processed number := 0;
    v_error_message varchar2(4000);  -- Declare a variable to hold error message
begin
    begin 
        insert into process_log(process_name, target_table, start_time, status)
        values('Loading: dim_police_officer', 'dim_police_officer', current_timestamp, 'STARTED')
        returning process_id into v_process_id;

          -- Fetch the last inserted process_id using ROWNUM and store it in v_process_id
        select process_id into v_process_id  -- INTO clause added
        from process_log
        where process_name = 'Loading: dim_police_officer'
        and target_table = 'dim_police_officer'
        and rownum = 1
        order by process_id desc;

        insert into dim_police_officer(
            police_officer_key,
            full_name,
            department,
            rank,
            data_source
        )
        select
            police_officer_key,
            full_name,
            department,
            rank,
            data_source
        from
            trans_police_officer;

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
            values (current_timestamp, 'N/A', 'dim_police_officer', v_error_message);

            update process_log
            set end_time=current_timestamp, status='FAILED', error_message=v_error_message
            where process_id=v_process_id;
            raise;
    end;
end;
/

begin
    load_dim_police_officer;
end;
/

select * from dim_police_officer;


create or replace procedure load_dim_crime_type as
    v_process_id number;
    v_rows_processed number := 0;
    v_error_message varchar2(4000);  -- Declare a variable to hold error message
begin
    begin 
        insert into process_log(process_name, target_table, start_time, status)
        values('Loading: dim_crime_type', 'dim_crime_type', current_timestamp, 'STARTED')
        returning process_id into v_process_id;

          -- Fetch the last inserted process_id using ROWNUM and store it in v_process_id
        select process_id into v_process_id  -- INTO clause added
        from process_log
        where process_name = 'Loading: dim_crime_type'
        and target_table = 'dim_crime_type'
        and rownum = 1
        order by process_id desc;

        insert into dim_crime_type(
            crime_type_key,
            closure_status,
            crime_type,
            data_source
        )
        select
            crime_type_key,
            closure_status,
            crime_type,
            data_source
        from
            trans_crime_type;

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
            values (current_timestamp, 'N/A', 'dim_crime_type', v_error_message);

            update process_log
            set end_time=current_timestamp, status='FAILED', error_message=v_error_message
            where process_id=v_process_id;
            raise;
    end;
end;
/

begin
    load_dim_crime_type;
end;
/

select * from dim_crime_type;

create or replace procedure load_dim_time as
    v_process_id number;
    v_rows_processed number := 0;
    v_error_message varchar2(4000);  -- Declare a variable to hold error message
begin
    begin 
        insert into process_log(process_name, target_table, start_time, status)
        values('Loading: dim_time', 'dim_time', current_timestamp, 'STARTED')
        returning process_id into v_process_id;

          -- Fetch the last inserted process_id using ROWNUM and store it in v_process_id
        select process_id into v_process_id  -- INTO clause added
        from process_log
        where process_name = 'Loading: dim_time'
        and target_table = 'dim_time'
        and rownum = 1
        order by process_id desc;
        
        insert into dim_time (
            year,
            month,
            day
        )
        select 
            extract (year from to_date(closed_date, 'MM-DD-YYYY')) as year,
            extract (month from to_date(closed_date, 'MM-DD-YYYY')) as month,
            extract (day from to_date(closed_date, 'MM-DD-YYYY')) as day
           
        from trans_crime_register;

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
            values (current_timestamp, 'N/A', 'dim_time', v_error_message);

            update process_log
            set end_time=current_timestamp, status='FAILED', error_message=v_error_message
            where process_id=v_process_id;
            raise;
    end;
end;
/

begin
    load_dim_time;
end;
/

select * from dim_time;


create or replace procedure load_from_leeds as
begin
    insert into dim_location(city_name, data_source)
    select distinct
        pdl.neighbourhood,
        'CRIME_DATA_LEEDS'
    from pivot_data_leeds pdl where pdl.neighbourhood is not null;


    insert into dim_crime_type(crime_type, closure_status, data_source)
    select distinct 
        pdl.crime_types,
        'CLOSED',
        'CRIME_DATA_LEEDS'
        from pivot_data_leeds pdl;

    insert into dim_police_officer(full_name, data_source)
    select distinct
        upper(pdl.force),
        'CRIME_DATA_LEEDS'
    from pivot_data_leeds pdl where pdl.force is not null;

    merge INTO dim_time c
          USING (
            select distinct 
            to_number(substr(pdl.month, 1,4)) as year,
            to_number(substr(pdl.month, 6,2)) as month
            from pivot_data_leeds pdl
            where pdl.month is not null
            )e
          ON (c.year = e.year and c.month = e.month)
        WHEN NOT MATCHED THEN
        INSERT (year, month)
        values(e.year, e.month);
end;
/

begin
    load_from_leeds;
end;
/


create or replace procedure load_fact_table as
begin
    merge into fact_closed_crimes fc
    using(
        select dt.time_id, dl.location_id, dct.crimetype_id, dpo.officer_id, sum(total_crimes) as no_of_crimes
        from pivot_data_leeds pdl
        inner join dim_location dl on pdl.NEIGHBOURHOOD=dl.city_name
        inner join dim_crime_type dct on pdl.crime_types=dct.crime_type
        inner join dim_police_officer dpo on upper(pdl.force)=dpo.full_name
        inner join dim_time dt on substr(pdl.month, 1,4)=dt.year and substr(pdl.month, 6,2)=dt.month
        where dct.closure_status='CLOSED'
        group by dl.location_id, dpo.officer_id, dct.crimetype_id, dt.time_id
    )s 
    on(
        fc.time_id=s.time_id and
        fc.location_id=s.location_id and
        fc.crimetype_id = s.crimetype_id and
        fc.officer_id=s.officer_id
    )

    when matched then
        update set fc.no_of_crimes= fc.no_of_crimes+ s.no_of_crimes
    when not matched then
        insert (time_id, location_id, crimetype_id, officer_id, no_of_crimes)
        values(s.time_id, s.location_id, s.crimetype_id, s.officer_id, s.no_of_crimes);


    merge into fact_closed_crimes fc
    using(
        select dt.time_id, dl.location_id, dct.crimetype_id, dpo.officer_id, count(dct.crimetype_id) as no_of_crimes
        from trans_crime_register tcr
        inner join dim_crime_type dct on tcr.fk_crime_type=dct.crime_type_key
        inner join dim_location dl on tcr.fk_location = dl.location_key
        inner join dim_police_officer dpo on tcr.fk_police_officer = dpo.police_officer_key
        inner join dim_time dt 
        on to_char(to_date(tcr.closed_date, 'MM-DD-YYYY'), 'YYYY') = to_char(dt.year)
        and to_char(to_date(tcr.closed_date, 'MM-DD-YYYY'), 'MM') = to_char(dt.month)
        and to_char(to_date(tcr.closed_date, 'MM-DD-YYYY'), 'DD') = to_char(dt.day)
        where dct.closure_status='CLOSED'
    group by dl.location_id, dpo.officer_id, dct.crimetype_id, dt.time_id
    )s 
    on(
        fc.time_id=s.time_id and
        fc.location_id=s.location_id and
        fc.crimetype_id = s.crimetype_id and
        fc.officer_id=s.officer_id
    )

    when matched then
        update set fc.no_of_crimes= fc.no_of_crimes+ s.no_of_crimes
    when not matched then
        insert (time_id, location_id, crimetype_id, officer_id, no_of_crimes)
        values(s.time_id, s.location_id, s.crimetype_id, s.officer_id, s.no_of_crimes);

end;
/

begin
    load_fact_table;
end;
/


