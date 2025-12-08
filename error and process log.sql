drop sequence process_log_seq;
drop sequence error_log_seq;

drop sequence error_log_trig;
drop sequence process_log_trig;

drop table process_log;
drop table error_log;

create sequence error_log_seq start with 1 increment by 1;
create sequence process_log_seq start with 1 increment by 1;


-------------------------------------------process and error log----------------------------
create table error_log(
    error_id number primary key,
    error_timestamp timestamp default current_timestamp,
    data_source varchar2(50),
    target_table varchar2(50),
    error_message varchar2(200)

);

create table process_log(
    process_id number primary key,
    process_name varchar2(100),
    target_table varchar2(50),
    start_time timestamp default current_timestamp,
    end_time timestamp,
    rows_processed integer,
    status varchar2(20),
    error_message varchar2(200)

);

create or replace trigger error_log_trig
before insert on error_log
for each row
begin
    if :new.error_id is null then
        select error_log_seq.nextval into :new.error_id from SYS.DUAL;
    end if;
end;
/

create or replace trigger process_log_trig
before insert on process_log
for each row
begin
    if :new.process_id is null then
        select process_log_seq.nextval into :new.process_id from SYS.DUAL;
    end if;
end;
/