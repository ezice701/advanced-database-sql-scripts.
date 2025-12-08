drop sequence pivot_data_leeds_seq;
drop table pivot_data_leeds;

--creating new table to insert pivoted data
CREATE TABLE pivot_data_leeds (
    leeds_id INTEGER,
    force VARCHAR(30),
    NEIGHBOURHOOD VARCHAR(20),
    month VARCHAR(20),
    crime_types VARCHAR(30),
    total_crimes Integer,
    PRIMARY KEY (leeds_id) 
);

create sequence pivot_data_leeds_seq start with 1 increment by 1;

create OR replace trigger  pivot_data_leeds_trig 
before insert on pivot_data_leeds
for each row
begin
	if :new.leeds_id is null then
		select pivot_data_leeds_seq.nextval into :new.leeds_id from SYS.DUAL;
	end if;
end;
/

-- Insert unpivoted data into pivot_data_leeds
INSERT INTO pivot_data_leeds ( force, NEIGHBOURHOOD, month, crime_types, total_crimes)
SELECT distinct
    force, 
    NEIGHBOURHOOD, 
    month, 
    crime_types, 
    TO_NUMBER(total_crimes) -- Convert total crimes to NUMBER
FROM (
    -- Unpivot the crime type columns into rows
    SELECT 
        FORCE,
        NEIGHBOURHOOD,
        MONTH,
        ANTI_SOCIAL_BEHAVIOUR,
        BURGLARY,
        CRIMINAL_DAMAGE_AND_ARSON,
        DRUGS,
        OTHER_THEFT,
        PUBLIC_DISORDER_AND_WEAPONS,
        ROBBERY,
        SHOPLIFTING,
        VEHICLE_CRIME,
        VIOLENT_CRIME,
        CASE 
            WHEN REGEXP_LIKE(OTHER_CRIME, '^\d+(\.\d+)?$') THEN TO_NUMBER(OTHER_CRIME) -- Convert numeric values
            ELSE NULL -- Handle non-numeric or invalid data
        END AS OTHER_CRIME
    FROM crime_data_leeds
) UNPIVOT (
    total_crimes FOR crime_types IN (
        ANTI_SOCIAL_BEHAVIOUR,
        BURGLARY,
        CRIMINAL_DAMAGE_AND_ARSON,
        DRUGS,
        OTHER_THEFT,
        PUBLIC_DISORDER_AND_WEAPONS,
        ROBBERY,
        SHOPLIFTING,
        VEHICLE_CRIME,
        VIOLENT_CRIME,
        OTHER_CRIME
    )
);


