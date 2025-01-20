/*
 __     __          _ _                           _      __  ___  __   __   
 \ \   / /_ _ _   _| | |_ ___ ____   ___  ___  __| |     \ \/ _ \/ /  /_/   
  \ \ / / _` | | | | | __/ __|  _ \ / _ \/ _ \/ _` |      \/ / \ \/ /\      
   \ V / (_| | |_| | | |_\__ \ |_) |  __/  __/ (_| |      / / \/\ \/ /      
    \_/ \__,_|\__,_|_|\__|___/ .__/ \___|\___|\__,_|     /_/ \/_/\__/       
                             |_|                                            

Vaultspeed version: 5.7.2.16, generation date: 2025/01/20 10:46:04
DV_NAME: moto_scn01 - Release: 2(2) - Comment: 2 - Release date: 2025/01/20 10:43:14
 */


CREATE OR REPLACE FUNCTION "moto_scn01_proc".ISDATATYPE(
	pstring character varying,
	pdatatype character varying,
	pformat character varying default '')
    RETURNS integer
    LANGUAGE 'plpgsql'
    IMMUTABLE 
AS $$
 
DECLARE 
	vConverted text;
	vIsDatatype integer DEFAULT 0;
BEGIN
	BEGIN
		if pdatatype = 'DATE' then
			vConverted := TO_DATE(pString,pFormat)::character varying;
		elsif pdatatype = 'TIME' then
			vConverted := TO_TIMESTAMP(pString,pFormat)::character varying;
		elsif pdatatype = 'TIMESTAMP' then
			vConverted := TO_TIMESTAMP(pString,pFormat)::character varying;
		elsif pdatatype = 'NUMERIC' then
			vConverted := TO_NUMBER(pString,pFormat)::character varying;
		elsif pdatatype = 'BYTEA' then
			vConverted := decode(attribute, format)::character varying;
		else
			execute concat('select ''',pString,'''','::', pdatatype,';')::character varying into vConverted;
		end if;
		vIsDatatype := 1;
	EXCEPTION
	WHEN OTHERS THEN
		vIsDatatype := 0;
	END;
RETURN vIsDatatype;
END;
$$;
CREATE OR REPLACE FUNCTION "moto_scn01_proc".datediff(
	pinterval_type VARCHAR,
	pstart_timestamp TIMESTAMP,
	pend_timestamp TIMESTAMP
)
RETURNS INTEGER
LANGUAGE 'plpgsql'
AS $$
DECLARE
	diff_interval INTERVAL;
	diff INT = 0;
	years_diff INT = 0;
BEGIN
	IF pinterval_type IN ('YEAR', 'MONTH') THEN
		years_diff = DATE_PART('year', pend_timestamp) - DATE_PART('year', pstart_timestamp);

		IF pinterval_type IN ('YEAR') THEN
            -- do not count full years passed (only difference between year parts)
            RETURN years_diff;
        ELSE
            -- If end month is less than start month it will subtracted
            RETURN years_diff * 12 + (DATE_PART('month', pend_timestamp) - DATE_PART('month', pstart_timestamp));
        END IF;
	END IF;

    diff_interval = pend_timestamp - pstart_timestamp;

    diff = diff + DATE_PART('day', diff_interval);

    IF pinterval_type IN ('WEEK') THEN
        RETURN diff/7;
    END IF;

    IF pinterval_type IN ('DAY') THEN
		RETURN diff;
    END IF;

    diff = diff * 24 + DATE_PART('hour', diff_interval);

    IF pinterval_type IN ('HOUR') THEN
		RETURN diff;
    END IF;

    diff = diff * 60 + DATE_PART('minute', diff_interval);

    IF pinterval_type IN ('MINUTE') THEN
		RETURN diff;
    END IF;

    diff = diff * 60 + DATE_PART('second', diff_interval);

    RETURN diff;
END;
$$;
