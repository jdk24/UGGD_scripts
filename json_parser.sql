CREATE OR REPLACE FUNCTION daten.netatmo_all()
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE

  json_data jsonb;

  i jsonb;
  k text;

  request_time integer;
  request_timestamp timestamp;

  b jsonb;
  id text;
  land text;
  hoehe integer;
  stadt text;
  strasse text;
  lon double precision;
  lat double precision;
  geom public.geometry;

  pressure numeric(5,1);
  pressure_time text;
  pressure_timestamp timestamp;

  temperature numeric(3,1);
  humidity integer;
  temp_time text;
  temp_timestamp timestamp;

  module_temp text;
  module_rain text;
  module_wind text;

  rain_24h text;
  rain_live text;
  rain_60min text;
  rain_time integer;
  rain_timestamp timestamp;

  gust_angle smallint;
  wind_angle smallint;
  wind_time_int integer;
  gust_strength smallint;
  wind_strength smallint;
  wind_timestamp timestamp;
  
  logentry_payload jsonb;

BEGIN

-----------------CREATE STAGING TABLES-----------------

  CREATE TEMP TABLE tmp_temperature(
      id text,
      temperature numeric(3,1),
      temp_timestamp timestamp);

  CREATE TEMP TABLE tmp_humidity(
      id text,
      humidity integer,
      temp_timestamp timestamp);

  CREATE TEMP TABLE tmp_pressure(
      id text,
      pressure numeric(5,1),
      pressure_timestamp timestamp);

  CREATE TEMP TABLE tmp_rain(
      id text,
      rain_live numeric(6,3),
      rain_60min numeric(6,3),
      rain_24h numeric(6,3),
      rain_timestamp timestamp);

  CREATE TEMP TABLE tmp_wind(
      id text,
      gust_angle smallint,
      wind_angle smallint,
      gust_strength smallint,
      wind_strength smallint,
      wind_timestamp timestamp); 
      
-----------------GET JSON VALUES FROM INPUT TBL-----------------

  SELECT *
  FROM daten.json_input
  INTO json_data;

-----------------GET API REQUEST TIMESTAMP-----------------

  request_time := json_data->>'time_server';
  request_timestamp := to_timestamp(request_time);

-----------------START FOR LOOP WITHIN {JSON{body... WITH i AS EACH STATION

  FOR i in
  SELECT jsonb_array_elements(json_data -> 'body')
  LOOP

-----------------ACCESS STATION INFO IN JSON-BODY-PLACE

    id := i->>'_id';
    land := i->'place'->>'country';
    hoehe := i->'place'->>'altitude';
    stadt := i->'place'->>'city';
    strasse := i->'place'->>'street';
    lat := (i->'place'->'location'->>1)::double precision;
    lon := (i->'place'->'location'->>0)::double precision;
    geom := st_setsrid(st_makepoint(lon,lat),4326);


-----------------GET MAC ADDR OF MODULES AS JSON OBJECT KEYS

    FOR k IN
    SELECT jsonb_object_keys(i -> 'measures')
    LOOP      
      CASE
        WHEN left(k,2)='02' THEN module_temp := k;
        WHEN left(k,2)='05' THEN module_rain := k;
        WHEN left(k,2)='06' THEN module_wind := k;
        ELSE CONTINUE;
        --RAISE NOTICE ': %, left: %',k,left(k,2);  
      END CASE;    
  END LOOP;

-----------------INSERT STATION INFO WITH RESPECTIVE MODULES INTO TBL 
 
  INSERT INTO daten.fcp_stationen(last_updated, netatmo_id, country, altitude, city, street, module_temp, module_rain, module_wind, geom)
  SELECT request_timestamp,
         id,
         land,
         hoehe,
         stadt,
         strasse,
         module_temp,
         module_rain,
         module_wind,
         geom
  ON CONFLICT DO NOTHING;
    
----------------PRESSURE INFO AND INSERT INTO STAGING TBL

  pressure_time := jsonb_object_keys(i->'measures'->id->'res');
  pressure := (i->'measures'->id->'res'->pressure_time->>0);
  pressure_timestamp := to_timestamp(pressure_time::integer);
  
  INSERT INTO tmp_pressure(id, pressure_timestamp, pressure)
  VALUES (id, pressure_timestamp, pressure)
  ON CONFLICT DO NOTHING;
  
-----------------TEMPERATURE AND HUMIDITY INFO, SPLIT INTO RESPECTIVE TABLES
 
  temp_time := jsonb_object_keys(i->'measures'->module_temp->'res');
  temperature := (i->'measures'->module_temp->'res'->temp_time->>0);
  humidity := (i->'measures'->module_temp->'res'->temp_time->>1);

  temp_timestamp := to_timestamp(temp_time::integer);

    INSERT INTO tmp_humidity(id, humidity, temp_timestamp)
      SELECT id, humidity, temp_timestamp 
      ON CONFLICT DO NOTHING;

    INSERT INTO tmp_temperature(id, temperature, temp_timestamp)
      SELECT id, temperature, temp_timestamp
      ON CONFLICT DO NOTHING;
 
-----------------RAIN INFO--!!CAN BE 'null' STRING in JSON!!-----------------

  IF NOT module_rain IS NULL THEN    
    rain_24h := (i->'measures'->module_rain->'rain_24h');
    rain_live := (i->'measures'->module_rain->'rain_live');
    rain_60min := (i->'measures'->module_rain->'rain_60min');
    rain_time := (i->'measures'->module_rain->'rain_timeutc');
    rain_timestamp := to_timestamp(rain_time);
  END IF;


  INSERT INTO tmp_rain(id, rain_timestamp, rain_live, rain_60min, rain_24h)
  SELECT id,
         rain_timestamp,
         coalesce(nullif(rain_live,'null'))::NUMERIC (6, 3),
         coalesce(nullif(rain_60min,'null'))::NUMERIC (6, 3),
         coalesce(nullif(rain_24h,'null'))::NUMERIC (6, 3)
  WHERE rain_live != 'null'
  AND rain_60min != 'null'
  AND rain_24h != 'null'
  ON CONFLICT DO NOTHING;

-----------------WIND INFO-----------------
  
  IF NOT module_wind IS NULL THEN
    gust_angle := (i->'measures'->module_wind->'gust_angle');
    wind_angle := (i->'measures'->module_wind->'wind_angle');
    wind_time_int := (i->'measures'->module_wind->'wind_timeutc');
    gust_strength := (i->'measures'->module_wind->'gust_strength');
    wind_strength := (i->'measures'->module_wind->'wind_strength');
    wind_timestamp := to_timestamp(wind_time_int);
  END IF;

  INSERT INTO tmp_wind(id, gust_angle, wind_angle, gust_strength, wind_strength, wind_timestamp)
  SELECT id, 
         gust_angle, 
         wind_angle, 
         gust_strength, 
         wind_strength, 
         wind_timestamp
  WHERE wind_timestamp IS NOT NULL
  ON CONFLICT DO NOTHING;

-----------------SET VARIABLES WITH STATION'S MODULES NULL FOR NEXT ITERATION
	
  module_wind:=NULL; 
  module_rain:=NULL; 
  module_temp:=NULL;               

END LOOP;

-----------------END LOOP, INSERT INTO PERMANENT TABLES-----------------
-----------------JOIN WITH STATION LOOKUP TABLE AS FOREIGN KEY----------

--PRESSURE

  WITH 
  lut_stat AS 
  (
  SELECT *
  from daten.fcp_stationen)  
  INSERT INTO daten.tab_pressure(fk_station, pressure_timestamp, pressure)
  SELECT lut_stat.idpk_station,
         t.pressure_timestamp,
         t.pressure
  FROM tmp_pressure t
       JOIN lut_stat on t.id = lut_stat.netatmo_id 
  ON CONFLICT DO NOTHING;

--HUMIDITY

  WITH 
  lut_stat AS 
  (
  SELECT *
  from daten.fcp_stationen)  
  INSERT INTO daten.tab_humidity(fk_station, humidity_timestamp, humidity)
  SELECT lut_stat.idpk_station,
         t.temp_timestamp,
         t.humidity
  FROM tmp_humidity t
       JOIN lut_stat on t.id = lut_stat.netatmo_id 
  ON CONFLICT DO NOTHING;

--TEMPERATURE

  WITH 
  lut_stat AS 
  (
  SELECT *
  from daten.fcp_stationen)  
  INSERT INTO daten.tab_temperature(fk_station, temperature_timestamp,
    temperature)
  SELECT lut_stat.idpk_station,
         t.temp_timestamp,
         t.temperature
  FROM tmp_temperature t
       JOIN lut_stat on t.id = lut_stat.netatmo_id 
  ON CONFLICT DO NOTHING;

--RAIN
  WITH 
  lut_stat AS 
  (
  SELECT *
  from daten.fcp_stationen)  
  INSERT INTO daten.tab_rain(fk_station, rain_timestamp, rain_24h, rain_60m,
    rain_live)
  SELECT lut_stat.idpk_station,
         t.rain_timestamp,
         t.rain_24h,
         t.rain_60min,
         t.rain_live
  FROM tmp_rain t
       JOIN lut_stat on t.id = lut_stat.netatmo_id
  WHERE lut_stat.module_rain IS NOT NULL
  ON CONFLICT DO NOTHING;


--WIND
  WITH 
  lut_stat AS 
  (
  SELECT * from daten.fcp_stationen
  )
  INSERT INTO daten.tab_wind(fk_station,wind_timestamp, wind_angle,
    wind_strength, gust_angle, gust_strength)
  SELECT lut_stat.idpk_station,
         t.wind_timestamp,
         t.wind_angle,
         t.wind_strength,
         t.gust_angle,
         t.gust_strength
  FROM tmp_wind t
       JOIN lut_stat on t.id = lut_stat.netatmo_id
  WHERE lut_stat.module_wind IS NOT NULL
  ON CONFLICT DO NOTHING;

-----------------DROP STAGING TABLES-----------------

DROP TABLE tmp_temperature;
DROP TABLE tmp_humidity;
DROP TABLE tmp_pressure;
DROP TABLE tmp_rain;
DROP TABLE tmp_wind;

logentry_payload = '{"ALL data updated at server time":"'||request_timestamp||'"}';
EXECUTE FORMAT ('SELECT daten.createlogentry(%L)',logentry_payload);

END;
$function$
;
