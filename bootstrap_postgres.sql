-- make sure user exists
DO
$body$
BEGIN
  IF NOT EXISTS (
    SELECT
      *
    FROM
      pg_catalog.pg_user
    WHERE
      usename = 'wmata'
  ) THEN
    CREATE ROLE wmata LOGIN PASSWORD 'wmata';
  END IF;
END
$body$
;


-- create the database
CREATE DATABASE wmata OWNER wmata;

-- connect to the database we just created
\c wmata

-- create the train positions table
BEGIN;
  CREATE TABLE train_positions (
    CarCount real
    , CircuitId real
    , DestinationStationCode text
    , DirectionNum real
    , LineCode text
    , SecondsAtLocation real
    , ServiceType text
    , TrainId text
    , TimeStamp timestamp
  );
COMMIT;

-- Make sure priveleges are sufficient for user
BEGIN;
  GRANT ALL PRIVILEGES ON TABLE "train_positions" TO wmata;
COMMIT;


-- same with the standard routes table
BEGIN;
  CREATE TABLE standard_routes (
    LineCode text
    , CircuitId real
    , SeqNum real
    , StationCode text
    , TrackNum real
    , TimeStamp timestamp
  );
COMMIT;

-- Make sure priveleges are sufficient for user
BEGIN;
  GRANT ALL PRIVILEGES ON TABLE "standard_routes" TO wmata;
COMMIT;


-- same with the lines table
BEGIN;
  CREATE TABLE lines (
    DisplayName text
    , EndStationCode text
    , InternalDestination1 text
    , InternalDestination2 text
    , LineCode text
    , StartStationCode text
    , TimeStamp timestamp
  );
COMMIT;

-- Make sure priveleges are sufficient for user
BEGIN;
  GRANT ALL PRIVILEGES ON TABLE "lines" TO wmata;
  COMMIT;


-- same with the station_information table
BEGIN;
  CREATE TABLE station_information (
    City text
    , Code text
    , Lat real
    , LineCode1 text
    , LineCode2 text
    , LineCode3 text
    , LineCode4 text
    , Lon real
    , Name text
    , State text
    , StationTogether1 text
    , StationTogether2 text
    , Street text
    , Zip text
    , TimeStamp timestamp
  );
COMMIT;

-- Make sure priveleges are sufficient for user
BEGIN;
  GRANT ALL PRIVILEGES ON TABLE "station_information" TO wmata;
COMMIT;


-- same with the station_to_station_information table
BEGIN;
  CREATE TABLE station_to_station_information (
    CompositeMiles real
    , DestinationStation text
    , OffPeakTime money
    , PeakTime money
    , RailTime real
    , SeniorDisabled money
    , SourceStation text
    , TimeStamp timestamp
  );
COMMIT;

-- Make sure priveleges are sufficient for user
BEGIN;
  GRANT ALL PRIVILEGES ON TABLE "station_to_station_information" TO wmata;
COMMIT;
