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
