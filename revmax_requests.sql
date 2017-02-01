CREATE TABLE revmax_requests (
  matched_car_id integer,
  rider_id integer,
  initiated_at date,
  matched_at date,
  location geometry,
  destination geometry,
  request_state_id_id integer,
  trip_id_id integer
);

ALTER TABLE revmax_requests ADD COLUMN request_id SERIAL PRIMARY KEY;

ALTER TABLE revmax_requests ALTER COLUMN location TYPE Geometry(POINT, 4326) USING ST_Transform("geom", 4326);

ALTER TABLE revmax_requests ALTER COLUMN destination TYPE Geometry(POINT, 4326) USING ST_Transform("geom", 4326);

ALTER TABLE revmax_requests ADD COLUMN location_latitude
