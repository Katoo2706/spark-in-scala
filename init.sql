SELECT current_database();

SET search_path TO postgres;

DROP SCHEMA IF EXISTS scala;

CREATE SCHEMA scala;

SET SCHEMA 'scala';

COMMIT;

SELECT * FROM scala.movies;