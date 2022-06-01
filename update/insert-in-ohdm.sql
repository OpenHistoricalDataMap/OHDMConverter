-- PostgreSQL script that update and insert entries in the OHDM db
--
-- NOTE: this script must run with access rights on the database
-- author: SteSad

\timing
-- SET client_min_messages TO WARNING;
-- DECLARE t TIMESTAMP := clock_timestamp();
DO $$
BEGIN
    RAISE NOTICE E'Insert new users';
    WITH new_users AS (
        INSERT INTO ohdm.external_users(userid, username)
        (
            SELECT uid::BIGINT,username 
            FROM updatedb.nodes 
            WHERE valid = false AND (object_changed = true or object_new = true or geom_changed = true)
                UNION
            SELECT uid::BIGINT,username 
            FROM updatedb.ways 
            WHERE valid = false AND (object_changed = true or object_new = true)
                UNION
            SELECT uid::BIGINT,username 
            FROM updatedb.relations 
            WHERE valid = false AND (object_changed = true or object_new = true)
        ) RETURNING *
    )
    UPDATE ohdm.external_users SET external_systems_id =
    (
        SELECT id
        FROM ohdm.external_systems
        WHERE source_name = 'osm'
    )
    WHERE userid IN (SELECT userid FROM new_users);

END;
$$;

DO $$
BEGIN

END;
$$;

