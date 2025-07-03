DROP TABLE IF EXISTS STV202506163__STAGING.group_log;

CREATE TABLE STV202506163__STAGING.group_log (
    group_id       INTEGER,
    user_id        INTEGER,
    user_id_from   INTEGER,
    event          VARCHAR(10),
    event_datetime TIMESTAMP
)
ORDER BY group_id, user_id
SEGMENTED BY hash(group_id) ALL NODES
PARTITION BY event_datetime::date
GROUP BY calendar_hierarchy_day(event_datetime::date, 3, 2);
