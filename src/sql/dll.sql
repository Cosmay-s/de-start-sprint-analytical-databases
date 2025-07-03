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

DROP TABLE IF EXISTS STV202506163__DWH.l_user_group_activity;

CREATE TABLE STV202506163__DWH.l_user_group_activity
(
    hk_l_user_group_activity INT PRIMARY KEY,
    hk_user_id INT NOT NULL,
    hk_group_id INT NOT NULL,
    load_dt DATETIME NOT NULL,
    load_src VARCHAR(20)
);

ALTER TABLE STV202506163__DWH.l_user_group_activity
    ADD CONSTRAINT fk_users_hk_user_id
    FOREIGN KEY (hk_user_id) REFERENCES STV202506163__DWH.h_users(hk_user_id);

ALTER TABLE STV202506163__DWH.l_user_group_activity
    ADD CONSTRAINT fk_groups_hk_group_id
    FOREIGN KEY (hk_group_id) REFERENCES STV202506163__DWH.h_groups(hk_group_id);
