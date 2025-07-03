INSERT INTO STV202506163__DWH.l_user_group_activity
    (hk_l_user_group_activity, hk_user_id, hk_group_id, load_dt, load_src)
SELECT DISTINCT
    HASH(hu.hk_user_id, hg.hk_group_id) AS hk_l_user_group_activity,
    hu.hk_user_id,
    hg.hk_group_id,
    gl.event_datetime AS load_dt,
    'group_log' AS load_src
FROM STV202506163__STAGING.group_log gl
LEFT JOIN STV202506163__DWH.h_users hu ON hu.user_id = gl.user_id
LEFT JOIN STV202506163__DWH.h_groups hg ON hg.group_id = gl.group_id
WHERE hu.hk_user_id IS NOT NULL
  AND hg.hk_group_id IS NOT NULL;


INSERT INTO STV202506163__DWH.s_auth_history (
    hk_l_user_group_activity,
    user_id_from,
    event,
    event_dt,
    load_dt,
    load_src
)
SELECT
    luga.hk_l_user_group_activity,
    gl.user_id_from,
    gl.event,
    gl.event_datetime,
    NOW() AS load_dt,
    'group_log' AS load_src
FROM STV202506163__STAGING.group_log AS gl
LEFT JOIN STV202506163__DWH.h_groups AS hg ON gl.group_id = hg.group_id
LEFT JOIN STV202506163__DWH.h_users AS hu ON gl.user_id = hu.user_id
LEFT JOIN STV202506163__DWH.l_user_group_activity AS luga 
    ON luga.hk_user_id = hu.hk_user_id
    AND luga.hk_group_id = hg.hk_group_id;

