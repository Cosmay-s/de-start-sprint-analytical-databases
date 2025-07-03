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

