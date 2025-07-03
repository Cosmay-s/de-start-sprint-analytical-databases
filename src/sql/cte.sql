WITH user_group_messages AS (
    SELECT
        lgd.hk_group_id,
        COUNT(DISTINCT lum.hk_user_id) AS cnt_users_in_group_with_messages
    FROM STV202506163__DWH.l_user_message lum
    JOIN STV202506163__DWH.l_groups_dialogs lgd ON lum.hk_message_id = lgd.hk_message_id
    GROUP BY lgd.hk_group_id
)

SELECT
    hk_group_id,
    cnt_users_in_group_with_messages
FROM user_group_messages
ORDER BY cnt_users_in_group_with_messages
LIMIT 10;

#7.2

WITH filtered_auth AS (
    SELECT DISTINCT hk_l_user_group_activity
    FROM STV202506163__DWH.s_auth_history
    WHERE event = 'add'
),
earliest_groups AS (
    SELECT hk_group_id
    FROM STV202506163__DWH.h_groups
    ORDER BY registration_dt
    LIMIT 10
),
user_group_log AS (
    SELECT
        luga.hk_group_id,
        COUNT(DISTINCT luga.hk_user_id) AS cnt_added_users
    FROM STV202506163__DWH.l_user_group_activity luga
    JOIN filtered_auth fa ON luga.hk_l_user_group_activity = fa.hk_l_user_group_activity
    WHERE luga.hk_group_id IN (SELECT hk_group_id FROM earliest_groups)
    GROUP BY luga.hk_group_id
)

SELECT
    hk_group_id,
    cnt_added_users
FROM user_group_log
ORDER BY cnt_added_users DESC
LIMIT 10;


#7.3


WITH filtered_auth AS (
    SELECT DISTINCT hk_l_user_group_activity
    FROM STV202506163__DWH.s_auth_history
    WHERE event = 'add'
),
earliest_groups AS (
    SELECT hk_group_id
    FROM STV202506163__DWH.h_groups
    ORDER BY registration_dt
    LIMIT 10
),
user_group_log AS (
    SELECT
        luga.hk_group_id,
        COUNT(DISTINCT luga.hk_user_id) AS cnt_added_users
    FROM STV202506163__DWH.l_user_group_activity luga
    JOIN filtered_auth fa ON luga.hk_l_user_group_activity = fa.hk_l_user_group_activity
    WHERE luga.hk_group_id IN (SELECT hk_group_id FROM earliest_groups)
    GROUP BY luga.hk_group_id
),
user_group_messages AS (
    SELECT
        luga.hk_group_id,
        COUNT(DISTINCT lum.hk_user_id) AS cnt_users_in_group_with_messages
    FROM STV202506163__DWH.l_user_group_activity luga
    JOIN filtered_auth fa ON luga.hk_l_user_group_activity = fa.hk_l_user_group_activity
    JOIN STV202506163__DWH.l_user_message lum ON lum.hk_user_id = luga.hk_user_id
    JOIN STV202506163__DWH.l_groups_dialogs lgd ON lum.hk_message_id = lgd.hk_message_id AND lgd.hk_group_id = luga.hk_group_id
    WHERE luga.hk_group_id IN (SELECT hk_group_id FROM earliest_groups)
    GROUP BY luga.hk_group_id
)

SELECT
    ugl.hk_group_id,
    ugl.cnt_added_users,
    COALESCE(ugm.cnt_users_in_group_with_messages, 0) AS cnt_users_in_group_with_messages,
    CASE 
        WHEN ugl.cnt_added_users = 0 THEN 0
        ELSE ROUND(COALESCE(ugm.cnt_users_in_group_with_messages, 0)::numeric / ugl.cnt_added_users, 4)
    END AS group_conversion
FROM user_group_log AS ugl
LEFT JOIN user_group_messages AS ugm ON ugl.hk_group_id = ugm.hk_group_id
ORDER BY group_conversion DESC
LIMIT 10;
