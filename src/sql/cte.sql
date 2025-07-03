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
