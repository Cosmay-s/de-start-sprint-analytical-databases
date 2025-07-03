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

