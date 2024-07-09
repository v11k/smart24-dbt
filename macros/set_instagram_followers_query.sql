{% macro get_instagram_followers_query(company_name) %}

SELECT
    i.business_account_id AS "User ID",
    u.name AS "Name",
    u.username AS "Username",
    u.website AS "Website",
    split_part(kv.key, '_', 1) AS "Age",
    CASE
        WHEN split_part(kv.key, '_', 2) = 'M' THEN 'male'
        WHEN split_part(kv.key, '_', 2) = 'F' THEN 'female'
        ELSE 'undefined'
    END AS "Gender",
    kv.value::integer AS "Profile followers",
    f.followers AS "New followers"
FROM
    {{ source('instagram', 'user_lifetime_insights') }} i
JOIN jsonb_each_text(i.value) AS kv(key, value) ON true
LEFT JOIN {{ source('instagram', 'users') }} u ON u.id = i.business_account_id
LEFT JOIN (
        SELECT
            business_account_id,
            SUM(COALESCE(follower_count, 0)) AS followers
        FROM
            {{ source('instagram', 'user_insights') }}
        WHERE
            date >= (CURRENT_DATE - 30)
        GROUP BY
            business_account_id
    ) f ON f.business_account_id = i.business_account_id
left join {{ ref("instagram_page_ids_with_attribute")}} a
    on a.id = i.business_account_id
WHERE
    i.breakdown = 'age,gender'
    AND
   a.attribute = '{{ company_name }}'

{% endmacro %}