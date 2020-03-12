-- We have exported messages for several chats, let's explore what chats
-- and how many messages each contains and the proportion of total messages
WITH
    SELECTION(chat_title, mid) AS (
        SELECT
            c.title,
            m.id
        FROM
            messages m
        INNER JOIN
            chats c
            ON c.id = m.chat_id
    ),
    TOTAL(total_count) AS (SELECT COUNT(mid) FROM SELECTION)
SELECT
    chat_title,
    COUNT(mid) message_count,
    round(CAST(COUNT(mid) AS numeric) / total_count * 100, 2) prop
FROM
   TOTAL, SELECTION
GROUP BY
    (chat_title, total_count)
ORDER BY
    message_count DESC;

-- The cross activity of users between chats can be explored, but it is more interesting
-- to explore individual chats and user behaviors within them.  In all subsequent requests
-- we limit the initial selection scope to messages of a single chat, thus we construct the
-- queries in such a way that there is just one parameter: the title of the chat,
-- and it is stated in the beginning of the query - so replace 'Telegram' title with a
-- chat of interest title.


-- How many messages and what proportion of total do people send in a given chat
-- sorted in a descending order by the number of messages? We create a view - we will need
-- to reuse it for later queries
-- Also if you need just to look up f.e. the top 10 most active users - add a LIMIT 10 clause
DROP VIEW IF EXISTS users_by_message_count;
CREATE VIEW users_by_message_count AS
WITH
    CHAT_TITLE(chat_title) AS (SELECT 'Telegram'),
    SELECTION(uid, username, first_name, last_name, mid) AS (
        SELECT
            u.id,
            u.username,
            u.first_name,
            u.last_name,
            m.id
        FROM
            CHAT_TITLE,
            users u
        INNER JOIN
            messages m
            ON u.id = m.sender_user_id
        INNER JOIN
            chats c
            ON c.id = m.chat_id
        WHERE
            c.title = chat_title
    ),
    TOTAL(total_count) AS (SELECT COUNT(mid) FROM SELECTION)
SELECT
    uid,
    username,
    first_name,
    last_name,
    COUNT(mid) message_count,
    round(CAST(COUNT(mid) AS numeric) / total_count * 100, 2) prop
FROM
   TOTAL, SELECTION
GROUP BY
    (uid, username, first_name, last_name, total_count)
ORDER BY
    message_count DESC;
SELECT * FROM users_by_message_count;

-- What is the most active day of the week in a given chat?
WITH
    CHAT_TITLE(chat_title) AS (SELECT 'Telegram'),
    SELECTION(day_of_week, mid) AS (
        SELECT
            to_char(m.send_date, 'Day'),
            m.id
        FROM
            CHAT_TITLE,
            messages m
        INNER JOIN
            chats c
            ON c.id = m.chat_id
        WHERE
            c.title = chat_title
    ),
    TOTAL(total_count) AS (SELECT COUNT(mid) FROM SELECTION)
SELECT
    day_of_week,
    COUNT(mid) message_count,
    round(CAST(COUNT(mid) AS numeric) / total_count * 100, 2) prop
FROM
   TOTAL, SELECTION
GROUP BY
    (day_of_week, total_count)
ORDER BY
    message_count DESC;


-- So now let's group by user and day_of_week and see the most active users and
-- corresponding days of week:
WITH
    CHAT_TITLE(chat_title) AS (SELECT 'Telegram'),
    SELECTION(username, first_name, last_name, day_of_week, mid) AS (
        SELECT
            u.username,
            u.first_name,
            u.last_name,
            to_char(m.send_date, 'Day'),
            m.id
        FROM
            CHAT_TITLE,
            messages m
        INNER JOIN
            users u
            ON u.id = m.sender_user_id
        INNER JOIN
            chats c
            ON c.id = m.chat_id
        WHERE
            c.title = chat_title
    ),
    TOTAL(total_count) AS (SELECT COUNT(mid) FROM SELECTION)
SELECT
    username,
    first_name,
    last_name,
    day_of_week,
    COUNT(mid) message_count,
    round(CAST(COUNT(mid) AS numeric) / total_count * 100, 2) prop
FROM
   TOTAL, SELECTION
GROUP BY
    (username, first_name, last_name, day_of_week, total_count)
ORDER BY
    message_count DESC;

-- A better ordering would be if we could first order by the user but
-- taken from a view where users are ordered by the number of messages they send
-- and then within that order days by the message_count, this can be achieved
-- if we use that users_by_message_count view for selecting and ordering users
WITH
    CHAT_TITLE(chat_title) AS (SELECT 'Telegram'),
    SELECTION(umessage_count, username, first_name, last_name, day_of_week, mid) AS (
        SELECT
            u.message_count,
            u.username,
            u.first_name,
            u.last_name,
            to_char(m.send_date, 'Day'),
            m.id
        FROM
            CHAT_TITLE,
            messages m
        INNER JOIN
            users_by_message_count u
            ON u.uid = m.sender_user_id
        INNER JOIN
            chats c
            ON c.id = m.chat_id
        WHERE
            c.title = chat_title
    ),
    TOTAL(total_count) AS (SELECT COUNT(mid) FROM SELECTION)
SELECT
    username,
    first_name,
    last_name,
    day_of_week,
    COUNT(mid) message_count,
    round(CAST(COUNT(mid) AS numeric) / total_count * 100, 2) prop
FROM
   TOTAL, SELECTION
GROUP BY
    (umessage_count, username, first_name, last_name, day_of_week, total_count)
ORDER BY
    umessage_count DESC, message_count DESC;


-- Let's research the activity by months of the year:
-- What is the most active month of the year in a given chat?
WITH
    CHAT_TITLE(chat_title) AS (SELECT 'Telegram'),
    SELECTION(month, mid) AS (
        SELECT
            to_char(m.send_date, 'Month'),
            m.id
        FROM
            CHAT_TITLE,
            messages m
        INNER JOIN
            chats c
            ON c.id = m.chat_id
        WHERE
            c.title = chat_title
    ),
    TOTAL(total_count) AS (SELECT COUNT(mid) FROM SELECTION)
SELECT
    month,
    COUNT(mid) message_count,
    round(CAST(COUNT(mid) AS numeric) / total_count * 100, 2) prop
FROM
   TOTAL, SELECTION
GROUP BY
    (month, total_count)
ORDER BY
    message_count DESC;

-- but here the data is severely skewed due to recent higher activity,
-- however let's also group by people and order them appropriately:
WITH
    CHAT_TITLE(chat_title) AS (SELECT 'Telegram'),
    SELECTION(umessage_count, username, first_name, last_name, month, mid) AS (
        SELECT
            u.message_count,
            u.username,
            u.first_name,
            u.last_name,
            to_char(m.send_date, 'Month'),
            m.id
        FROM
            CHAT_TITLE,
            messages m
        INNER JOIN
            users_by_message_count u
            ON u.uid = m.sender_user_id
        INNER JOIN
            chats c
            ON c.id = m.chat_id
        WHERE
            c.title = chat_title
    ),
    TOTAL(total_count) AS (SELECT COUNT(mid) FROM SELECTION)
SELECT
    username,
    first_name,
    last_name,
    month,
    COUNT(mid) message_count,
    round(CAST(COUNT(mid) AS numeric) / total_count * 100, 2) prop
FROM
   TOTAL, SELECTION
GROUP BY
    (umessage_count, username, first_name, last_name, month, total_count)
ORDER BY
    umessage_count DESC, message_count DESC;


-- Let's research the types of messages in a given chat:
WITH
    CHAT_TITLE(chat_title) AS (SELECT 'Telegram'),
    SELECTION(content_type, mid) AS (
        SELECT
            m.content_type,
            m.id
        FROM
            CHAT_TITLE,
            messages m
        INNER JOIN
            chats c
            ON c.id = m.chat_id
        WHERE
            c.title = chat_title
    ),
    TOTAL(total_count) AS (SELECT COUNT(mid) FROM SELECTION)
SELECT
    content_type,
    COUNT(mid) message_count,
    round(CAST(COUNT(mid) AS numeric) / total_count * 100, 2) prop
FROM
   TOTAL, SELECTION
GROUP BY
    (content_type, total_count)
ORDER BY
    message_count DESC;

-- Let's join with users:
WITH
    CHAT_TITLE(chat_title) AS (SELECT 'Telegram'),
    SELECTION(umessage_count, username, first_name, last_name, content_type, mid) AS (
        SELECT
            u.message_count,
            u.username,
            u.first_name,
            u.last_name,
            m.content_type,
            m.id
        FROM
            CHAT_TITLE,
            messages m
        INNER JOIN
            users_by_message_count u
            ON u.uid = m.sender_user_id
        INNER JOIN
            chats c
            ON c.id = m.chat_id
        WHERE
            c.title = chat_title
    ),
    TOTAL(total_count) AS (SELECT COUNT(mid) FROM SELECTION)
SELECT
    username,
    first_name,
    last_name,
    content_type,
    COUNT(mid) message_count,
    round(CAST(COUNT(mid) AS numeric) / total_count * 100, 2) prop
FROM
   TOTAL, SELECTION
GROUP BY
    (umessage_count, username, first_name, last_name, content_type, total_count)
ORDER BY
    umessage_count DESC, message_count DESC;