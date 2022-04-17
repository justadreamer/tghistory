-- channels that we don't have data for
select c.id, c.username, min(m.send_date) min_date, max(m.send_date) max_date,
        count(m.id) message_count, count(m.metadata) media_count,
        count(m.uploaded) uploaded_count
        from chats c
        inner join messages m
        on m.chat_id = c.id
        group by c.id
        order by c.username

-- channels that we don't have data for up to the 02-24
with t as
    (select c.id, c.username, min(m.send_date) min_date, max(m.send_date) max_date,
        count(m.id) message_count, count(m.metadata) media_count,
        count(m.uploaded) uploaded_count
        from chats c
        inner join messages m
        on m.chat_id = c.id
        group by c.id
        order by c.username)
select * from t where min_date > '2022-02-24';


-- how many not uploaded videos and photos do we have:
select count(*) from messages m inner join chats c on c.id = m.chat_id where (content_type='photo' or content_type='video') and uploaded is null;

