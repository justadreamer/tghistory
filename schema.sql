drop table if exists messages; -- comment out once schema is stable
drop table if exists chats; -- comment out once schema is stable
drop table if exists users; -- comment out once schema is stable

create table if not exists messages (
    id bigint,
    send_date timestamp with time zone not null,
    chat_id bigint not null,
    sender_user_id bigint not null,
    content_type varchar(255),
    message_text text, -- if content_type is messageText,
    metadata varchar(255),
    uploaded varchar(255),
    constraint pkey (id, chat_id)
);

create table if not exists chats (
    id bigint primary key,
    title varchar(255),
    username varchar(255)
);

create table if not exists users (
    id bigint primary key,
    first_name varchar(255),
    last_name varchar(255),
    username varchar(255),
    type varchar(255)
);

