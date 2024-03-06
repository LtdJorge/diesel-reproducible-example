-- Your SQL goes here
create table codigos
(
    id            serial primary key,
    created_at_ts timestamp default now() not null,
    code          varchar(26)             not null,
    expires_at    date                    not null,
    expired       bool      default false not null
);
