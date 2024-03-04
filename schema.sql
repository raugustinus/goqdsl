create table foo (
  uuid    varchar(36) not null primary key default '' || gen_random_uuid(),
  name    varchar(100) not null unique,
  created timestamp not null default now()
);

