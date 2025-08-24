create table if not exists DWH_FACT_PASSPORT_BLACKLIST (
    id serial primary key,
    date date,
    passport varchar (30)
);

create table if not exists DWH_FACT_TRANSACTIONS (
    id serial primary key,
    transaction_id varchar (30),
    transaction_date timestamp,
    amount decimal (10,2),
    card_num varchar (30),
    oper_type varchar (30),
    oper_result varchar (30),
    terminal varchar (30)
);

create table if not exists DWH_DIM_terminals_HIST (
    id serial primary key,
    terminal_id varchar (30),
    terminal_type varchar (30),
    terminal_city varchar (50),
    terminal_address varchar (100),
    effective_from timestamp default current_timestamp,
    effective_to timestamp default ('5999-12-31 23:59:59'::timestamp),
    deleted_flg integer default 0
);



