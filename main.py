#python -m venv env
#env\Scripts\activate
#pip install Pandas
#pip install sqlalchemy
#pip install psycopg2-binary

#pip install openpyxl

import pandas as pd
from sqlalchemy import create_engine, DECIMAL
import psycopg2
import json

with open("cred.json", "r") as f:
    cred = json.load(f)

url = f"postgresql://{cred['user']}:{cred['password']}@{cred['host']}:{cred['port']}/{cred['database']}"
engine = create_engine(url)
conn = psycopg2.connect(**cred)
cursor = conn.cursor()

# with open("ddl_dml.sql", "r", encoding="utf-8") as file:
#     sql_script = file.read()

# cursor.execute('SET search_path TO final;') 
# cursor.execute(sql_script)
# conn.commit()

import os
from pathlib import Path
import shutil


# создание таблиц в БД
def create_tables_DB(path):
    with open(path, "r", encoding="utf-8") as file:
        sql_script = file.read()
    cursor.execute('SET search_path TO final;') 
    cursor.execute(sql_script)
    conn.commit()

create_tables_DB("create_tables.sql")

# TRANSACTIONS & PASSPORTS & TERMINALS
#Загрузка данных каждого дня
def passports(path):
    df = pd.read_excel(path)
    df.to_sql(name="stg_passports", con=engine, schema="final", if_exists="replace", index=False)
    backup_path = path + '.backup'
    os.rename(path, backup_path)
    
    archive_dir = 'archive'
    os.makedirs(archive_dir, exist_ok=True)
    # Перемещаем backup файл в архив
    archive_file_path = os.path.join(archive_dir, os.path.basename(backup_path))
    shutil.move(backup_path, archive_file_path)

    conn.commit()


def terminals(path):
    df = pd.read_excel(path)
    df.to_sql(name="stg_terminals", con=engine, schema="final", if_exists="replace", index=False)
    backup_path = path + '.backup'
    os.rename(path, backup_path)
    
    archive_dir = 'archive'
    os.makedirs(archive_dir, exist_ok=True)
    # Перемещаем backup файл в архив
    archive_file_path = os.path.join(archive_dir, os.path.basename(backup_path))
    shutil.move(backup_path, archive_file_path)

    conn.commit()


def transactions(path):
    df = pd.read_csv(path, sep=';')
    df['amount'] = df['amount'].str.replace(',', '.').astype(float)
    df['amount'] = df['amount'].round(2)
    # df['amount'] = df['amount'].str.replace(',', '.').astype(str)
    # df['card_num'] = df['card_num'].str.replace(' ', '')
    df['transaction_date'] = pd.to_datetime(df['transaction_date'])

    df.to_sql(name="stg_transactions", con=engine, schema="final", 
              if_exists="replace", index=False, dtype={'amount': DECIMAL(10, 2)})
    
    backup_path = path + '.backup'
    os.rename(path, backup_path)

    archive_dir = 'archive'
    os.makedirs(archive_dir, exist_ok=True)
    # Перемещаем backup файл в архив
    archive_file_path = os.path.join(archive_dir, os.path.basename(backup_path))
    shutil.move(backup_path, archive_file_path)

    conn.commit()

#Загрузка данных 1го дня
passports("passport_blacklist_01032021.xlsx")
terminals("terminals_01032021.xlsx")
transactions("transactions_01032021.txt")


#наполнение таблиц в БД
#TRANSACTIONS & PASSPORTS просто дополняем,
#TERMINALS загружаем инкремент

def update_tables_DB(path):
    with open(path, "r", encoding="utf-8") as file:
        sql_script = file.read()
    cursor.execute('SET search_path TO final;') 
    cursor.execute(sql_script)
    conn.commit()

update_tables_DB("update_tables.sql")

#оказалось, что таблицы каждого дня с паспортами созданы нарастающим итогом, 
# поэтому в БД данные предыдущих дней повторяются. 
# Но я решила не менять код, т.к. на итоговый результат это не влияет 
# и таблица небольшая.

#ОТЧЕТ
def create_fraud_table():
    cursor.execute('SET search_path TO final;') 
    cursor.execute("""
        create table if not exists REP_FRAUD (
            event_dt timestamp,
            passport varchar (30),
            fio varchar (100),
            phone varchar (20),
            event_type varchar (100),
            report_dt timestamp);
        """)

    conn.commit()

create_fraud_table()

#находим мошенников по 1му признаку - просроченный паспорт:
def create_stg_fraud():
    cursor.execute('SET search_path TO final;') 
    cursor.execute('drop table if exists stg_fraud;')
    cursor.execute("""
        create table stg_fraud (
        transaction_date timestamp,
        card_num varchar (30),
        oper_result varchar (30),
        account varchar (30),
        client varchar (30),
        passport_valid_to date); """)

    conn.commit()

create_stg_fraud()

def update_stg_fraud():
    cursor.execute('SET search_path TO final;') 
    cursor.execute("""
        INSERT INTO final.stg_fraud 
        (transaction_date, 
        card_num, 
        oper_result, 
        account, 
        client, 
        passport_valid_to)
            select
            t1.transaction_date ,
            t1.card_num,
            t1.oper_result,
            t2.account,
            t3.client,
            t4.passport_valid_to    
            from DWH_FACT_TRANSACTIONS t1
        join cards t2
        on t1.card_num = t2.card_num
        join accounts t3
        on t2.account = t3.account
        join clients t4
	    on t3.client = t4.client_id;        
    """)
    conn.commit()

update_stg_fraud()

def update_fraud_table():
    cursor.execute('SET search_path TO final;') 
    cursor.execute("""
        insert into final.rep_fraud (
            event_dt,
            passport,
            fio,
            phone,
            event_type,
            report_dt)
            select
                t1.transaction_date,
                t2.passport_num,
                t2.last_name,
                t2.phone,
                'passport not valid',
                t1.transaction_date 
            from stg_fraud t1
            join final.clients t2
            on t1.client = t2.client_id
            where t2.passport_valid_to < t1.transaction_date - INTERVAL '1 day';
        """)

    conn.commit()

update_fraud_table()

#находим мошенников по 2му признаку - недействующий договор:
def create_stg2_fraud():
    cursor.execute('SET search_path TO final;') 
    cursor.execute('drop table if exists stg2_fraud;')
    cursor.execute("""
        create table stg2_fraud (
        transaction_date timestamp,
        card_num varchar (30),
        oper_result varchar (30),
        account varchar (30),
        client varchar (30),
        valid_to date); """)

    conn.commit()

create_stg2_fraud()

def update_stg2_fraud():
    cursor.execute('SET search_path TO final;') 
    cursor.execute("""
        INSERT INTO final.stg2_fraud 
        (transaction_date, 
        card_num, 
        oper_result, 
        account, 
        client, 
        valid_to)
            select
            t1.transaction_date ,
            t1.card_num,
            t1.oper_result,
            t2.account,
            t3.client,
            t3.valid_to    
            from DWH_FACT_TRANSACTIONS t1
        join cards t2
        on t1.card_num = t2.card_num
        join accounts t3
        on t2.account = t3.account
        where t3.valid_to < t1.transaction_date - INTERVAL '1 day';        
    """)
    conn.commit()

update_stg2_fraud()

def update2_fraud_table():
    cursor.execute('SET search_path TO final;') 
    cursor.execute("""
        insert into final.rep_fraud (
            event_dt,
            passport,
            fio,
            phone,
            event_type,
            report_dt)
            select
                t1.transaction_date,
                t2.passport_num,
                t2.last_name,
                t2.phone,
                'account not valid',
                t1.transaction_date 
            from stg2_fraud t1
            join final.clients t2
            on t1.client = t2.client_id;
        """)

    conn.commit()

update2_fraud_table()

#####

#Загрузка данных 2го дня
passports("passport_blacklist_02032021.xlsx")
terminals("terminals_02032021.xlsx")
transactions("transactions_02032021.txt")

update_tables_DB("update_tables.sql")

#строим отчет 2го дня 
create_stg_fraud()
update_stg_fraud()
update_fraud_table()

create_stg2_fraud()
update_stg2_fraud()
update2_fraud_table()

#####

# Загрузка данных 3го дня
passports("passport_blacklist_03032021.xlsx")
terminals("terminals_03032021.xlsx")
transactions("transactions_03032021.txt")

update_tables_DB("update_tables.sql")

# строим отчет 3го дня 
create_stg_fraud()
update_stg_fraud()
update_fraud_table()

create_stg2_fraud()
update_stg2_fraud()
update2_fraud_table()