import sqlite3
import json
import datetime

conn = sqlite3.connect('events.db') 
cursor = conn.cursor()


cursor.execute('''
    create table if not exists events (
        event_id INTEGER PRIMARY KEY,
        event_type STRING,
        event_timestamp INTEGER,
        user_id STRING,
        country STRING,
        name STRING,
        device_os STRING,
        marketing_campaign STRING,
        transaction_amount FLOAT,
        transaction_currency STRING
    )
''')


with open('Data/events.jsonl', 'r') as jsonl_file:
    for line in jsonl_file:
        data = json.loads(line)
        event_id = data.get('event_id')
        event_type = data.get('event_type')
        event_timestamp = data.get('event_timestamp')
        user_data = data.get('event_data')
        user_id = user_data.get('user_id')
        country = user_data.get('country')
        name = user_data.get('name')
        device_os = user_data.get('device_os')
        marketing_campaign = user_data.get('marketing_campaign')
        transaction_amount= user_data.get('transaction_amount')
        transaction_currency=user_data.get('transaction_currency')
        
        if event_type == 'registration' and (user_id is None or country is None or device_os is None ):
            continue
        elif event_type == 'transaction' and (user_id is None or transaction_amount is None or transaction_currency is None):
            continue
        elif event_type in ['login', 'logout'] and (user_id is None):
            continue

        cursor.execute('insert into events (event_id, event_type, event_timestamp, user_id, country, name, device_os, marketing_campaign, transaction_amount, transaction_currency) VALUES (?, ?, ?, ?, ?, ?, ?, ?,?,?)',
                       (event_id, event_type, event_timestamp, user_id, country, name, device_os, marketing_campaign,transaction_amount,transaction_currency))

cursor.execute("delete from events where event_id in (select event_id from events where user_id not in (select user_id from events where event_type=='registration'))")

cursor.execute("delete from events where event_id in (select event_id from events group by event_type,event_timestamp,user_id having count(*)>1)")

cursor.execute("create table users as select distinct user_id,name,country,device_os,marketing_campaign,cast(STRFTIME('%Y', DATETIME(event_timestamp, 'unixepoch')) as INTEGER) as event_year,cast(STRFTIME('%m', DATETIME(event_timestamp, 'unixepoch')) as INTEGER) as event_month,cast(STRFTIME('%d', DATETIME(event_timestamp, 'unixepoch')) as INTEGER) as event_day from events where event_type=='registration'")

cursor.execute("select distinct user_id from events")
user_ids = set(row[0] for row in cursor.fetchall())

def checker(events,cursor,user_id,invalid_event):
    match events:
        case [head]:
            invalid_event.append(events[0][0])
            return invalid_event
        case []:
            return invalid_event
        case [head,second,*tail]:
            if head[1] == 'login' and second[1] == 'logout':
                session = second[2]-head[2]
                converted_time=datetime.datetime.utcfromtimestamp(head[2])
                cursor.execute('insert into session (session_duration,user_id,login_year,login_month,login_day,login_event_id,login_timestamp,logout_event_id) VALUES (?,?, ?,?,?,?,?,?)', (session,user_id,converted_time.year,converted_time.month,converted_time.day,head[0],head[2],second[0]))
                checker(events[2:],cursor,user_id,invalid_event)
            else:
                invalid_event.append(events[0][0])
                checker(events[1:],cursor,user_id,invalid_event)


cursor.execute("create table incorrect (event_id INTEGER PRIMARY KEY)")
cursor.execute("create table session (session_id INTEGER PRIMARY KEY AUTOINCREMENT,session_duration INTEGER, user_id STRING,login_year INTEGER,login_month INTEGER,login_day INTEGER,login_event_id,login_timestamp INTEGER,INTEGER,logout_event_id INTEGER)")
cursor.execute("""create table transactions as select
    user_id,
    case
        when transaction_currency = 'EUR' then transaction_amount * 1.3 
        else transaction_amount 
    end as amount,
    cast(STRFTIME('%Y', DATETIME(event_timestamp, 'unixepoch')) as INTEGER) as event_year,
    cast(STRFTIME('%m', DATETIME(event_timestamp, 'unixepoch')) as INTEGER) as event_month,
    cast(STRFTIME('%d', DATETIME(event_timestamp, 'unixepoch')) as INTEGER) as event_day
    from events
    where event_type = 'transaction'""")

for user_id in user_ids:
    cursor.execute("""
        select event_id,event_type,event_timestamp from events
        where user_id=? and event_type in ('login', 'logout')
        ORDER BY event_timestamp ASC
    """, (user_id,))

    events = [(event[0], event[1], event[2]) for event in cursor.fetchall()]

    if not events:
        continue
    
    invalid_event=[]
    checker(events,cursor,user_id,invalid_event)
    for el in invalid_event:
        cursor.execute('insert into incorrect (event_id) VALUES (?)',(int(el),))


cursor.execute("delete from events where event_id in (select * from incorrect)")
cursor.execute("drop table incorrect")

conn.commit()


conn.close()
