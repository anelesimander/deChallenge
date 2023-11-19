import sqlite3
import datetime

def connect_db():
    conn = sqlite3.connect('events.db')
    return conn

def user_info(user_id,cursor=None):
    if not cursor:
        conn = connect_db()
        cursor = conn.cursor()
    
    cursor.execute("select * from users where user_id=?",(user_id,))
    temp=cursor.fetchone()
    return {
        'Country': temp[0],
        'Name': temp[1],
        'Device os':temp[2],
        'Marketing campaign':temp[3],
    }

def get_user_stats(user_id, date=None):
    conn = connect_db()
    cursor = conn.cursor()
    user=user_info(user_id,cursor)

    if date:
        year,month,day= map(int,date.split("."))
        query = "select session_duration,login_timestamp from session where user_id==? and login_year=? and login_month=? and login_day=? order by login_timestamp asc"
        cursor.execute(query, (user_id,year,month,day))
        user_data = [(data[0],data[1]) for data in cursor.fetchall()]
        if not user_data:
            query = "select session_duration, login_timestamp FROM session WHERE user_id==? AND login_day<=? ORDER BY login_timestamp ASC"
            cursor.execute(query, (user_id,day))
            user_data = [(data[0],data[1]) for data in cursor.fetchall()]
            
        if not user_data:
            return {'message': 'User stats not found'}
        print(user_data)
        since_last_login= abs(datetime.datetime.utcfromtimestamp(user_data[len(user_data)-1][1]).day - day)
    else:
        query = "select session_duration,login_year,login_month,login_day from session where user_id==? order by login_timestamp asc"
        cursor.execute(query, (user_id,))
        user_data = [(data[0], data[1], data[2],data[3]) for data in cursor.fetchall()]
        since_last_login=datetime.datetime.utcfromtimestamp(last_log_date).day-user_data[len(user_data)-1][3]

    if not user_data:
        return {'message': 'User stats not found'}

    time_in_session=sum(el[0] for el in user_data)
    num_of_logs=len(user_data)
    user=user_info(user_id,cursor)
    user.update({
    'Time in session': time_in_session,
    'Number of logins': num_of_logs,
    'Since last login' : since_last_login
    })
    conn.close()
    return user


def daily_active_users(cursor,date=None,country=None):
    if country:
        if date:
            year,month,day= map(int,date.split("."))
            query = "select count(distinct user_id) from session where user_id in (select user_id from users where country==?) and login_day==?"
            cursor.execute(query, (country,day))
            users= cursor.fetchall()
        else:
            query = "select count(distinct user_id) from session where user_id in (select user_id from users where country==?)"
            cursor.execute(query, (country,))
            users= cursor.fetchall()
    else:
        if date:
            year,month,day= map(int,date.split("."))
            query = "select count(distinct user_id) from session where and login_day==?"
            cursor.execute(query, (country,day))
            users= cursor.fetchall()
        else:
            query = "select count(distinct user_id) from session"
            cursor.execute(query, (country,))
            users= cursor.fetchall()
    return users
def number_of_logins(cursor,date=None,country=None):
    if country:
        if date:
            year,month,day= map(int,date.split("."))
            query = "select count(user_id) from session where user_id in (select user_id from users where country==?) and login_day==?"
            cursor.execute(query, (country,day))
        else:
            query = "select count(user_id) from session where user_id in (select user_id from users where country==?)"
            cursor.execute(query, (country,))
    else:
        if date:
            year,month,day= map(int,date.split("."))
            query = "select count(user_id) from session where and login_day==?"
            cursor.execute(query, (day,))
        else:
            query = "select count(user_id) from session"
            cursor.execute(query)
    return cursor.fetchall()

def total_revenue(cursor,date=None,country=None):
    if country:
        if date:
            year,month,day= map(int,date.split("."))
            query="select sum(amount) as total_amount from transactions where user_id in (select user_id from users where country==?) and event_day==?"
            cursor.execute(query, (country,day))
        else:
            query = "select sum(amount) as total_amount from transactions where user_id in (select user_id from users where country==?)"       
            cursor.execute(query, (country,))
    else:
        if date:
            year,month,day= map(int,date.split("."))
            query = "select sum(amount) as total_amount from transactions where event_day==?"
            cursor.execute(query, (day,))
        else:
            query = "select sum(amount) as total_amount from transactions "
            cursor.execute(query)
    return cursor.fetchall()

def paid_users(cursor,date=None,country=None):
    if country:
        if date:
            year,month,day= map(int,date.split("."))
            query="select count(user_id) from users where marketing_campaign not null and user_id in (select user_id from users where country==?) and event_day==?"
            cursor.execute(query, (country,day))
        else:
            query="select count(user_id) from users where marketing_campaign not null and user_id in (select user_id from users where country==?)"
            cursor.execute(query, (country,))
    else:
        if date:
            year,month,day= map(int,date.split("."))
            query="select count(user_id) from users where marketing_campaign not null and event_day==?"
            cursor.execute(query, (day,))
        else:
            query="select count(user_id) from users where marketing_campaign not null"
            cursor.execute(query)
    return cursor.fetchall()

def average_number_of_sesions_for_users(cursor,date=None,country=None):
    if country:
        if date:
            year,month,day= map(int,date.split("."))
            query="select avg(session_count) from (select count(*) as session_count from session where user_id in (select user_id from users where country==?) and login_day==? group by user_id having count(*)>=1)"
            cursor.execute(query, (country,day))
        else:
            query="select avg(session_count) from (select count(*) as session_count from session where user_id in (select user_id from users where country==?) group by user_id having count(*)>=1)"
            cursor.execute(query, (country,))
    else:
        if date:
            year,month,day= map(int,date.split("."))
            query="select avg(session_count) from (select count(*) as session_count from session where login_day==? group by user_id having count(*)>=1)"
            cursor.execute(query, (day,))
        else:
            query="select avg(session_count) from (select count(*) as session_count from session group by user_id having count(*)>=1)"
            cursor.execute(query)
    return cursor.fetchall()

def avg_time_in_game(cursor,date=None,country=None):
    if country:
        if date:
            year,month,day= map(int,date.split("."))
            query="select avg(total_time) from (select sum(session_duration) as total_time from session where user_id in (select user_id from users where country==?) and login_day==? group by user_id having sum(session_duration)>=1)"
            cursor.execute(query, (country,day))
        else:
            query="select avg(total_time) from (select sum(session_duration) as total_time from session where user_id in (select user_id from users where country==?) group by user_id having sum(session_duration)>=1)"
            cursor.execute(query, (country,))
    else:
        if date:
            year,month,day= map(int,date.split("."))
            query="select avg(total_time) from (select sum(session_duration) as total_time from session where login_day==? group by user_id having sum(session_duration)>=1)"
            cursor.execute(query, (day,))
        else:
            query="select avg(total_time) from (select sum(session_duration) as total_time from session group by user_id having sum(session_duration)>=1)"
            cursor.execute(query)
    return cursor.fetchall()

def game_statistic(date=None,country=None):
    conn = connect_db()
    cursor = conn.cursor()
    daily_users=daily_active_users(cursor,date,country)[0][0]
    num_login=number_of_logins(cursor,date,country)[0][0]
    revenue=total_revenue(cursor,date,country)[0][0]
    paid=paid_users(cursor,date,country)[0][0]
    avg_num_sess=average_number_of_sesions_for_users(cursor,date,country)[0][0]
    avg_time_in=avg_time_in_game(cursor,date,country)[0][0]

    return {
        'Daily active users': daily_users,
        'Number of logins': num_login,
        'Total revenue':revenue,
        'Paid users':paid,
        'Average number of sessions for users':avg_num_sess,
        'Average total time spent in game':avg_time_in,
    }


if __name__ == '__main__':
    last_log_date=1274573468
    user_id_input = input("Enter user ID: ")
    date_input=input("Enter date like yyyy.mm.dd :")
    date = date_input if date_input else None
    user_stats = get_user_stats(user_id_input,date)
    print("User Stats:")
    print(user_stats)

    # country_input = input("Enter country: ")
    # country=country_input if country_input else None
    # date_input=input("Enter date like yyyy.mm.dd :")
    # date = date_input if date_input else None
    # game_stats=game_statistic(date,country)
    # print("Game Stats:")
    # print(game_stats)

