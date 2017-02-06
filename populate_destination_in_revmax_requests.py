import psycopg2

HOST = "localhost"
DATABASE = "revmax_development"
USER = "nandukalidindi"
PASSWORD = "qwerty123"

def postgres_connection():
    try:
        connection = psycopg2.connect("dbname={} user={} host={} password={}".format(DATABASE, USER, HOST, PASSWORD))
    except:
        print("Unable to connect database. Please try again!")

    return connection

pg_connection = postgres_connection()
cursor = pg_connection.cursor()

select_sql = "SELECT request_id, destination FROM revmax_requests"

cursor.execute(select_sql)

rows = cursor.fetchall()

for row in rows:
    select_random_sql = "SELECT dropoff_longitude, dropoff_latitude FROM yellow_cabs ORDER BY random() LIMIT 1"
    cursor.execute(select_random_sql)
    geometry = cursor.fetchone()
    print(geometry[0])
    print(geometry[1])
    # update_sql = "UPDATE revmax_requests SET destination=%s WHERE request_id=%s"
    # cursor.execute(update_sql, [geometry, row[0]])
    # pg_connection.commit()
