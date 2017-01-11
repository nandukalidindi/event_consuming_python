import requests
import psycopg2

def get_access_token


try:
    conn = psycopg2.connect("dbname='revmax_dev' user='dev_master' host='ec2-54-205-81-141.compute-1.amazonaws.com' password='master01'")
except:
    print("I am unable to connect to the database")

cur = conn.cursor()

cur.execute("""SELECT count(*) from events""")

rows = cur.fetchall()

print("\nShow me the databases:\n")
for row in rows:
    print("   ", row[0])

# r = requests.post("https://graph.facebook.com/v2.6/oauth/access_token", data={'client_id': '587748278082312', 'client_secret': '653f038ce5648e38a231609066407d86', 'grant_type': 'client_credentials'})
#
# token = r.json()['access_token']
#
# response = requests.get("https://graph.facebook.com/v2.8/thegarden/events?access_token=" + token + "&debug=all&format=json&method=get&pretty=0&suppress_http_code=1")
#
#
# print(response.json())
