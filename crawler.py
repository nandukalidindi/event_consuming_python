import requests
import psycopg2

FACEBOOK_ACCESS_TOKEN_URL = "https://graph.facebook.com/v2.6/oauth/access_token"
FACEBOOK_GRAPH_API = "https://graph.facebook.com/v2.8/"

APP_CLIENT_ID = "587748278082312"
APP_CLIENT_SECRET = "653f038ce5648e38a231609066407d86"
GRANT_TYPE = "client_credentials"

HOST = "ec2-54-205-81-141.compute-1.amazonaws.com"
DATABASE = "revmax_dev"
USER = "dev_master"
PASSWORD = "master01"

def get_access_token():
    oauth_response = requests.post(FACEBOOK_ACCESS_TOKEN_URL, data={ 'client_id': APP_CLIENT_ID, 'client_secret': APP_CLIENT_SECRET, 'grant_type': GRANT_TYPE } )
    return oauth_response.json()['access_token']

def get_api_response(access_token, endpoint):
    response = requests.get(FACEBOOK_GRAPH_API + "thegarden/events?access_token=" + access_token + "&debug=all&format=json&method=get&pretty=0&suppress_http_code=1")
    return response.json()


print(get_api_response(get_access_token(), ""))
# print(get_access_token())

# try:
#     conn = psycopg2.connect("dbname='revmax_dev' user='dev_master' host='ec2-54-205-81-141.compute-1.amazonaws.com' password='master01'")
# except:
#     print("I am unable to connect to the database")
#
# cur = conn.cursor()
#
# cur.execute("""SELECT count(*) from events""")
#
# rows = cur.fetchall()
#
# print("\nShow me the databases:\n")
# for row in rows:
#     print("   ", row[0])

# r = requests.post("https://graph.facebook.com/v2.6/oauth/access_token", data={'client_id': '587748278082312', 'client_secret': '653f038ce5648e38a231609066407d86', 'grant_type': 'client_credentials'})
#
# token = r.json()['access_token']
#
# response = requests.get("https://graph.facebook.com/v2.8/thegarden/events?access_token=" + token + "&debug=all&format=json&method=get&pretty=0&suppress_http_code=1")
#
#
# print(response.json())
