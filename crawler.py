import requests
import psycopg2

HANDLES_LIST = ['nyrangers', 'thegarden', 'terminal5nyc', 'websterhallnyc', 'brooklynbowl', 'thestudioatwebsterhall', 'barclayscenter', 'townhallnyc', 'radiocitymusichall', 'boweryballroom', 'mercuryloungeny', 'cakeshopnyc', '92YConcerts', 'pianosnyc', 'citywinerynyc', 'goodroombk', 'madisonsquarepark', 'centralparknyc', 'summerstagenyc', 'prospectparkbrooklyn', 'apollotheater', 'beacontheatre', 'carnegiehall', 'bluenotenyc', 'jazzstandard', 'theiridium', 'dizzysclubcocacola', 'bbkingbluesnyc', 'outputclub', 'nyuskirball', 'musichallofwb', 'knittingfactorybrooklyn', 'javitscenter', 'smorgasburg', 'bkbazaar', 'babysallright', 'theboweryelectric', 'stvitusbar', 'lprnyc', 'leftfield', 'highlineballroom', 'irvingplaza', 'warsawconcerts', 'popgunpresents', 'roughtradenyc', 'gramercytheatre', 'subrosanyc', 'brooklyncenterfortheperformingarts', 'wickedwillysnyc', 'maxwellshoboken', 'thebellhouseny', 'dromny', 'cmoneverybodybk', 'rooftopfilmsinc', 'sobsnyc', 'blackbearbk', 'therockshop', 'nationalsawdust', 'sheastadiumbk', 'cuttingroomnyc', 'silentbarn', 'lavony', 'thehallbrooklyn', 'marqueeny', 'theshopbk', 'transpecos', 'rockwoodmusichall', 'cieloclub', 'slakenyc', 'spaceibizanewyork', 'birdlandjazzclub', 'foresthillsstadium', 'littlefieldnewyorkcity', 'kingstheatrebklyn', 'metopera', 'prucenter', 'yankeestadium0', 'yankees', 'mets', 'metlifestadium', 'stage48', 'houseofyes', 'nyknicks', 'newyorkislanders', 'newyorkgiants', 'nyliberty', 'newyorkriveters', '4040club', 'yiddishnewyork', 'nikonjbt']

TEST_HANDLE_LIST = ['nyrangers']#, 'thegarden', 'terminal5nyc', 'websterhallnyc', 'brooklynbowl']

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

def crawl_all_events():
    for handle in TEST_HANDLE_LIST:
        crawl_page_events(handle)


def crawl_page_events(handle):
    url = ""
    while url != None:
        paginated_response = get_api_response(access_token, url)
        for event in paginated_response:
            enriched_event = enriched_event(event["id"])
            persist_event(enriched_event)

def enrich_event(event_id):

def persist_event(pg_connection):
    cursor = pg_connection.cursor()
    cursor.execute("""INSERT INTO events(column_name) VALUES(values)""")

def postgres_connector_cursor():
    try:
        connection = psycopg2.connect("dbname={} user={} host={} password={}".format(DATABASE, USER, HOST, PASSWORD))
    except:
        print("DATABASE NOT ACCESSIBLE")

    return connection;


print(get_api_response(get_access_token(), ""))
print(get_access_token())

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
