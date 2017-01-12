import requests
import psycopg2

HANDLES_LIST = ['nyrangers', 'thegarden', 'terminal5nyc', 'websterhallnyc', 'brooklynbowl', 'thestudioatwebsterhall', 'barclayscenter', 'townhallnyc', 'radiocitymusichall', 'boweryballroom', 'mercuryloungeny', 'cakeshopnyc', '92YConcerts', 'pianosnyc', 'citywinerynyc', 'goodroombk', 'madisonsquarepark', 'centralparknyc', 'summerstagenyc', 'prospectparkbrooklyn', 'apollotheater', 'beacontheatre', 'carnegiehall', 'bluenotenyc', 'jazzstandard', 'theiridium', 'dizzysclubcocacola', 'bbkingbluesnyc', 'outputclub', 'nyuskirball', 'musichallofwb', 'knittingfactorybrooklyn', 'javitscenter', 'smorgasburg', 'bkbazaar', 'babysallright', 'theboweryelectric', 'stvitusbar', 'lprnyc', 'leftfield', 'highlineballroom', 'irvingplaza', 'warsawconcerts', 'popgunpresents', 'roughtradenyc', 'gramercytheatre', 'subrosanyc', 'brooklyncenterfortheperformingarts', 'wickedwillysnyc', 'maxwellshoboken', 'thebellhouseny', 'dromny', 'cmoneverybodybk', 'rooftopfilmsinc', 'sobsnyc', 'blackbearbk', 'therockshop', 'nationalsawdust', 'sheastadiumbk', 'cuttingroomnyc', 'silentbarn', 'lavony', 'thehallbrooklyn', 'marqueeny', 'theshopbk', 'transpecos', 'rockwoodmusichall', 'cieloclub', 'slakenyc', 'spaceibizanewyork', 'birdlandjazzclub', 'foresthillsstadium', 'littlefieldnewyorkcity', 'kingstheatrebklyn', 'metopera', 'prucenter', 'yankeestadium0', 'yankees', 'mets', 'metlifestadium', 'stage48', 'houseofyes', 'nyknicks', 'newyorkislanders', 'newyorkgiants', 'nyliberty', 'newyorkriveters', '4040club', 'yiddishnewyork', 'nikonjbt']

TEST_HANDLE_LIST = ['nyrangers']#, 'thegarden', 'terminal5nyc', 'websterhallnyc', 'brooklynbowl']

FACEBOOK_ACCESS_TOKEN_URL = "https://graph.facebook.com/v2.6/oauth/access_token"
FACEBOOK_GRAPH_API = "https://graph.facebook.com/v2.8/"

APP_CLIENT_ID = "587748278082312"
APP_CLIENT_SECRET = "653f038ce5648e38a231609066407d86"
GRANT_TYPE = "client_credentials"

# HOST = "ec2-54-205-81-141.compute-1.amazonaws.com"
# DATABASE = "revmax_dev"
# USER = "dev_master"
# PASSWORD = "master01"

HOST = "localhost"
DATABASE = "revmax_development"
USER = "nandukalidindi"
PASSWORD = "qwerty123"

access_token = ""


def schema():
    return {'created_at': 'datetime', 'updated_at': 'datetime', 'name': 'string', 'start_time': 'datetime', 'handle': 'string', 'fid': 'string', 'attending_count': 'integer', 'can_guests_invite': 'boolean', 'category': 'string', 'declined_count': 'integer', 'guest_list_enabled': 'boolean', 'interested_count': 'integer', 'is_canceled': 'boolean', 'is_page_owned': 'boolean', 'is_viewer_admin': 'boolean', 'maybe_count': 'integer', 'noreply_count': 'integer', 'timezone': 'string', 'end_time': 'datetime', 'updated_time': 'datetime', 'type': 'string', 'venue_fid': 'string', 'venue_name': 'string', 'venue_city': 'string', 'venue_state': 'string', 'venue_country': 'string', 'venue_latitude': 'string', 'venue_longitude': '', 'geometry': 'geometry', 'venue_capacity': 'integer'}



def get_access_token():
    oauth_response = requests.post(FACEBOOK_ACCESS_TOKEN_URL, data={ 'client_id': APP_CLIENT_ID, 'client_secret': APP_CLIENT_SECRET, 'grant_type': GRANT_TYPE } )
    token = oauth_response.json()['access_token']
    global access_token
    access_token = token
    return token

def get_api_response(access_token, endpoint, query_param_string):
    response = requests.get(FACEBOOK_GRAPH_API + endpoint + "?access_token=" + access_token + query_param_string + "&debug=all&format=json&method=get&pretty=0&suppress_http_code=1")
    return response.json()

def crawl_all_events():
    for handle in TEST_HANDLE_LIST:
        crawl_page_events(handle)


def crawl_page_events(handle):
    url = ""
    while url != None:
        paginated_response = get_api_response(access_token, url)
        for event in paginated_response:
            enriched_event = enriched_event(event["id"], handle)
            persist_event(enriched_event)

def enrich_event(event_id, handle):
    event_endpoint = str(event_id)
    query_param_string = "&debug=all&fields=attending_count,can_guests_invite,category,declined_count,end_time,guest_list_enabled,interested_count,is_canceled,is_page_owned,is_viewer_admin,maybe_count,name,noreply_count,parent_group,place,start_time,ticket_uri,timezone,type,updated_time&"
    response = get_api_response(access_token, event_endpoint, query_param_string)

    response['fid'] = response['id']
    response['handle'] = handle

    if response['place'] != None:
        place = response['place']
        response['venue_fid'] = place.get('id', "")
        response['venue_name'] = place.get('name', "")
        if place['location'] != None:
            location = place['location']
            response['venue_city'] = location.get('city', "")
            response['venue_state'] = location.get('state', "")
            response['venue_country'] = location.get('country', "")
            response['venue_latitude'] = location.get('latitude', "")
            response['venue_longitude'] = location.get('longitude', "")

    del response['id']
    del response['place']
    del response['__debug__']

    return response


def persist_event(pg_connection):
    cursor = pg_connection.cursor()
    cursor.execute("""INSERT INTO events(column_name) VALUES(values)""")

def postgres_connector_cursor():
    try:
        connection = psycopg2.connect("dbname={} user={} host={} password={}".format(DATABASE, USER, HOST, PASSWORD))
    except:
        print("DATABASE NOT ACCESSIBLE")

    return connection;

def event_schema():
    return {""}


get_access_token()
d = enrich_event('1073154732805592', 'thegarden')
for k, v in d.items():
    print(k, v)

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
