import requests
import psycopg2
from datetime import datetime

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

def get_schema():
    return {'name': 'string', 'start_time': 'datetime', 'handle': 'string', 'fid': 'string', 'attending_count': 'integer', 'can_guests_invite': 'boolean', 'category': 'string', 'declined_count': 'integer', 'guest_list_enabled': 'boolean', 'interested_count': 'integer', 'is_canceled': 'boolean', 'is_page_owned': 'boolean', 'is_viewer_admin': 'boolean', 'maybe_count': 'integer', 'noreply_count': 'integer', 'timezone': 'string', 'end_time': 'datetime', 'updated_time': 'datetime', 'type': 'string', 'venue_fid': 'string', 'venue_name': 'string', 'venue_city': 'string', 'venue_state': 'string', 'venue_country': 'string', 'venue_latitude': 'string', 'venue_longitude': '', 'geometry': 'string', 'venue_capacity': 'integer'}

def get_access_token():
    oauth_response = requests.post(FACEBOOK_ACCESS_TOKEN_URL, data={ 'client_id': APP_CLIENT_ID, 'client_secret': APP_CLIENT_SECRET, 'grant_type': GRANT_TYPE } )
    token = oauth_response.json()['access_token']
    global access_token
    access_token = token
    return token

def get_api_response(url):
    response = requests.get(url)
    return response.json()

def crawl_all_events():
    for handle in TEST_HANDLE_LIST:
        crawl_page_events(handle)

def crawl_page_events(handle):
    url = FACEBOOK_GRAPH_API + handle + "/events?access_token=" + access_token + "&debug=all&format=json&method=get&pretty=0&suppress_http_code=1"
    while url != None:
        full_response = get_api_response(url)
        paginated_response = get_api_response(url)['data']
        for event in paginated_response:
            enriched_event = enrich_event(event["id"], handle)
            persist_event(stringify_schema(enriched_event))
        url = full_response['paging']['next']

def enrich_event(event_id, handle):
    event_url = FACEBOOK_GRAPH_API + event_id + "?access_token=" + access_token + "&fields=attending_count,can_guests_invite,category,declined_count,end_time,guest_list_enabled,interested_count,is_canceled,is_page_owned,is_viewer_admin,maybe_count,name,noreply_count,parent_group,place,start_time,ticket_uri,timezone,type,updated_time&debug=all&format=json&method=get&pretty=0&suppress_http_code=1"
    response = get_api_response(event_url)

    # print(response)
    response['fid'] = response['id']
    response['handle'] = handle
    response['geometry'] = None
    response['venue_capacity'] = None

    if response['place'] != None:
        place = response['place']
        response['venue_fid'] = place['id']
        response['venue_name'] = place['name']
        if place['location'] != None:
            location = place['location']
            response['venue_city'] = location['city']
            response['venue_state'] = location['state']
            response['venue_country'] = location['country']
            response['venue_latitude'] = location['latitude']
            response['venue_longitude'] = location['longitude']

    del response['id']
    del response['place']
    del response['ticket_uri']
    del response['__debug__']

    return response

def persist_event(stringified_event):
    cursor = pg_connection.cursor()
    sql = "INSERT INTO events (" + stringified_event[0] + ") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    values = tuple(stringified_event[1])
    print(sql)
    print("-----------")
    print(values)
    print("|||||||||||||||||||||||||||||||||||")
    cursor.execute(sql, values)

def stringify_schema(response_dict):
    stringified_value = []
    column_string = ""
    schema = get_schema()
    for k, v in response_dict.items():
        if v != None:
            column_string = column_string + k + ","
            stringified_value.append(v)
            # if schema[k] == 'integer':
            #     stringified_value = stringified_value + str(v)
            # elif schema[k] == 'boolean':
            #     stringified_value = stringified_value + str(v)
            # else:
            #     stringified_value = stringified_value + "'" + str(v) + "'"
            # stringified_value = stringified_value + ","

    column_string += 'created_at,updated_at'
    current_time = str(datetime.now().replace(microsecond=0).isoformat())
    stringified_value.append(current_time)
    stringified_value.append(current_time)
    # stringified_value += "'" + current_time + "','" + current_time + "'"
    return column_string, stringified_value

def postgres_connection():
    try:
        connection = psycopg2.connect("dbname={} user={} host={} password={}".format(DATABASE, USER, HOST, PASSWORD))
    except:
        print("DATABASE NOT ACCESSIBLE")

    return connection;


access_token = get_access_token()
pg_connection = postgres_connection()

crawl_page_events('thegarden')

pg_connection.commit();

# try:
#     conn = psycopg2.connect("dbname='revmax_development' user='nandukalidindi' host='localhost' password='qwerty123'")
# except:
#     print("I am unable to connect to the database")
#
# cur = conn.cursor()
#
# k = "category,declined_count,fid,is_canceled,venue_longitude,maybe_count,type,is_page_owned,venue_latitude,guest_list_enabled,name,timezone,interested_count,handle,venue_city,noreply_count,venue_state,attending_count,start_time,can_guests_invite,is_viewer_admin,venue_name,venue_fid,venue_country,updated_time,created_at,updated_at"
# p = "'SPORTS_EVENT',0,'1073154732805592',False,'-73.992761112223',23,'public',False,'40.750579714048',True,'World Championship Boxing:  Gennady Golovkin vs. Daniel Geale','America/New_York',23,'thegarden','New York',0,'NY',2,'2038-11-30T14:00:00-0500',True,False,'The Garden','28859306498','United States','2016-10-21T23:40:48+0000','2017-01-12 04:09:30.004935','2017-01-12 04:09:30.004935'"
#
# sql = """INSERT INTO events (category,declined_count,fid,is_canceled,venue_longitude,maybe_count,type,is_page_owned,venue_latitude,guest_list_enabled,name,timezone,interested_count,handle,venue_city,noreply_count,venue_state,attending_count,start_time,can_guests_invite,is_viewer_admin,venue_name,venue_fid,venue_country,updated_time,created_at,updated_at) VALUES ('SPORTS_EVENT',0,'1073154732805592',False,'-73.992761112223',23,'public',False,'40.750579714048',True,'World Championship Boxing:  Gennady Golovkin vs. Daniel Geale','America/New_York',23,'thegarden','New York',0,'NY',2,'2038-11-30T14:00:00-0500',True,False,'The Garden','28859306498','United States','2016-10-21T23:40:48+0000','2017-01-12 04:09:30.004935','2017-01-12 04:09:30.004935');"""
#
# cur.execute(sql)
#
# conn.commit()

# cur.execute("""INSERT INTO events(handle,created_at,updated_at) VALUES ('handle','2016-10-21T23:40:48+0000','2016-10-21T23:40:48+0000');""")

# rows = cur.fetchall()

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
