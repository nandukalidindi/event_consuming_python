import requests
import psycopg2
from datetime import datetime, timedelta
import time
import csv

HANDLES_LIST = ['nyrangers', 'thegarden', 'terminal5nyc', 'websterhallnyc', 'brooklynbowl', 'thestudioatwebsterhall', 'barclayscenter', 'townhallnyc', 'radiocitymusichall', 'boweryballroom', 'mercuryloungeny', 'cakeshopnyc', '92YConcerts', 'pianosnyc', 'citywinerynyc', 'goodroombk', 'madisonsquarepark', 'centralparknyc', 'summerstagenyc', 'prospectparkbrooklyn', 'apollotheater', 'beacontheatre', 'carnegiehall', 'bluenotenyc', 'jazzstandard', 'theiridium', 'dizzysclubcocacola', 'bbkingbluesnyc', 'outputclub', 'nyuskirball', 'musichallofwb', 'knittingfactorybrooklyn', 'javitscenter', 'smorgasburg', 'bkbazaar', 'babysallright', 'theboweryelectric', 'stvitusbar', 'lprnyc', 'leftfield', 'highlineballroom', 'irvingplaza', 'warsawconcerts', 'popgunpresents', 'roughtradenyc', 'gramercytheatre', 'subrosanyc', 'brooklyncenterfortheperformingarts', 'wickedwillysnyc', 'maxwellshoboken', 'thebellhouseny', 'dromny', 'cmoneverybodybk', 'rooftopfilmsinc', 'sobsnyc', 'blackbearbk', 'therockshop', 'nationalsawdust', 'sheastadiumbk', 'cuttingroomnyc', 'silentbarn', 'lavony', 'thehallbrooklyn', 'marqueeny', 'theshopbk', 'transpecos', 'rockwoodmusichall', 'cieloclub', 'slakenyc', 'spaceibizanewyork', 'birdlandjazzclub', 'foresthillsstadium', 'littlefieldnewyorkcity', 'kingstheatrebklyn', 'metopera', 'prucenter', 'yankeestadium0', 'yankees', 'mets', 'metlifestadium', 'stage48', 'houseofyes', 'nyknicks', 'newyorkislanders', 'newyorkgiants', 'nyliberty', 'newyorkriveters', '4040club', 'yiddishnewyork', 'nikonjbt']

FACEBOOK_ACCESS_TOKEN_URL = "https://graph.facebook.com/v2.6/oauth/access_token"
FACEBOOK_GRAPH_API = "https://graph.facebook.com/v2.8/"
ADDITIONAL_PARAMS = "&debug=all&format=json&method=get&pretty=0&suppress_http_code=1"
FETCHABLE_EVENT_FIELDS = "&fields=attending_count,can_guests_invite,category,declined_count,end_time,guest_list_enabled,interested_count,is_canceled,is_page_owned,is_viewer_admin,maybe_count,name,noreply_count,place,start_time,timezone,type,updated_time"

APP_CLIENT_ID = "587748278082312"
APP_CLIENT_SECRET = "653f038ce5648e38a231609066407d86"
GRANT_TYPE = "client_credentials"

HOST = "localhost"
DATABASE = "revmax_development"
USER = "nandukalidindi"
PASSWORD = "qwerty123"

CRAWL_START_DATE = '2017-01-01T00:00:00-4000'
CRAWL_END_DATE = '2018-01-01T00:00:00-4000'

def get_handles_from_csv(filename):
    with open(filename) as file_csv:
        csv_reader = csv.reader(file_csv, delimiter=',', skipinitialspace=True)
        handle_list =  []
        for row in csv_reader:
            if row[1] != None and row[1] != '' and row[1] != 'Handle':
                handle_list.append(row[1])

    return handle_list


def schema():
    return {
                'name'              : 'varchar',
                'start_time'        : 'datetime',
                'handle'            : 'varchar',
                'fid'               : 'varchar',
                'attending_count'   : 'integer',
                'can_guests_invite' : 'boolean',
                'category'          : 'string',
                'declined_count'    : 'integer',
                'guest_list_enabled': 'boolean',
                'interested_count'  : 'integer',
                'is_canceled'       : 'boolean',
                'is_page_owned'     : 'boolean',
                'is_viewer_admin'   : 'boolean',
                'maybe_count'       : 'integer',
                'noreply_count'     : 'integer',
                'timezone'          : 'varchar',
                'end_time'          : 'datetime',
                'updated_time'      : 'datetime',
                'type'              : 'varchar',
                'venue_fid'         : 'varchar',
                'venue_name'        : 'varchar',
                'venue_city'        : 'varchar',
                'venue_state'       : 'varchar',
                'venue_country'     : 'varchar',
                'venue_latitude'    : 'varchar',
                'venue_longitude'   : 'varchar',
                'geometry'          : 'geometry',
                'venue_capacity'    : 'integer'
            }

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

def crawl_from_today():
    current_timestamp = time.mktime(datetime.now().timetuple())
    super_future_timestamp = epoch('3000-01-01T00:00:00-4000')

def crawl_from_to(handle):
    start_date = CRAWL_START_DATE
    end_date = CRAWL_END_DATE

    epoch_start_date = epoch(start_date)
    epoch_end_date = epoch(end_date)

    crawl_events_in_time_frame(handle, epoch_start_date, epoch_end_date)

def crawl_events_in_time_frame(handle, start_date, end_date):
    count = 0
    url = "%s%s/events?access_token=%s&since=%s&limit=1000%s" % (FACEBOOK_GRAPH_API, handle, access_token, str(start_date), ADDITIONAL_PARAMS)

    while url != None:
        full_response = get_api_response(url)
        paginated_response = full_response.get('data')
        max_end_date = 0
        if paginated_response != None:
            for event in paginated_response:
                epoch_start_time = epoch(event['start_time'])
                if epoch_start_time < end_date:
                    enriched_event = enrich_event(event["id"], handle)
                    persist_event(enriched_event, stringify_schema(enriched_event))
                    count += 1
                    if epoch_start_time > max_end_date:
                        max_end_date = epoch_start_time

            if len(paginated_response) == 0:
                url = None
            elif full_response.get('paging').get('next') != None:
                url = full_response.get('paging').get('next')
            elif max_end_date > end_date or max_end_date == 0:
                url = None
            else:
                url = "%s%s/events?access_token=%s&since=%s&limit=1000%s" % (FACEBOOK_GRAPH_API, handle, access_token, str(max_end_date), ADDITIONAL_PARAMS)
        else:
            url = None

    print("%s events for handle: %s crawled at %s" % (count, handle, str(datetime.now().replace(microsecond=0).isoformat())))

def epoch(string_date):
    yyyyMMddTHHMMSS_date = string_date.rpartition("-")[0] if (len(string_date.rpartition("+")[0]) == 0) else string_date.rpartition("+")[0]
    pattern = "%Y-%m-%dT%H:%M:%S"
    epoch = int(time.mktime(time.strptime(yyyyMMddTHHMMSS_date, pattern)))
    return epoch

def addHourOffset(string_date, offset):
    yyyyMMddTHHMMSS_date = string_date.rpartition("-")[0] if (len(string_date.rpartition("+")[0]) == 0) else string_date.rpartition("+")[0]
    pattern = "%Y-%m-%dT%H:%M:%S"
    offset_time = datetime.strptime(yyyyMMddTHHMMSS_date, pattern)
    offset_time += timedelta(hours=offset)
    return offset_time.strftime(pattern)

def crawl_page_events(handle):
    url = "%s%s/events?access_token=%s%s" % (FACEBOOK_GRAPH_API, handle, access_token, ADDITIONAL_PARAMS)
    while url != None:
        full_response = get_api_response(url)
        paginated_response = get_api_response(url)['data']
        for event in paginated_response:
            enriched_event = enrich_event(event["id"], handle)
            persist_event(enriched_event, stringify_schema(enriched_event))
        url = full_response.get('paging').get('next')

def enrich_event(event_id, handle):
    event_url = "%s%s?access_token=%s%s%s" % (FACEBOOK_GRAPH_API, event_id, access_token, FETCHABLE_EVENT_FIELDS, ADDITIONAL_PARAMS)
    response = get_api_response(event_url)

    response['fid'] = response['id']
    response['handle'] = handle
    response['geometry'] = None
    response['venue_capacity'] = None
    response['created_at'] = str(datetime.now().replace(microsecond=0).isoformat())
    response['updated_at'] = str(datetime.now().replace(microsecond=0).isoformat())

    if response.get('place') != None:
        place = response.get('place')
        response['venue_fid'] = place.get('id')
        response['venue_name'] = place.get('name')
        if place.get('location') != None:
            location = place['location']
            response['venue_city'] = location.get('city')
            response['venue_state'] = location.get('state')
            response['venue_country'] = location.get('country')
            response['venue_latitude'] = location.get('latitude')
            response['venue_longitude'] = location.get('longitude')
            response['venue_capacity'] = 10
        del response['place']

    del response['id']
    del response['__debug__']

    return response

def persist_event(event, stringified_event):
    stringified_event_0 = stringified_event[0][:-1]

    formatter_list = []
    for i in range(0, len(stringified_event[1])):
        formatter_list.append('%s')

    formatter_list_string = ','.join(formatter_list)

    if event.get('venue_latitude') != None and event.get('venue_longitude') != None:
        stringified_event_0 = stringified_event_0 + ",geometry"
        formatter_list_string = formatter_list_string + ",ST_GeomFromText(%s,4326)"
        coordinates = "POINT(%s %s)" % (event.get('venue_longitude'), event.get('venue_latitude'))
        stringified_event[1].append(coordinates)

    select_check_sql = "SELECT updated_at FROM events where fid=%s"
    select_check_values = [event.get('fid')]

    cursor.execute(select_check_sql, select_check_values)
    updated_at_row = cursor.fetchone()

    if updated_at_row != None:
        updated_at = updated_at_row[0]
        updated_at_epoch = int(time.mktime(time.strptime(str(updated_at), "%Y-%m-%d")))
        updated_time_epoch = epoch(event['updated_time'])

        if updated_time_epoch > updated_at_epoch:
            update_sql = "UPDATE events SET (" + stringified_event_0 + ") = (" + formatter_list_string + ") WHERE fid='" + event['fid'] + "'"
            values = stringified_event[1]
            cursor.execute(update_sql, values)
            pg_connection.commit()

    if event.get('venue_fid') == None:
        sql = "SELECT fid FROM events WHERE name=%s AND venue_fid IS NULL AND start_time=%s ORDER BY updated_at DESC"
        values = [event.get('name'), event.get('start_time')]
    else:
        sql = "SELECT fid FROM events WHERE name=%s AND venue_fid=%s AND start_time=%s ORDER BY updated_at DESC"
        values = [event.get('name'), event.get('venue_fid'), event.get('start_time')]

    cursor.execute(sql, values)
    rows = cursor.fetchall()
    count = len(rows)

    if count == 0:
        sql = "INSERT INTO events (" + stringified_event_0 + ") VALUES (" + formatter_list_string + ")"
        values = stringified_event[1]
        cursor.execute(sql, values)

    elif count > 1:
        for x in range(1, count):
            delete_sql = "DELETE FROM events where fid=%s"
            deletable_fid = rows[x][0]
            cursor.execute(delete_sql, deletable_fid)
            
    if event.get('venue_latitude') != None and event.get('venue_longitude') != None:
        for i in range(0, event.get('venue_capacity')):
            estimated_initiated_at = ""
            if event.get('end_time') != None:
                estimated_initiated_at = event.get('end_time')
            else:
                estimated_initiated_at = addHourOffset(event.get('start_time'), 2)

            select_random_sql = "SELECT dropoff_longitude, dropoff_latitude FROM yellow_cabs ORDER BY random() limit 1";
            cursor.execute(select_random_sql)
            randomly_picked_geometry = cursor.fetchone()

            destination_coordinates = "POINT(%s %s)" % (str(randomly_picked_geometry[0]), str(randomly_picked_geometry[1]))

            request_sql = "INSERT INTO revmax_requests (initiated_at, location, destination, source, location_text, destination_text) VALUES (%s, ST_GeomFromText(%s,4326), ST_GeomFromText(%s, 4326), %s, %s, %s)"
            cursor.execute(request_sql, [estimated_initiated_at, coordinates, destination_coordinates, "events", coordinates, destination_coordinates])

    pg_connection.commit()

def delete_all_ride_requests():
    delete_sql = "DELETE FROM revmax_requests"
    cursor.execute(delete_sql)
    pg_connection.commit()


def stringify_schema(dictionary):
    stringified_value = []
    column_string = ""
    for k, v in dictionary.items():
        if v != None:
            column_string = column_string + k + ","
            stringified_value.append(v)

    return column_string, stringified_value

def postgres_connection():
    try:
        connection = psycopg2.connect("dbname={} user={} host={} password={}".format(DATABASE, USER, HOST, PASSWORD))
    except:
        print("Unable to connect database. Please try again!")

    return connection;


access_token = get_access_token()
pg_connection = postgres_connection()
cursor = pg_connection.cursor()

print("=======================================================================")
print("=======================================================================")
handle_list = get_handles_from_csv("facebook_pages.csv")

delete_all_ride_requests()

for handle in handle_list:
    print("CRAWLING FOR HANDLE: " + handle)
    crawl_from_to(handle)

print("ALL PAGE EVENTS CRAWLED AT %s" % (str(datetime.now().replace(microsecond=0).isoformat())))
print("=======================================================================")
print("=======================================================================")

pg_connection.close()
