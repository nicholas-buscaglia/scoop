import os
import json
import math
import pymysql
import pandas as pd
import requests
import urllib.parse
import warnings

warnings.filterwarnings("ignore")


def search(search_1, search_2, search_3, lat, lon, radius, market):
    
    mydb = pymysql.connect(
        host=os.environ['HOST'],
        user=os.environ['USER'],
        passwd=os.environ['PASSWD'])
    cursor = mydb.cursor()

    # technique 3 full text full boolean
    df = pd.read_sql("SELECT DISTINCT uid, name, address, phone_number, category, rating, lat, lon, "
                     + "google_image_links, "
                     + "length(reviews) - length(replace(reviews, '" + search_1 + "', '')) AS diff_a, "
                     + "length(reviews) - length(replace(reviews, '" + search_2 + "', '')) AS diff_b, "
                     + "length(reviews) - length(replace(reviews, '" + search_3 + "', '')) AS diff_c, "
                     + "(ST_Distance_Sphere(point(lon, lat), point(" + lon + ", " + lat + ")) *.000621371192) AS dist "
                     + "FROM results." + market + " "
                     + "WHERE (ST_Distance_Sphere(point(lon, lat), point(" + lon + ", " + lat + ")) *.000621371192) <= "
                     + radius + " "
                     + "AND MATCH(reviews) AGAINST ('+" + '"' + search_1 + '" ' + '+"' + search_2 + '" '
                     + '+"' + search_3 + '"' + "' IN BOOLEAN MODE)", con=mydb)

    cursor.close()
    mydb.close()

    return df


def lambda_handler(event, context):
    
    while True:

        try:
            search_term_a = event['queryStringParameters']['search_term_a']
        except:
            try:
                search_term_a = event['search_term_a']
            except:
                search_term_a = ''
                
        try:
            search_term_b = event['queryStringParameters']['search_term_b']
        except:
            try:
                search_term_b = event['search_term_b']
            except:
                search_term_b = ''
                
        try:
            search_term_c = event['queryStringParameters']['search_term_c']
        except:
            try:
                search_term_c = event['search_term_c']
            except:
                search_term_c = ''

        search_lat = None
        search_lon = None
        search_loc = None
        search_radius = None

        try:
            search_lat = event['queryStringParameters']['latitude']
            search_lon = event['queryStringParameters']['longitude']
        except:
            try:
                search_lat = event['latitude']
                search_lon = event['longitude']
            except:
                pass
            
        try:
            search_loc = event['queryStringParameters']['search_location']
        except:
            pass
        
        try:
            search_radius = event['queryStringParameters']['radius']
            search_radius = str(search_radius)
        except:
            pass

        if search_loc is not None:

            try:
                url = 'https://nominatim.openstreetmap.org/search/' + urllib.parse.quote(search_loc) + '?format=json'
                response = requests.get(url).json()
                search_lat = response[0]["lat"]
                search_lon = response[0]["lon"]

            except:
                print('error getting location coords from string')
                print(search_loc)

                # message variable populate for no location result
                data = {
                    "message": 'Location error occured, please try again',
                    "total": 0,
                    "top_results": []
                }

                break

        # assign market table

        route_table = [{'market': 'Buffalo', 'lat': 42.8867166, 'lon': -78.8783922},
                       {'market': 'Manhattan', 'lat': 40.7896239, 'lon': -73.9598939},
                    #   {'market': 'Brooklyn', 'lat': 40.6526006, 'lon': -73.9497211},
                       {'market': 'Tampa', 'lat': 27.6886419, 'lon': -82.5723193},
                       {'market': 'Miami', 'lat': 25.7741728, 'lon': -80.19362},
                       {'market': 'Austin', 'lat': 30.2711286, 'lon': -97.7436995},
                       {'market': 'Denver', 'lat': 39.7392364, 'lon': -104.984862},
                       {'market': 'San_Diego', 'lat': 32.7174202, 'lon': -117.1627728}]

        obj_lat = [float(search_lat)]
        obj_lon = [float(search_lon)]

        search_market = ''
        minz = 10000000

        for obj in route_table:
            market_rt = obj['market']
            lat_rt = [obj['lat']]
            lon_rt = [obj['lon']]
            maths = (math.dist(obj_lat, lat_rt)) + (math.dist(obj_lon, lon_rt))
            if maths <= minz:
                search_market = market_rt
                minz = maths

        # print(f'search market: {search_market}')

        # assign default search radius 10 miles until pete adds req to front end
        if search_radius == '' or search_radius == '0' or search_radius is None:
            search_radius = '10'

        # invoke search function which employs boolean to return one dataframe
        df = search(search_term_a, search_term_b, search_term_c, search_lat, search_lon, search_radius, search_market)

        total_results = df.shape[0]
        x_shape = total_results

        df['count_a'] = df['count_b'] = df['count_c'] = 0

        term_length_a = len(search_term_a)
        term_length_b = len(search_term_b)
        term_length_c = len(search_term_c)

        for record in range(total_results):

            if search_term_a != '':
                count = round(df['diff_a'][record] / term_length_a)
                if count < 1:
                    count = 1
                df['count_a'][record] = count

            if search_term_b != '':
                count = round(df['diff_b'][record] / term_length_b)
                if count < 1:
                    count = 1
                df['count_b'][record] = count

            if search_term_c != '':
                count = round(df['diff_c'][record] / term_length_c)
                if count < 1:
                    count = 1
                df['count_c'][record] = count

        total_a = df['count_a'].sum()
        total_b = df['count_b'].sum()
        total_c = df['count_c'].sum()

        df = df.head(50)

        df['total_mentions'] = df['count_a'] + df['count_b'] + df['count_c']

        df['a_score'] = df['b_score'] = df['c_score'] = 0

        # sum count mentions create average score and sort by score
        if total_a > 0:
            df['a_score'] = round((df['count_a'] / total_a) * 100)
        if total_b > 0:
            df['b_score'] = round((df['count_b'] / total_b) * 100)
        if total_c > 0:
            df['c_score'] = round((df['count_c'] / total_c) * 100)

        if total_a > 0 and total_b > 0 and total_c > 0:
            df['score'] = df[['a_score', 'b_score', 'c_score']].mean(axis=1)
        elif total_a > 0 and total_b > 0:
            df['score'] = df[['a_score', 'b_score']].mean(axis=1)
        elif total_a > 0 and total_c > 0:
            df['score'] = df[['a_score', 'c_score']].mean(axis=1)
        elif total_b > 0 and total_c > 0:
            df['score'] = df[['b_score', 'c_score']].mean(axis=1)
        elif total_a > 0:
            df['score'] = df['a_score']
        elif total_b > 0:
            df['score'] = df['b_score']
        elif total_c > 0:
            df['score'] = df['c_score']
        else:
            df['score'] = 0

        df = df.sort_values(by=['score', 'total_mentions', 'dist'], ascending=[False, False, False])
        df.reset_index(inplace=True)

        # target variables to pass to json: results, name, score, dist

        # create return text
        if x_shape > 0:
            x_message = f'Huzzah! {x_shape} restaurants match your weird search'
        else:
            x_message = f'Sorry no matches, try something else'

        # cap results at 20
        if x_shape > 20:
            output_range = 20
        else:
            output_range = x_shape

        image_links = []
        for result in range(output_range):
            try:
                link = (df['google_image_links'][result]).split(",")
            except:
                try:
                    link = df['google_image_links'][result]
                except:
                    link = []
            image_links.append(link)

        top_results = []

        for result in range(output_range):
            top_results.append({
                "ref_id": df['uid'][result],
                "name": df['name'][result],
                "address": df['address'][result],
                "phone_number": df['phone_number'][result],
                "category": df['category'][result],
                "rating": round(float(df['rating'][result]),1),
                "mentions": round(df['total_mentions'][result]),
                "score": round(df['score'][result]),
                "latitude": df['lat'][result],
                "longitude": df['lon'][result],
                "distance": round(df['dist'][result], 1),
                "google_image_links": image_links[result]
            })

        search_location = {
            "latitude": float(search_lat),
            "longitude": float(search_lon),
            "market": search_market
        }

        data = {
            "message": x_message,
            "search_location": search_location,
            "total": x_shape,
            "top_results": top_results
        }

        break

    return {
        'statusCode': 200,
        'body': json.dumps(data)
    }

