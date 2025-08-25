import os
from dotenv import load_dotenv
from flask import Flask, jsonify, request, Response
import requests
from bs4 import BeautifulSoup
import redis
import json
import re

load_dotenv()

redis_host = os.getenv('REDIS_HOST')
redis_port = os.getenv('REDIS_PORT')
redis_decode = os.getenv('REDIS_DECODE')
redis_username = os.getenv('REDIS_USERNAME')
redis_password = os.getenv('REDIS_PASSWORD')

debug = os.getenv('DEBUG')
url = os.getenv('URL')
secure = os.getenv('SECURE')

r = redis.Redis(host=redis_host,
    port=redis_port,
    decode_responses=redis_decode,
    username=redis_username,
    password=redis_password)

app = Flask(__name__)

def getPageData():
    try:
        response = requests.get(
            url,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"},
            timeout=3200
        )
        cities = []
        soup = BeautifulSoup(response.text, "html.parser")
        rows = soup.select("table tbody tr")
        headers = ["nro", "date", "address", "type", "state", "machine", "map"]
        data = []
        
        if len(rows) > 0:
            n=0
            for row in rows:
                cells = row.find_all("td")
                line = {"fila" : n}
                y=0
                for cell in cells:
                    if headers[y] == 'address':

                        parens = re.findall(r"\(([^)]+)\)", cell.text)
                        city_split = str(cell.text).lower().split("-")
                        city = str(city_split[-1]).strip()
                        lat_lng = [None, None]
                        for p in parens:
                            partes = p.split(',')
                            if len(partes) == 2:
                                try:
                                    lat = float(partes[0])
                                    lng = float(partes[1])
                                    lat_lng = [lat, lng]
                                    break  # Solo toma la primera válida
                                except ValueError:
                                    continue
                        if lat_lng[0] != 0.0 and lat_lng[1] != 0.0:
                            cities.append(city)
                        address = {
                            "coords": {
                                "lat" : None if lat_lng[0] == 0.0 else lat_lng[0],
                                "lng" : None if lat_lng[1] == 0.0 else lat_lng[1]                                
                            },
                            "city" : city,
                            "full_address": str(cell.text).strip(),
                        }
                        line[headers[y]] = address
                    else:
                        line[headers[y]] = str(cell.text).strip()
                    y+=1
                if line.get("address").get("coords").get("lat") is not None and line.get("address").get("coords").get("lng") is not None:
                    data.append(line)
                    n+=1
        return { "incidents": data, "cities": list(set(cities))}

    except Exception as e:
        print(e)
        return []


@app.route('/', methods=['GET'])
def home():
    if request.method == "GET":
        return 'Home'
    
@app.route('/data', methods=['GET', 'POST'])
def get_data():

    request_header = request.headers.get('secure')

    if request.method == "GET":
        return 'Hello world'
    elif request.method == "POST" and request_header == secure:

        data = request.json

        get_state = data.get('state')
        get_district = data.get('city')

        ff_list = r.get('firefighter-list')
        data = {}

        if ff_list is None:
            resultado = getPageData()

            r.set('firefighter-list', json.dumps(resultado))
            r.expire('firefighter-list', 600)

            data = resultado
        else:
            data = json.loads(ff_list)
        if get_district is not None:
            incident_list = data.get('incidents')
            incident_filter = []
            for item in incident_list:
                district_name = item.get('address').get('city')
                if district_name is not None and str(district_name).lower() == str(get_district).lower():
                    incident_filter.append(item)
            data['incidents'] = incident_filter

        if get_state is not None:
            incident_list = data.get('incidents')
            incident_filter = []
            for item in incident_list:
                state_name = item.get('state')
                if state_name is not None and str(state_name).lower() == str(get_state).lower():
                    incident_filter.append(item)
            data['incidents'] = incident_filter

        return jsonify(data)
    else:
        return Response('Acceso prohibido', status=403)

if __name__ == '__main__':
    app.run(debug=debug)
