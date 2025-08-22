import os
from dotenv import load_dotenv
from flask import Flask, jsonify, request, Response
import requests
from bs4 import BeautifulSoup
import re

load_dotenv()

debug = os.getenv('DEBUG')
url = os.getenv('URL')
secure = os.getenv('SECURE')

app = Flask(__name__)

def getPageData():
    try:

        response = requests.get(
            url,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"},
            timeout=2900
        )
        print(response)
        soup = BeautifulSoup(response.text, "html.parser")
        rows = soup.select("table tbody tr")
        HEADERS = ["nro", "date", "address", "type", "state", "machine", "map"]
        data = []

        if len(rows) > 0:
            n=0
            for row in rows:
                cells = row.find_all("td")
                line = {"fila" : n}
                y=0    
                for cell in cells:               
                    if HEADERS[y] == 'address':
                        coordenadas = re.findall(r"\(([^)]+)\)", cell.text)
                        if coordenadas and len(coordenadas[0].split(',')) == 2:
                            lat_lng = [float(x) for x in coordenadas[0].split(',')]
                        else:
                            lat_lng = [0.0, 0.0]

                        line[HEADERS[y]] = {
                            "coords": {
                                "lat" : lat_lng[0],
                                "lng" : lat_lng[1]
                            },
                            "full_address": str(cell.text).strip()
                        }
                    else:
                        line[HEADERS[y]] = str(cell.text).strip()
                    y+=1
                data.append(line)
                n+=1

        return data
    
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
        resultado = getPageData()
        return jsonify(resultado)
    else:
        return Response('Acceso prohibido', status=403)

if __name__ == '__main__':
    app.run(debug=debug)