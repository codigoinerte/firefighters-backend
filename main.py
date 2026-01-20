import os
from dotenv import load_dotenv
from flask import Flask, jsonify, request, Response
from flask_cors import CORS
import requests
from bs4 import BeautifulSoup
import redis
import json
import re
import pymysql

load_dotenv()

redis_host = os.getenv('REDIS_HOST')
redis_port = os.getenv('REDIS_PORT')
redis_decode = os.getenv('REDIS_DECODE')
redis_username = os.getenv('REDIS_USERNAME')
redis_password = os.getenv('REDIS_PASSWORD')
redis_disabled = os.getenv('REDIS_DISABLED')

# Database configuration
db_host = os.getenv('DB_HOST', 'localhost')
db_port = int(os.getenv('DB_PORT', 3306))
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_USER_PASSWORD')
db_name = os.getenv('DB_DATABASE')

app_origins = os.getenv('APP_ORIGINS')
app_origins = app_origins.split(",")

remote_url = os.getenv('REMOTE_URL')
remote_secure = os.getenv('REMOTE_SECURE')

debug = os.getenv('DEBUG')
url = os.getenv('URL')
secure = os.getenv('SECURE')

r = redis.Redis(host=redis_host,
    port=redis_port,
    decode_responses=redis_decode,
    username=redis_username,
    password=redis_password)

app = Flask(__name__)
CORS(app)
CORS(app, origins=app_origins)

def string_to_bool(s):
    """
    Converts a string representation of truth to True or False.

    True values are 'y', 'yes', 't', 'true', 'on', and '1' (case-insensitive).
    False values are 'n', 'no', 'f', 'false', 'off', and '0' (case-insensitive).
    Raises ValueError if 's' is anything else.
    """
    s = s.strip().lower()
    if s in ('yes', 'true', 't', 'on', '1'):
        return True
    elif s in ('no', 'false', 'f', 'off', '0'):
        return False
    else:
        raise ValueError(f"Invalid truth value {s!r}")

def get_boundary(district):
    """Listado de limites de distrito
    @district: string (nombre de distrito)"""
    try:
        connection = pymysql.connect(
            host=db_host,
            port=db_port,
            user=db_user,
            password=db_password,
            database=db_name,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )

        with connection:
            with connection.cursor() as cursor:
                # Obtener todas las tablas
                cursor.execute("""SELECT coordinates, department, district, id, province, ubigeo
                                FROM `districts` 
                                WHERE province = 'Lima' AND district = %s""", [district])
                tables = cursor.fetchall()
                return tables

    except pymysql.Error as db_error:
        print(f"Error de base de datos: {db_error}")
        return []
    except Exception as e:
        print(f"Error general: {e}")
        return []

def get_page_data():
    """obtener data desde tabla scrapping"""
    try:
        response = requests.get(
            url,
            headers={"User-Agent": """Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"""},
            timeout=500
        )
        cities = []
        states = []
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
                        state = str(cell.text).strip()
                        if headers[y] == 'state':
                            states.append(str(state).lower())
                        line[headers[y]] = state
                    y+=1
                if line.get("address").get("coords").get("lat") is not None and line.get("address").get("coords").get("lng") is not None:
                    data.append(line)
                    n+=1
        cities = list(set(cities))
        return { "incidents": data, "cities": cities, "states": list(set(states)) }

    except Exception as e:
        print(e)
        return []


@app.route('/', methods=['GET'])
def home():
    """endpoint consulta ruta base home"""
    if request.method == "GET":
        return 'Home'
@app.route('/boundary', methods=['POST'])
def boundary():
    """endpoint de llamado de limites de distrito"""
    request_header = request.headers.get('secure')
    if request.method == "POST" and request_header == secure:
        req_json = request.get_json(force=True, silent=True)
        district = req_json.get('district') if req_json else None
        boundary_data = get_boundary(district=district)
        return jsonify(boundary_data)
    else:
        return Response('Acceso prohibido', status=403)
@app.route('/data', methods=['GET', 'POST'])
def get_data():
    """endpoint de llamado de data de emergencias"""
    request_header = request.headers.get('secure')

    if request.method == "GET":
        return 'Hello world'
    elif request.method == "POST" and request_header == secure:

        ff_list = r.get('firefighter-list')
        data = {}

        if ff_list is None or string_to_bool(redis_disabled) is True:
            resultado = get_page_data()

            r.set('firefighter-list', json.dumps(resultado))
            r.expire('firefighter-list', 600)

            data = resultado
        else:
            data = json.loads(ff_list)

        get_request = request.get_json(force=True, silent=True)

        if get_request is not None:
            get_state = get_request.get('state')
            get_district = get_request.get('city')

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

@app.route('/seed', methods=['GET'])
def seed_database():
    """
    Limpia la base de datos y ejecuta el archivo districts.sql
    """
    try:
        # Conectar a la base de datos
        connection = pymysql.connect(
            host=db_host,
            port=db_port,
            user=db_user,
            password=db_password,
            database=db_name,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )

        with connection:
            with connection.cursor() as cursor:
                # Obtener todas las tablas
                cursor.execute("SHOW TABLES")
                tables = cursor.fetchall()

                # Deshabilitar foreign key checks para eliminar tablas
                cursor.execute("SET FOREIGN_KEY_CHECKS = 0")

                # Eliminar todas las tablas
                for table in tables:
                    table_name = list(table.values())[0]
                    cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`")
                    print(f"Tabla eliminada: {table_name}")

                # Rehabilitar foreign key checks
                cursor.execute("SET FOREIGN_KEY_CHECKS = 1")

                connection.commit()
                print("Base de datos limpiada")

                # Leer y ejecutar el archivo SQL
                sql_file_path = os.path.join(os.path.dirname(__file__), 'districts.sql')

                if not os.path.exists(sql_file_path):
                    return jsonify({
                        "success": False,
                        "message": "Archivo districts.sql no encontrado"
                    }), 404

                print(f"Ejecutando archivo SQL: {sql_file_path}")

                # Leer el archivo en bloques para archivos grandes
                with open(sql_file_path, 'r', encoding='utf-8') as file:
                    sql_statements = []
                    current_statement = []

                    for line in file:
                        line = line.strip()
                        # Ignorar comentarios y líneas vacías
                        if not line or line.startswith('--') or line.startswith('#'):
                            continue

                        current_statement.append(line)

                        # Si la línea termina en ;, es el final de una declaración
                        if line.endswith(';'):
                            statement = ' '.join(current_statement)
                            sql_statements.append(statement)
                            current_statement = []

                    # Ejecutar declaraciones en lotes para mejor rendimiento
                    batch_size = 100
                    total_statements = len(sql_statements)

                    for i in range(0, total_statements, batch_size):
                        batch = sql_statements[i:i + batch_size]
                        for statement in batch:
                            try:
                                cursor.execute(statement)
                            except Exception as stmt_error:
                                print(f"Error en declaración: {str(stmt_error)[:100]}")
                                # Continuar con las siguientes declaraciones
                                continue

                        connection.commit()
                        print(f"Progreso: {min(i + batch_size, total_statements)}/{total_statements} declaraciones ejecutadas")

                print("Archivo SQL ejecutado exitosamente")

                return jsonify({
                    "success": True,
                    "message": "Base de datos limpiada y sembrada exitosamente",
                    "statements_executed": total_statements
                }), 200

    except pymysql.Error as db_error:
        print(f"Error de base de datos: {db_error}")
        return jsonify({
            "success": False,
            "message": f"Error de base de datos: {str(db_error)}"
        }), 500
    except Exception as e:
        print(f"Error general: {e}")
        return jsonify({
            "success": False,
            "message": f"Error: {str(e)}"
        }), 500

if __name__ == '__main__':
    app.run(debug=debug)
