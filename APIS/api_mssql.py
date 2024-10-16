from flask import Flask, jsonify, make_response, request, send_file
import json
import sys
import APIS.mssql_functions as MSSql

# Connect to mssql dB from start
mssql_params = {}
mssql_params['DB_HOST'] = '100.80.80.7'
mssql_params['DB_NAME'] = 'reto'
mssql_params['DB_USER'] = 'SA'
mssql_params['DB_PASSWORD'] = 'Shakira123.'

try:
    MSSql.cnx = MSSql.mssql_connect(mssql_params)
except Exception as e:
    print("Cannot connect to mssql server!: {}".format(e))
    sys.exit()

app = Flask(__name__)

@app.route("/hello")
def hello():
    return "Shakira rocks!\n"

@app.route("/eventos", defaults={'ID_EVENTO': None}, methods=['GET'])
@app.route("/eventos/<int:ID_EVENTO>", methods=['GET'])
def eventos(ID_EVENTO):
    if ID_EVENTO is not None:
        d_evento = MSSql.sql_read_where('EVENTOS', {'ID_EVENTO': ID_EVENTO})
        return make_response(jsonify(d_evento))
    
    d_eventos = MSSql.sql_read_all('EVENTOS')
    return make_response(jsonify(d_eventos))


@app.route("/login", methods=['POST'])
def login():
    data = request.json
    email = data.get('email')
    contrasena = data.get('contrasena')

    if not email or not contrasena:
        return make_response(jsonify({"error": "email y contrasena requerida"}), 400)

    result = MSSql.verify_login(email, contrasena)
    if result['success']:
        return make_response(jsonify({"message": "Login successful", "user": result['user']}), 200)
    else:
        return make_response(jsonify({"error": result['error']}), 401)


@app.route("/beneficios", defaults={'ID_BENEFICIO': None}, methods=['GET'])
@app.route("/beneficios/<int:ID_BENEFICIO>", methods=['GET'])
def beneficios(ID_BENEFICIO):
    if ID_BENEFICIO is not None:
        d_beneficio = MSSql.sql_read_where('BENEFICIOS', {'ID_BENEFICIO': ID_BENEFICIO})
        return make_response(jsonify(d_beneficio))
    
    d_beneficios = MSSql.sql_read_all('BENEFICIOS')
    return make_response(jsonify(d_beneficios))


@app.route('/eventos_usuario/<int:id_usuario>', methods=['GET'])
def get_eventos_usuario(id_usuario):
    try:
        query = """
        SELECT E.ID_EVENTO, E.NOMBRE, E.DESCRIPCION, E.FECHA, E.PUNTAJE, E.IMAGEN
        FROM USUARIOS_EVENTOS UE
        JOIN EVENTOS E ON UE.EVENTO = E.ID_EVENTO
        WHERE UE.USUARIO = %s AND E.FECHA < GETDATE()
        """
        cursor = MSSql.cnx.cursor(as_dict=True)
        cursor.execute(query, (id_usuario,))
        eventos = cursor.fetchall()
        cursor.close()

        return jsonify(eventos)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route("/eventos/update_asistentes/<int:id_evento>", methods=['POST'])
def update_asistentes(id_evento):
    try:
        success = MSSql.update_num_max_asistentes(id_evento)
        if success:
            return jsonify({"message": "Number of attendees updated successfully"}), 200
        else:
            return jsonify({"error": "Failed to update number of attendees"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/eventos/asistentes/<int:id_evento>", methods=['GET'])
def get_asistentes(id_evento):
    try:
        num_asistentes = MSSql.get_num_max_asistentes(id_evento)
        if num_asistentes is not None:
            return jsonify({"num_max_asistentes": num_asistentes}), 200
        else:
            return jsonify({"error": "Event not found"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500

API_CERT = '/home/user01/mnt/reto/SSL/titans.tc2007b.tec.mx.cer'
API_KEY = '/home/user01/mnt/reto/SSL/titans.tc2007b.tec.mx.key'

if __name__ == '__main__':
    import ssl
    print("Running API...")
    #app.run(host='0.0.0.0', port=10206, debug=True)
    context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
    context.load_cert_chain(API_CERT, API_KEY)
    app.run(host='0.0.0.0', port=10206, ssl_context=context, debug=True)