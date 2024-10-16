cnx = None
mssql_params = {}
from datetime import datetime




def mssql_connect(sql_creds):
    import pymssql
    cnx = pymssql.connect(
        server=sql_creds['DB_HOST'],
        user=sql_creds['DB_USER'],
        password=sql_creds['DB_PASSWORD'],
        database=sql_creds['DB_NAME'])
    return cnx

def convert_datetime_to_string(data):
    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, datetime):
                data[key] = value.isoformat()
    elif isinstance(data, list):
        for item in data:
            convert_datetime_to_string(item)
    return data

def read_usuario_data(table_name, id):
    import pymssql
    global cnx, mssql_params
    read = "SELECT * FROM {} WHERE ID_USUARIO = '{}'" .format(table_name,id)
    #print(read)
    try:
        try:
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(read)
        except pymssql._pymssql.InterfaceError:
            print("reconnecting...")
            cnx = mssql_connect(mssql_params)
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(read)
        a = cursor.fetchall()
        cursor.close()
        return a
    except Exception as e:
        raise TypeError("read_usuarios_data: %s" % e)
    
def read_evento_data(table_name, id):
    import pymssql
    global cnx, mssql_params
    read = "SELECT * FROM {} WHERE ID_EVENTO = '{}'" .format(table_name,id)
    try:
        try:
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(read)
        except pymssql._pymssql.InterfaceError:
            print("reconnecting...")
            cnx = mssql_connect(mssql_params)
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(read)
        a = cursor.fetchall()
        cursor.close()
        return a
    except Exception as e:
        raise TypeError("read_evento_data: %s" % e)

def sql_read_all(table_name):
    import pymssql
    global cnx, mssql_params
    read = 'SELECT * FROM %s' % table_name
    try:
        try:
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(read)
        except pymssql._pymssql.InterfaceError:
            print("reconnecting...")
            cnx = mssql_connect(mssql_params)
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(read)
        a = cursor.fetchall()
        cursor.close()
        return a
    except Exception as e:
        raise TypeError("sql_read_where:%s" % e)

def sql_read_where(table_name, d_where):
    import pymssql
    global cnx, mssql_params
    read = 'SELECT * FROM %s WHERE ' % table_name
    read += '('
    for k,v in d_where.items():
        if v is not None:
            if isinstance(v,bool):
                read += "%s = '%s' AND " % (k,int(v == True))
            else:
                read += "%s = '%s' AND " % (k,v)
        else:
            read += '%s is NULL AND ' % (k)
    # Remove last "AND "
    read = read[:-4]
    read += ')'
    try:
        try:
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(read)    
        except pymssql._pymssql.InterfaceError:
            print("reconnecting...")
            cnx = mssql_connect(mssql_params)
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(read)
        a = cursor.fetchall()
        cursor.close()
        return a
    except Exception as e:
        raise TypeError("sql_read_where:%s" % e)

def sql_insert_row_into(table_name, d):
    import pymssql
    global cnx, mssql_params
    keys = ""
    values = ""
    data = []
    for k in d:
        keys += k + ','
        values += "%s,"
        if isinstance(d[k],bool):
            data.append(int(d[k] == True))
        else:
            data.append(d[k])
    keys = keys[:-1]
    values = values[:-1]
    insert = 'INSERT INTO %s (%s) VALUES (%s)'  % (table_name, keys, values)
    data = tuple(data)
    #print(insert)
    #print(data)
    try:
        try:
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(insert, data)
        except pymssql._pymssql.InterfaceError:
            print("reconnecting...")
            cnx = mssql_connect(mssql_params)
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(insert, data)
        cnx.commit()
        id_new = cursor.lastrowid
        cursor.close()
        return id_new
    except Exception as e:
        raise TypeError("sql_insert_row_into:%s" % e)

def sql_update_where(table_name, d_field, d_where):
    import pymssql
    global cnx, mssql_params
    update = 'UPDATE %s SET ' % table_name
    for k,v in d_field.items():
        if v is None:
            update +='%s = NULL, ' % (k)
        elif isinstance(v,bool):
            update +='%s = %s, ' % (k,int(v == True))
        elif isinstance(v,str):
            update +="%s = '%s', " % (k,v)
        else:
            update +='%s = %s, ' % (k,v)
    # Remove last ", "
    update = update[:-2]
    update += ' WHERE ( '
    for k,v in d_where.items():
        if v is not None:
            if isinstance(v,bool):
                update += "%s = '%s' AND " % (k,int(v == True))
            else:
                update += "%s = '%s' AND " % (k,v)
        else:
            update += '%s is NULL AND ' % (k)
    # Remove last "AND "
    update = update[:-4]
    update += ")"
    #print(update)
    try:
        try:
            cursor = cnx.cursor(as_dict=True)
            a = cursor.execute(update)
        except pymssql._pymssql.InterfaceError:
            print("reconnecting...")
            cnx = mssql_connect(mssql_params)
            cursor = cnx.cursor(as_dict=True)
            a = cursor.execute(update)
        cnx.commit()
        cursor.close()
        return a
    except Exception as e:
        raise TypeError("sql_update_where:%s" % e)

def sql_delete_where(table_name, d_where):
    import pymssql
    global cnx, mssql_params
    delete = 'DELETE FROM %s ' % table_name
    delete += ' WHERE ( '
    for k,v in d_where.items():
        if v is not None:
            if isinstance(v,bool):
                delete += "%s = '%s' AND " % (k,int(v == True))
            else:
                delete += "%s = '%s' AND " % (k,v)
        else:
            delete += '%s is NULL AND ' % (k)
    # Remove last "AND "
    delete = delete[:-4]
    delete += ")"
    #print(delete)
    try:
        try:
            cursor = cnx.cursor(as_dict=True)
            a = cursor.execute(delete)
        except pymssql._pymssql.InterfaceError:
            print("reconnecting...")
            cnx = mssql_connect(mssql_params)
            cursor = cnx.cursor(as_dict=True)
            a = cursor.execute(delete)
        cnx.commit()
        cursor.close()
        return a
    except Exception as e:
        raise TypeError("sql_delete_where:%s" % e)

def verify_login(email, password):
    import pymssql
    global cnx, mssql_params
    query = "SELECT * FROM USUARIOS WHERE EMAIL = %s AND CONTRASENA = %s"
    
    try:
        try:
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(query, (email, password))
        except pymssql._pymssql.InterfaceError:
            print("reconnecting...")
            cnx = mssql_connect(mssql_params)
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(query, (email, password))

        user = cursor.fetchone()
        cursor.close()

        # Si se encontró el usuario, el login fue exitoso
        if user:
            return {"success": True, "user": user}
        else:
            return {"success": False, "error": "Invalid username or password"}
    
    except Exception as e:
        raise TypeError("verify_login: %s" % e)

def get_eventos_usuario(id_usuario):
    import pymssql
    global cnx, mssql_params

    # Consulta para obtener los eventos del usuario
    query = """
        SELECT E.ID_EVENTO, E.NOMBRE, E.DESCRIPCION, E.FECHA, E.PUNTAJE, E.IMAGEN
        FROM USUARIOS_EVENTOS UE
        JOIN EVENTOS E ON UE.EVENTO = E.ID_EVENTO
        WHERE UE.USUARIO = %s
    """

    try:
        try:
            # Intenta ejecutar la consulta con la conexión actual
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(query, (id_usuario,))
        except pymssql._pymssql.InterfaceError:
            # Si la conexión ha fallado, reconéctate y vuelve a ejecutar la consulta
            print("Reconnecting...")
            cnx = mssql_connect(mssql_params)
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(query, (id_usuario,))

        eventos = cursor.fetchall()
        cursor.close()

        # Retornar los eventos encontrados
        return eventos
    except Exception as e:
        raise TypeError("get_eventos_usuario: %s" % e)

if __name__ == '__main__':
    import json
    mssql_params = {}
    mssql_params['DB_HOST'] = '100.80.80.7'
    mssql_params['DB_NAME'] = 'reto'
    mssql_params['DB_USER'] = 'SA'
    mssql_params['DB_PASSWORD'] = 'Shakira123.'
    cnx = mssql_connect(mssql_params)

    # Do your thing
    try:
        rx = verify_login('juan.perez@example.com','hashed_password_juan')
        print(json.dumps(rx, indent=4))
        input("press Enter to continue...")
    except Exception as e:
        print(e)
    cnx.close()

def update_num_max_asistentes(id_evento):
    import pymssql
    global cnx, mssql_params
    update = """
    UPDATE EVENTOS
    SET NUM_MAX_ASISTENTES = NUM_MAX_ASISTENTES - 1
    WHERE ID_EVENTO = '{}' AND NUM_MAX_ASISTENTES > 0
    """.format(id_evento)
    try:
        try:
            cursor = cnx.cursor()
            cursor.execute(update)
            cnx.commit()
        except pymssql._pymssql.InterfaceError:
            print("reconnecting...")
            cnx = mssql_connect(mssql_params)
            cursor = cnx.cursor()
            cursor.execute(update)
            cnx.commit()
        cursor.close()
        return True
    except Exception as e:
        raise TypeError("update_num_max_asistentes: %s" % e)
    
def get_num_max_asistentes(id_evento):
    import pymssql
    global cnx, mssql_params
    query = """
    SELECT NUM_MAX_ASISTENTES
    FROM EVENTOS
    WHERE ID_EVENTO = '{}'
    """.format(id_evento)
    try:
        try:
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(query)
            result = cursor.fetchone()
        except pymssql._pymssql.InterfaceError:
            print("reconnecting...")
            cnx = mssql_connect(mssql_params)
            cursor = cnx.cursor(as_dict=True)
            cursor.execute(query)
            result = cursor.fetchone()
        cursor.close()
        return result['NUM_MAX_ASISTENTES'] if result else None
    except Exception as e:
        raise TypeError("get_num_max_asistentes: %s" % e)