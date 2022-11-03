from flask import Flask
# from flask_socketio import SocketIO
import mysql.connector
from mysql.connector import Error
import json
import websockets
import asyncio
import time

app = Flask(__name__)
app.config['SECRET_KEY'] = 'bruh'

def create_connection(): # Подключение базы данных
    try:
        connection = mysql.connector.connect(
            host='localhost',
            user='root',
            passwd='25102005rus',
            database='computer_science_messenger',
            auth_plugin='mysql_native_password'
        )
        print("Connection to MySQL DB successful")
    except Error as e:
        print(f"The error '{e}' occurred")

    return connection

def execute_read_query(connection, query): # Чтение данных из БД
    try:
        cursor = connection.cursor()
        result = None
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Error as e:
        print(f"The error '{e}' occurred")

def execute_query(connection, query):
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        connection.commit()
        print("Query executed successfully")
    except Error as e:
        print(f"The error '{e}' occurred")


# socket = SocketIO(app, cors_allowed_origins="*")


# @socket.on('message')
# def handle_message(msg):
#     print(msg)
#     socket.send('ok')

# @socket.on("connect")
# def connected():
#     print("client has connected")

# @app.route('/get_messages', methods=['POST'])
# def get_messages():
#     db_connect = create_connection()

#     get_messages_sql = f''' SELECT * FROM `messages` '''
#     messages = execute_read_query(db_connect, get_messages_sql)

#     print(messages)
#     db_connect.close()

#     return json.dumps(messages)

connected = set()

async def server(websocket, path):

    connected.add(websocket)

    async for message in websocket:
        if json.loads(message)['type'] == 'init':
            db_connect = create_connection()

            get_messages_data_sql = f''' SELECT `name`, `message`, `hours`, `minutes` FROM `messages` ORDER BY `time` '''
            messages_data = execute_read_query(db_connect, get_messages_data_sql)

            data = []

            for message_data in messages_data:
                data.append({
                    'name': message_data[0],
                    'text': message_data[1],
                    'hours': message_data[2],
                    'minutes': message_data[3]
                })

            print(data)
            db_connect.close()
            await websocket.send(json.dumps({'type': 'messages', 'data': json.dumps(data)}))
        elif json.loads(message)['type'] == 'message':
            db_connect = create_connection()

            add_message_sql = f''' INSERT INTO `messages` (`name`, `message`, `hours`, `minutes`, `time`) VALUES ('{json.loads(message)['name']}', '{json.loads(message)['text']}', '{json.loads(message)['hours']}', '{json.loads(message)['minutes']}', {time.time()}) '''
            execute_query(db_connect, add_message_sql)

            for conn in connected:
                await conn.send(json.dumps({'type': 'new_message', 'data': {
                    'name': json.loads(message)['name'],
                    'text': json.loads(message)['text'],
                    'hours': json.loads(message)['hours'],
                    'minutes': json.loads(message)['minutes']
                }}))



start_server = websockets.serve(server, 'localhost', 5001)


# db_connect = create_connection()

# workers_table_sql = '''
#     CREATE TABLE messages(
#     id INTEGER PRIMARY KEY AUTO_INCREMENT,
#     name TEXT NULL,
#     message TEXT NULL,
#     hours TEXT NULL,
#     minutes TEXT NULL,
#     time FLOAT NULL
#     );
# '''
# execute_query(db_connect, workers_table_sql)

# if __name__ == '__main__':
# app.run(debug=True)
asyncio.get_event_loop().run_until_complete(start_server)
asyncio.get_event_loop().run_forever()