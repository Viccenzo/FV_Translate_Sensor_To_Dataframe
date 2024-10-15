import os
import pandas as pd
import dotenv
import paho.mqtt.client as mqtt
import json
import time
import datetime

def createMqttConnection(mqtt_broker,mqtt_port,topic):
    client = mqtt.Client()
    client.connect(mqtt_broker, mqtt_port)
    client.on_connect = on_connect
    client.on_message = on_message
    client.loop_start()
    
def on_connect(client, userdata, flags, rc):
    print(f"Conectado com o código de resultado {rc}")
    #client.subscribe("DB_INSERT/#")  # Subscrição ao tópico passado via userdata
    #client.subscribe("DB_GERT_RECENT_ROW/")  # Subscrição ao tópico passado via userdata
    client.subscribe("Trans/Matheus/Nissan_Poste01")
    client.subscribe("message/Matheus/Nissan_Poste01")

def on_message(client, userdata, msg):
    print(f"Mensagem recebida no tópico {msg.topic}")
    print(f'Message payload: {msg.payload}')
    
    command, user, tableName = msg.topic.split('/')

    if command == "message":
        print(msg.payload)
    else:
        # Converte o payload de bytes para string
        payload_str = msg.payload.decode('utf-8')
        print(f"Payload como string: {payload_str}")

        try:
            # Carrega o JSON em um dicionário Python
            json_data = json.loads(payload_str)
            print(f"Dados em formato JSON: {json_data}")

            data={}

            # Verifica se o JSON é um array ou um objeto único
            if isinstance(json_data, list):
                # JSON é um array de objetos -> pode ser convertido diretamente
                data['df_data'] = pd.DataFrame(json_data)
            else:
                # JSON é um objeto único -> converte para DataFrame com index manual
                data['df_data'] = pd.DataFrame([json_data])

            #data['loggerRequestBeginTime'] = datetime.datetime.now().isoformat() # Colocar para que seja inserido coisas na tabela de LOG
            #data['loggerRequestEndTime'] = datetime.datetime.now().isoformat() # Colocar para que seja inserido coisas na tabela de LOG
            data['df_data'] = data['df_data'].to_json()

            print(data)

        except (json.JSONDecodeError, ValueError) as e:
            print(f"Erro ao processar o JSON: {e}")

        # Publica a resposta no tópico MQTT
        client.publish(f'DB_INSERT/{user}/{tableName}', json.dumps(data), qos=1)

    """mqttArivalTime = datetime.datetime.now()
    try:
        command,user,tableName = msg.topic.split("/")
    except Exception as e:
        print(e)
        client.publish(f'message/{user}/{tableName}', "Missing topic information on FV_mqtt_to_Server code ", qos=1)
        return
    
    if command == "DB_INSERT":
        try:    
            data = json.loads(msg.payload.decode())
            df = pd.read_json(data['df_data'])
            convert_to_numeric(df)
        except Exception as e:
            print(e)
            client.publish(f'message/{user}/{tableName}', f'error decoding mqtt to dataframe: {e}', qos=1)
            return
        if 'TIMESTAMP' not in df:
            print("Missing TIMESTAMP on Header")
            client.publish(f'message/{user}/{tableName}', f'Missing TIMESTAMP on Header', qos=1)
            return

        df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'])
        column_types = {name: map_dtype(dtype) for name, dtype in df.dtypes.items()}

        for engine in userdata:
            try:
                #Check if table exist and create one otherwise (rethink this)
                if not tableExists(tableName,userdata[0]):
                    print('The table does not exist, creating table')
                    createTable(df, engine, tableName,column_types)
                    createTableUser(engine,tableName,user)
                    primaryKey = primaryKeyExists(engine,tableName)
                    if primaryKey == []:
                        print(primaryKey)
                        addPrimarykey(engine,tableName,'TIMESTAMP') # for now the only primary key is going to be timestamp, changein the future
                

                # Check for missmach on headers
                missmach = headerMismach(tableName,engine,df)
                if(len(missmach) != 0):
                    #print(f'{len(missmach)} mismach were found, adding headers to database')
                    client.publish(f'message/{user}/{tableName}', f'The header you are providing doesent match the server headres. Those are the extra headers: {missmach}', qos=1)
                    return

                #upload to database
                print("uploading data to database")
                print(df)
                dataBaseStartUploadTime = datetime.datetime.now()
                dbMessage = uploadToDB(engine,df,tableName)
                dataBaseEndUploadTime = datetime.datetime.now()
                client.publish(f'message/{user}/{tableName}', dbMessage, qos=1)
                statusDF = createStatus(dataBaseEndUploadTime,
                                        tableName, 
                                        "Success", 
                                        "Data inserted successfully", 
                                        datetime.datetime.fromisoformat(data['loggerRequestBeginTime']), 
                                        dataBaseEndUploadTime, 
                                        (dataBaseEndUploadTime-datetime.datetime.fromisoformat(data['loggerRequestBeginTime'])).total_seconds(),
                                        datetime.datetime.fromisoformat(data['loggerRequestBeginTime']),
                                        datetime.datetime.fromisoformat(data['loggerRequestEndTime']),
                                        (datetime.datetime.fromisoformat(data['loggerRequestEndTime'])-datetime.datetime.fromisoformat(data['loggerRequestBeginTime'])).total_seconds(),
                                        datetime.datetime.fromisoformat(data['loggerRequestEndTime']),
                                        mqttArivalTime,
                                        (mqttArivalTime-datetime.datetime.fromisoformat(data['loggerRequestEndTime'])).total_seconds(),
                                        dataBaseStartUploadTime,
                                        dataBaseEndUploadTime,
                                        (dataBaseEndUploadTime-dataBaseStartUploadTime).total_seconds(),
                                        user
                                        )
                #criar uma função depois
                records = statusDF.to_dict(orient='records')
                print(records)
    
                metadata = MetaData()
                table = Table("TABLES_RUNNING_STATUS", metadata, autoload_with=engine)
                
                with engine.connect() as conn:
                    with conn.begin():
                        # Inserção em massa com tratamento de conflito
                        stmt = insert(table).values(records)

                        # Executa a inserção em lote com tratamento de conflito
                        conn.execute(stmt)
            except Exception as e:
                print(e)
                client.publish(f'message/{user}/{tableName}', f'error while treating data to upload to DB: {e}', qos=1)
    
    if command == "DB_GERT_RECENT_ROW":
        engine = userdata[0]

        #Check if table exist and create one otherwise (rethink this)
        if not tableExists(tableName,userdata[0]):
            client.publish(f'message/{user}/{tableName}', f'Table doesent exist', qos=1)
            return

        response = getRecentTimestamp(engine,tableName,user)

        
        client.publish(f'message/{user}/{tableName}', f'{response}', qos=1)
        """

def main():

    dotenv.load_dotenv()

    #Connecting to MQTT Brokers
    brokers = os.getenv("MQTT_BROKERS").split(';')

    for broker in brokers:
        arguments = broker.split(",")
        createMqttConnection(arguments[0],int(arguments[1]),arguments[2])

    while(1):
        time.sleep(1)

main()