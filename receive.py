import json
import pika, sys, os
import mysql.connector

def main():
    config = {
    'user': 'root',
    'password': 'pass',
    'host': '127.0.0.1',
    'port': '3306',
    'database': 'pc3',
    'raise_on_warnings': True,}
    
    cnx = mysql.connector.connect(**config)    
    cursor = cnx.cursor()
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='147.182.132.98'))
    channel = connection.channel()

    channel.queue_declare(queue='check')
    channel.queue_declare(queue='create')
    
    def callback(ch, method, properties, body):
        message=json.loads(body)
        print(message)
        str="select * from pc3.Users where nombre ="+"'"+message['nombre']+"'"+" and apellidos="+"'"+ message['apellidos']+"' and dni="+"'"+ message['dni']+"'" 
        cursor.execute(str)
        result=cursor.fetchall()

        data={
        "response":"User exists! "+message['nombre']
        }

        for row in result:
            print(row)
        if(result!=[]):
            channel.basic_publish(exchange='', routing_key='create', body=json.dumps(data))
        else:
            try:
                sql="insert into pc3.Users (dni,nombre,apellidos,lugar,ubigeo,direccion) values (%s,%s,%s,%s,%s,%s)" 
                val=(message['dni'],message['nombre'],message['apellidos'],'','','')
                cursor.execute(sql,val)
                cnx.commit()
                print(cursor.rowcount, "Record inserted successfully into Laptop table")

            except mysql.connector.Error as error:
                print("Failed to insert record into Laptop table {}".format(error))
            
            channel.basic_publish(exchange='', routing_key='noExists', body=body)


    channel.basic_consume(queue='check', on_message_callback=callback, auto_ack=True)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)