#!/usr/bin/env python
import json
import pika
import json
connection = pika.BlockingConnection(pika.ConnectionParameters(host='147.182.132.98'))
channel = connection.channel()

channel.queue_declare(queue='check')
data={"nombre":"andree3","apellidos":"anchi3","correo":"anchi@uni.pe","clave":"+123456","dni":"8532185K","telefono":999888777,"amigos":["Anchi","Hasser","Masacre","Dashiel"]}
channel.basic_publish(exchange='', routing_key='check', body=json.dumps(data))
print(" [x] Sent 'Hello World!'")
connection.close()