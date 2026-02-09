from flask import Flask, request
from google.cloud import pubsub_v1
import json

app = Flask(__name__)

@app.route('/', methods=['GET'])
def home():
    return {"status": "API está rodando!"}, 200

@app.route('/processar', methods=['POST'])
def processar():
    # Criar o client AQUI dentro da função, não no topo do arquivo!
    publisher = pubsub_v1.PublisherClient()
    
    data = request.get_json()
    usuario = data.get('usuario')
    
    message = {
        "usuario": usuario,
        "timestamp": "2025-02-09"
    }
    
    topic_path = publisher.topic_path('hello-4ch', 'hello-4ch-topic')
    future = publisher.publish(topic_path, json.dumps(message).encode())
    
    return {"status": "ok", "message_id": future.result()}