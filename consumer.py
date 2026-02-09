from google.cloud import pubsub_v1
from google.cloud import storage
import json

PROJECT_ID = "hello-4ch"
SUBSCRIPTION_ID = "hello-4ch-subscription"
BUCKET_NAME = "hello-4ch-bucket"

def callback(message):
    print(f"📩 Mensagem recebida: {message.data.decode('utf-8')}")
    
    data = json.loads(message.data.decode('utf-8'))
    usuario = data.get('usuario')
    timestamp = data.get('timestamp')
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(f"{usuario}_{timestamp}.txt")
    
    conteudo = f"Usuario: {usuario}\nTimestamp: {timestamp}\n"
    blob.upload_from_string(conteudo)
    
    print(f"✅ Arquivo salvo: {usuario}_{timestamp}.txt")
    message.ack()

def main():
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)
    
    print(f"🎧 Consumindo mensagens de: {subscription_path}")
    
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
        print("\n👋 Consumer encerrado!")

if __name__ == "__main__":
    main()