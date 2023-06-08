import asyncio
import os
from dotenv import load_dotenv
from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient
from azure.storage.blob import BlobServiceClient


load_dotenv()

EVENT_HUB_CONNECTION_STR = os.getenv("EVENT_HUB_CONNECTION_STR")
EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME")

async def run():

    producer = EventHubProducerClient.from_connection_string(
        conn_str=EVENT_HUB_CONNECTION_STR, eventhub_name=EVENT_HUB_NAME
    )

    async with producer:

        event_data_batch = await producer.create_batch()

        event_data_batch.add(EventData("Event 1"))
        event_data_batch.add(EventData("Event 2"))
        event_data_batch.add(EventData("Event 3"))
        
        await producer.send_batch(event_data_batch)
        print('events sent')

def upload_blob_file(blob_service_client: BlobServiceClient, container_name:str):
    container_client = blob_service_client.get_container_client(container=container_name)
    with open(file="masterA.csv",mode='rb') as data:
        blob_client = container_client.upload_blob(name='masterA.csv',data=data)

asyncio.run(run())