from azure.storage.blob import (
    BlobServiceClient, 
    BlobClient, 
    ContainerClient
)
import os
import asyncio
from glob import glob
from dotenv import load_dotenv
from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient
from azure.storage.blob import BlobServiceClient
import json

load_dotenv()

EVENT_HUB_CONNECTION_STR = os.getenv("EVENT_HUB_CONNECTION_STR")
EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME")
BLOB_STORAGE_CONNECTION_STRING = os.getenv("BLOB_STORAGE_CONNECTION_STRING")
EVENT_HUB_A = os.getenv("EVENT_HUB_A")
EVENT_HUB_B = os.getenv("EVENT_HUB_B")
EVENT_HUB_C = os.getenv("EVENT_HUB_C")

async def send_event_data():

    # declare needed variables
    local_dir = "furnace_data/"
    furnace_data = glob(local_dir+"*[!zip]")

    producer = EventHubProducerClient.from_connection_string(
        conn_str=EVENT_HUB_CONNECTION_STR, eventhub_name=EVENT_HUB_NAME
    )

    try:

        async with producer:
            
            # go through all data files in appropriate furnace raw data
            for furnace_file in furnace_data:
                event_data_batch = await producer.create_batch()

                furnace_name = furnace_file.split('\\')[-1]
                print("sending {} as message".format(furnace_name))

                with open(file=furnace_file, mode='rb') as data:
                    json_obj = json.load(data)
                data.close()
                json_string = json.dumps(json_obj)
                event_data_batch.add(EventData(json_string))
                print("blob {} uploaded".format(furnace_name))
                await producer.send_batch(event_data_batch)
                os.remove(furnace_file)
            print('Events all sent')
            await producer.close()
            print('closed')

    except Exception as e:
        print(e)


asyncio.run(send_event_data())



# try:
#     # for the storage account connection 
#     blob_service_client = BlobServiceClient.from_connection_string(BLOB_STORAGE_CONNECTION_STRING)

#     # for the container connection
#     container_client = blob_service_client.get_container_client("data")
#     container_name = "data"
    
#     local_dir = "furnace_data/"
#     furnace_data = glob(local_dir+"*[!zip]")

#     # iterates through all the master csv files in the furnace_data folder and uploads them as blobs to the given container
#     for furnace_file in furnace_data:

#         furnace_name = furnace_file.split('\\')[-1]
#         blob_client = blob_service_client.get_blob_client(container=container_name, blob=furnace_name)
#         print("uploading {} to {} container".format(furnace_name,container_name,))

#         with open(file=furnace_file, mode='rb') as data:
#             # overwrites the previous csv file for the respective furnace
#             blob_client.upload_blob(data,overwrite=True)

#         print("blob {} uploaded".format(furnace_name))

#     blob_list = container_client.list_blobs()
#     for blob in blob_list:
#         print(blob.name)

# except Exception as e:
#     print(e)

