import os
import asyncio
from glob import glob
from dotenv import load_dotenv
from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient
import json

load_dotenv()

EVENT_HUB_CONNECTION_STR = os.getenv("EVENT_HUB_CONNECTION_STR")
EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME")
BLOB_STORAGE_CONNECTION_STRING = os.getenv("BLOB_STORAGE_CONNECTION_STRING")
EVENT_HUB_A = os.getenv("EVENT_HUB_A")
EVENT_HUB_B = os.getenv("EVENT_HUB_B")
EVENT_HUB_C = os.getenv("EVENT_HUB_C")

async def send_event_data():
    """
    input: None
    output: None
    description: sends chunked json files from furnace_processor to the azure event hub to be processed into parquet files through the DataLake.
    """

    # declare needed variables
    local_dir = "furnace_data/"
    furnace_data = glob(local_dir+"*.json")

    producer = EventHubProducerClient.from_connection_string(
        conn_str=EVENT_HUB_CONNECTION_STR, eventhub_name=EVENT_HUB_NAME
    )

    try:

        async with producer:
            # go through all data files in appropriate furnace raw data
            if furnace_data:
                for furnace_file in furnace_data:
                    event_data_batch = await producer.create_batch()

                    furnace_name = furnace_file.split('\\')[-1]
                    print("sending {} to event hub".format(furnace_name))

                    with open(file=furnace_file, mode='rb') as data:
                        json_obj = json.load(data)

                    data.close()
                    json_string = json.dumps(json_obj)
                    event_data_batch.add(EventData(json_string))
                    print(" {} sent to event hub".format(furnace_name))
                    await producer.send_batch(event_data_batch)
                    os.remove(furnace_file)
                print('Events all sent')
                await producer.close()
            else:
                print("no json files found")

    except Exception as e:
        print(e)


# asyncio.run(send_event_data())