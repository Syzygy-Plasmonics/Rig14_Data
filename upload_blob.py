import os
import asyncio
from glob import glob
from dotenv import load_dotenv
from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient
import json
import aiofiles

load_dotenv()

# AZURE CONFIG VARIABLES 
EVENT_HUB_CONNECTION_STR = os.getenv("EVENT_HUB_CONNECTION_STR")
EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME")
BLOB_STORAGE_CONNECTION_STRING = os.getenv("BLOB_STORAGE_CONNECTION_STRING")

async def send_event_data():
    """
    input: None
    output: None
    description: sends chunked json files from furnace_processor to appropriate event hubs based on different furnaces to categorized output parquet files
    """


    # declare needed variables
    local_dir = "furnace_data/"
    furnace_data = glob(local_dir+"*.json")

    # old master eventhub connecction
    producer = EventHubProducerClient.from_connection_string(
        conn_str=EVENT_HUB_CONNECTION_STR, eventhub_name=EVENT_HUB_NAME
    )

    try:

# old event hub code which sends all files to same eventhub, mashing all data together
        async with producer:
            # go through all data files in appropriate furnace raw data
            if furnace_data:
                for furnace_file in furnace_data:

                    furnace_name = furnace_file.split('\\')[-1]
                    # furnace_name_pure = furnace_name.split('_')[0]

                    event_data_batch = await producer.create_batch()

                    print("sending {} to event hub".format(furnace_name))

                    async with aiofiles.open(file=furnace_file, mode='r') as data:
                        content = await data.read()
                    data.close()
                    json_string = json.loads(content)
                    json_string = json.dumps(json_string)
                    event_data_batch.add(EventData(json_string))
                    await producer.send_batch(event_data_batch)
                    print("{} sent to event hub".format(furnace_name))
                    os.remove(furnace_file)
                print('Events all sent')
                await producer.close()
            else:
                print("failed")

    except Exception as e:
        print(e)

# asyncio.run(send_event_data())