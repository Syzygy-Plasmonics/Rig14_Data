import os
import pandas as pd
from pandas.api.types import is_numeric_dtype
import numpy as np
from glob import glob
from upload_blob import *
from dotenv import load_dotenv
import asyncio
from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient
from datetime import datetime

start = datetime.now()

load_dotenv()

# AZURE CONFIG VARIABLES 
EVENT_HUB_CONNECTION_STR = os.getenv("EVENT_HUB_CONNECTION_STR")
EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME")
BLOB_STORAGE_CONNECTION_STRING = os.getenv("BLOB_STORAGE_CONNECTION_STRING")
rootPath = os.getenv("ROOT_PATH")

def determineStep(gasSelection:str, catalystBedAvg:float, heaterSetpointDiff=0):
    """
    input: gasSelection -> a string of the gas selection that was in the raw data
           catalystBedAvg -> a float of the catalyst bed average 
           heaterSetpointDiff -> a float of the heater setpoint difference
    output: a string categorizing the stage of which the reactor is in at that date-time
    description: uses the given input to determine the classification of the data
    """
    # classifies experiment step based on values
    if gasSelection=="Air" and (catalystBedAvg>=699 or heaterSetpointDiff==0): return "Calcination" 
    elif gasSelection=='Air' : return "Ramp up"
    if gasSelection=="Hydrogen" : return "Reduction"
    if gasSelection=="Argon" and catalystBedAvg<=50 and heaterSetpointDiff<=0 : return "Completed Ramp down"
    elif gasSelection=="Argon": return "Ramp down"
    if gasSelection=="Helium" : return "Leak Test"
    return "Uncategorized"

def findDataStart(fileLines:list):
    """
    input: fileLines -> a list of strings representing the lines in the file
    output: an integer of the index at which to start parsing data; returns -1 if no line was found
    description: iterates through each line in fileLines, looking for the string 'Date and time'
    """
    for idx, line in enumerate(fileLines):
        if 'Date and time' in line : return idx
    return -1

def createDataframe(fileName:str,furnaceLetter:str,lines:list):
    """
    input: fileName -> the name and absolute directory of the given file to convert to a dataframe
           furnaceLetter -> a string representing the type of furnace (A/B/C)
           lines -> a list of strings representing each line in the file
    output: a raw dataframe with the chosen columns from each rig
    description: converts the raw data to a dataframe for furnaceFileAnalyzer to run calculations on and manipulate
    """

    # declare only needed columns
    furnaceColumns = {
    "A" : ['Dateandtime', 'Elapsedtime', 'Tc_A-1', 'Tc_A-2', 'Tc_A-Enclosure',
               'Tc_A-4', 'Tc_A-5', 'Estop-A','MFC1_PSIA', 'MFC1_TC', 'MFC1_VCCM', 'MFC1_MCCM',
                'H2GasDetector-furnaceA','ExhaustFlowRate(ft/min)', 'HeaterVoltage', 'GasSelection','HeaterSetpoint',
               'Comment'],
    "B":['Dateandtime', 'Elapsedtime', 'Tc_B-1', 'Tc_B-2', 'Tc_B-Enclosure',
                   'Tc_B-4', 'Tc_B-5', 'Estop-B','MFC2_PSIA',
                   'MFC2_TC', 'MFC2_VCCM', 'MFC2_MCCM',
                    'H2GasDetector-furnaceB','ExhaustFlowRate(ft/min)', 'HeaterVoltage', 'GasSelection','HeaterSetpoint',
                   'Comment'] ,
    "C_1" : ['Dateandtime', 'Elapsedtime', 'TC0_1_C1', 'TC1_1_C2', 'TC2_1_C3',
            'TC3_1_C4', 'TC4_1_C5', 'TC5_1', 'MFC3_PSIA', 'MFC3_TC', 'MFC3_VCCM', 'MFC3_MCCM',
            'H2GasDetector-furnaceA', 'H2GasDetector-furnaceB',
            'ExhaustFlowRate(ft/min)', 'HeaterVoltage', 'GasSelection',
            'Comment'],
    "C_2" : ['Dateandtime', 'Elapsedtime', 'TC0_1_C1', 'TC1_1_C2', 'TC2_1_C3',
               'TC3_1_C4', 'TC4_1_C5', 'TC5_1', 'MFC3_PSIA', 'MFC3_TC', 'MFC3_VCCM', 'MFC3_MCCM',
               'H2GasDetector-furnaceA', 'H2GasDetector-furnaceB',
               'ExhaustFlowRate(ft/min)', 'HeaterVoltage', 'GasSelection',
               'Comment','ZoneTemperature_1','ZoneTemperature_2','ZoneTemperature_3',
                'ZoneSetpoint_1','ZoneSetpoint_2','ZoneSetpoint_3','ZoneOutput_1','ZoneOutput_2','ZoneOutput_3']
    }

    startIdx = findDataStart(lines)
    if startIdx == -1: return pd.DataFrame
    furnaceDf = pd.read_csv(fileName, skiprows=startIdx, sep="\,|\t", engine='python', parse_dates=[1], date_format = '%m/%d/%Y %I:%M:%S %p', on_bad_lines='error' )
        
    furnaceDf.columns=furnaceDf.columns.str.replace(' ','')

    try:
        if furnaceLetter=='C':
            furnaceDf = furnaceDf[furnaceColumns[furnaceLetter+"_2"]] if "ZoneTemperature_1" in furnaceDf.columns else furnaceDf[furnaceColumns[furnaceLetter+"_1"]]
        else:
            furnaceDf = furnaceDf[furnaceColumns[furnaceLetter]]    

        return furnaceDf
    except Exception as e:
        print(e)
        return pd.DataFrame



def furnaceFileAnalyzer(fileName:str):
    """
    input: fileName -> a string representing the filename to convert to a dataframe, should be passed with absolute path
    output: a string classifying the output dataframe, and the appropriate dataframe; if the file is unidentified or has messed up data, returns the name of the file
    description: uses the createDataFrame function and outputs a cleaned and formatted dataframe with calculated and assigned values
    """    
    try:
        with open(fileName,'r') as tempF:
            lines = tempF.readlines()

        tempF.close()

        if not lines : return "unknown", fileName

        # Rig C
        if "Rig14-CD" in lines[0]:
            furnaceType="Furnace 14C"

            furnaceDf = createDataframe(fileName,"C",lines)
            
            furnaceTemps = ['TC0_1_C1', 'TC1_1_C2','TC3_1_C4', 'TC4_1_C5']

        # Rig A or B
        elif "Rig-14" in lines[0] or "Rig14-AB" in lines[0]:
            if "Rig 14A" in lines[12]:
                furnaceDf = createDataframe(fileName,"A",lines)
                furnaceType="Furnace 14A"
                furnaceTemps = ['Tc_A-1', 'Tc_A-2','Tc_A-4', 'Tc_A-5']

            elif "Rig 14B" in lines[12]:
                furnaceDf = createDataframe(fileName,"B",lines)
                furnaceType="Furnace 14B"
                furnaceTemps = ['Tc_B-1', 'Tc_B-2','Tc_B-4', 'Tc_B-5']

            # Is Rig14 but unknown type (A/B/C)?
            else:
                return "unknown", fileName

        # furnace could not be recognized from file
        else:
            return "unknown", fileName
        
        # means the start index in the raw data was not found (see findDataStart)
        if furnaceDf.empty:
            return "error", fileName

        for temp in furnaceTemps:
            if is_numeric_dtype(furnaceDf[temp]) : furnaceDf[temp] = np.where(furnaceDf[temp]>=1300,np.nan,furnaceDf[temp])
        furnaceDf['furnace_type'] = furnaceType
        furnaceDf['experiment'] = fileName.split('\\')[-1]
        furnaceDf['Catalyst_bed_avg'] = furnaceDf[furnaceTemps].mean(axis=1)
        furnaceDf['max_temp'] = furnaceDf[furnaceTemps].max(axis=1)
        furnaceDf['min_temp'] = furnaceDf[furnaceTemps].min(axis=1)
        furnaceDf['Catalyst Bed Temperature Range'] = furnaceDf['max_temp']-furnaceDf['min_temp']

        if 'HeaterSetpoint' in furnaceDf.columns:
            furnaceDf['HeaterSetpoint_diff']=furnaceDf['HeaterSetpoint'].diff()
            furnaceDf['Step'] = furnaceDf[['GasSelection','HeaterSetpoint_diff','Catalyst_bed_avg']].apply(lambda x: determineStep(*x), axis=1)
        furnaceDf['Step'] = furnaceDf[['GasSelection','Catalyst_bed_avg']].apply(lambda x: determineStep(*x), axis=1)

        return furnaceType, furnaceDf
    except Exception as e:
        print(fileName)
        print(e)
        return "error", fileName


# to track the number of the event sent
furnaceCount = {
    "Furnace 14A":0,
    "Furnace 14B":0,
    "Furnace 14C":0
}

def chunkFiles(df:pd.DataFrame,fileName:str):
    """
    input: df -> a dataframe representing the data for each experiment 
           fileName -> the filename representing the type of furance for logging purposes
    output: None
    description: Breaks event data into 1000 lines and appends to the file queue
    """
    eventDict = df.to_dict('records')
    temp = []
    if len(eventDict) >1000:
        i= 0 
        j = 1000
        while j < len(eventDict)+1000:
            eventJson = df[i:j].to_json(orient='records')
            parsedJson = json.loads(eventJson)
            stringJson = json.dumps(parsedJson)
            temp.append(((fileName+str((furnaceCount[fileName])),stringJson)))
            i=j+1
            j+=1000
            furnaceCount[fileName]+=1
    else:
        eventJson = df.to_json(orient='records')
        parsedJson = json.loads(eventJson)
        stringJson = json.dumps(parsedJson)
        furnaceCount[fileName]+=1
        temp.append(((fileName+str((furnaceCount[fileName])),stringJson)))
    return temp


# grabs raw data from directory
furnaceFiles = glob(rootPath+"*[!zip]")

def main():
    """
    input: None
    output: None
    description: main function to run both process and send data
    """

    logF = open('errorLog.txt','w')

    # nested function to process and upload files
    async def processAndUpload(file):
        """
        input: file -> a string representing the name of the file to process and upload
        output: None
        description: processes the file into a dataframe and either classifies as an error or unknown, or chunks and adds to the eventhub
        """
        furnaceType,furnaceDf = furnaceFileAnalyzer(file)

        print("Processing: "+file)

        if furnaceType!="unknown" and furnaceType!='error':
            data = chunkFiles(furnaceDf,furnaceType)
            producer = EventHubProducerClient.from_connection_string(
            conn_str=EVENT_HUB_CONNECTION_STR, eventhub_name=EVENT_HUB_NAME
            )
        
            try:
                # adding to the eventhub
                async with producer:
                    for d in data:
                        fName, fData = d
                        event_data_batch = await producer.create_batch()

                        print("sending {} to event hub".format(fName))

                        event_data_batch.add(EventData(fData))

                        await producer.send_batch(event_data_batch)

                        print("{} sent to event hub".format(fName))
                    await producer.close()

            except Exception as e:
                print(e)

        else:
            logF.write(furnaceType+","+furnaceDf+"\n")

        print("Finished "+file)
        os.remove(file)

    print("Processing raw data...")
    # adds processing and uploading data files as distinct async tasks
    loop = asyncio.get_event_loop()
    tasks = [loop.create_task(processAndUpload(file)) for file in furnaceFiles]
    loop.run_until_complete(asyncio.wait(tasks))
    loop.close()

main()
print('Events Sent. Process done.')

# to track runtime
print(datetime.now()-start)
