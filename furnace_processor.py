import os
import pandas as pd
from pandas.api.types import is_numeric_dtype
import numpy as np
from glob import glob
from upload_blob import *

rootPath = "C:\\temp\\testdownload\\"

def determineStep(gasSelection:str, catalystBedAvg:float, heaterSetpointDiff=0):
    """
    input: gasSelection -> a string of the gas selection that was in the raw data
           catalystBedAvg -> a float of the catalyst bed average 
           heaterSetpointDiff -> a float of the heater setpoint difference
    output: a string categorizing the stage of which the reactor is in at that date-time
    description: uses the given input to determine the classification of the data
    """
    # classifies experiment step based on values
    if gasSelection=="Air" and catalystBedAvg>=699 or heaterSetpointDiff==0 : return "Calcination" 
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


# chunks dataframe into 1000 line json
def chunkDfJson(df:pd.DataFrame,fileName:str):
    """
    input: df -> a pandas dataframe to chunk
    output: none
    description: breaks up dataframes into 1000 line json files and saves due as the data surpasses the given max byte size for events. naming convention: furnaceX_#.json
    """
    i=0
    j=1000
    while j < len(df)+1000:
        df[i:j].to_json("furnace_data/{}_{}.json".format(fileName,int(j-1000)/1000),mode="w",orient="records")
        i=j+1
        j+=1000


# grabs raw data from directory
furnaceFiles = glob(rootPath+"*[!zip]")

furnaceData = {
    "Furnace 14A" : [],
    "Furnace 14B" : [],
    "Furnace 14C" : [],
    "unknown" : [],
    "error" : []
}

# makes output folder
if not os.path.exists("furnace_data"):
    os.mkdir("furnace_data")

# runs through each file, creates dataframe, then deletes file 
for file in furnaceFiles:
    furnaceType,furnaceDf = furnaceFileAnalyzer(file)
    # os.remove(file)
    furnaceData[furnaceType].append(furnaceDf)

# creates master dataframe, then chunks data using the chunkDfJson function
if len(furnaceData['Furnace 14A']) > 0:
    masterFurnaceA = pd.concat(furnaceData['Furnace 14A'],ignore_index=True)
    # masterFurnaceA.to_json("furnace_data/furnaceA.json",mode='w',orient='records')
    chunkDfJson(masterFurnaceA,"furnaceA")

if len(furnaceData['Furnace 14B']) > 0:
    masterFurnaceB = pd.concat(furnaceData['Furnace 14B'],ignore_index=True)
    chunkDfJson(masterFurnaceB,"furnaceB")
    # masterFurnaceB.to_json("furnace_data/furnaceB.json",mode="w",orient='records')
    # masterFurnaceB.to_csv("furnace_data/furnaceB.csv",mode='w',index=False,header=True)

if len(furnaceData['Furnace 14C']) > 0:
    masterFurnaceC = pd.concat(furnaceData['Furnace 14C'],ignore_index=True)
    chunkDfJson(masterFurnaceC,"furnaceC")
    # masterFurnaceC.to_json("furnace_data/furnaceC.json",mode="w",orient='records')
    # masterFurnaceC.to_json("furnace_data/furnaceC.json",mode='w',index=False,header=True)


# sends json files to event hub after processing raw data
# asyncio.run(send_event_data())
