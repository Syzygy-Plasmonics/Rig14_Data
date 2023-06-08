import os
import pandas as pd
from pandas.api.types import is_numeric_dtype
import numpy as np
from glob import glob

rootPath = "C:\\temp\\testdownload\\"

def determineStep(gasSelection:str, catalystBedAvg:str, heaterSetpointDiff=0):
    """
    classifies the experiment step based on ranges and variables according to Robert's logic
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
    given a list representing a raw data file, finds the row at which the data begins
    """
    for idx, line in enumerate(fileLines):
        if 'Date and time' in line : return idx
    return -1

def createDataframe(fileName:str,furnaceLetter:str,lines:str):
    """
    returns dataframe based on type of furnace data passed in
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
    "C" : ['Dateandtime', 'Elapsedtime', 'TC0_1_C1', 'TC1_1_C2', 'TC2_1_C3',
            'TC3_1_C4', 'TC4_1_C5', 'TC5_1', 'MFC3_PSIA', 'MFC3_TC', 'MFC3_VCCM', 'MFC3_MCCM',
            'H2GasDetector-furnaceA', 'H2GasDetector-furnaceB',
            'ExhaustFlowRate(ft/min)', 'HeaterVoltage', 'GasSelection',
            'Comment']
    }

    furnaceDf = pd.read_csv(fileName, skiprows=findDataStart(lines), sep="\,|\t", engine='python', parse_dates=[1], date_format = '%m/%d/%Y %I:%M:%S %p', on_bad_lines='error' )
        
    furnaceDf.columns=furnaceDf.columns.str.replace(' ','')
    furnaceDf = furnaceDf[furnaceColumns[furnaceLetter]]    

    return furnaceDf



def furnaceFileAnalyzer(fileName:str):
    """
    parses a file and converts to a dataframe in standarized column format with added identifiers and calculated values
    """    
    try:
        with open(fileName,'r') as tempF:
            lines = tempF.readlines()

        tempF.close()

        if not lines : return "unknown", fileName

        # Needs to check if rig is A/B or C/Saturn

        # Rig C or Saturn
        if "Rig14-CD" in lines[0]:
            furnaceType="Furnace 14C"

            furnaceDf = createDataframe(fileName,"C",lines)
            
            furnaceTemps = ['TC0_1_C1', 'TC1_1_C2','TC3_1_C4', 'TC4_1_C5']

        # Rig A or B
        elif "Rig-14" in lines[0]:
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

        return furnaceType, furnaceDf
    except Exception as e:
        print(fileName)
        print(e)
        return "error", fileName




# chunks dataframe into 1000 line json
def chunkDfJson(df:pd.DataFrame,fileName:str):
    i=0
    j=1000
    while j < len(df)+1000:
        df[i:j].to_json("furnace_data/{}_{}.json".format(fileName,int(j-1000)/1000),mode="w",orient="records")
        i=j+1
        j+=1000



furnaceFiles = glob(rootPath+"*[!zip]")

furnaceData = {
    "Furnace 14A" : [],
    "Furnace 14B" : [],
    "Furnace 14C" : [],
    "unknown" : [],
    "error" : []
}

if not os.path.exists("furnace_data"):
    os.mkdir("furnace_data")

for file in furnaceFiles:
    furnaceType,furnaceDf = furnaceFileAnalyzer(file)
    # os.remove(file)
    furnaceData[furnaceType].append(furnaceDf)

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



