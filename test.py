def determineStep(gasSelection:str, catalystBedAvg:float, heaterSetpointDiff=None):
    """
    input: gasSelection -> a string of the gas selection that was in the raw data
           catalystBedAvg -> a float of the catalyst bed average 
           heaterSetpointDiff -> a float of the heater setpoint difference
    output: a string categorizing the stage of which the reactor is in at that date-time
    description: uses the given input to determine the classification of the data
    """
    print(heaterSetpointDiff)
    # classifies experiment step based on values
    if gasSelection=='Air':
        if heaterSetpointDiff!=None:
            if heaterSetpointDiff==0: return "Calcination"
            else: return "Ramp up"
        else:
            if catalystBedAvg>=699:
                return "Calcination" 
            else: 
                return "Ramp up"
    if gasSelection=="Hydrogen" : return "Reduction"
    if gasSelection=='Argon':
        if heaterSetpointDiff!=None:
            if heaterSetpointDiff<=0 and catalystBedAvg<=50:
                return "Completed Ramp down"
            elif heaterSetpointDiff<=0:
                return "Ramp down"
            else:
                return "Uncategorized"
        else:
            return "Ramp down"
    if gasSelection=="Helium" : return "Leak Test"
    return "Uncategorized"


def step_determiner(GasSelection,Catalyst_bed_avg):
    """ Helper function that creates a step value based on the GasSelection column and catalyst_bed_avg"""
    
    #if gas selection is air and the catalyst bed average temperature is greater than 699 Step is considered to be Calcination
    if (GasSelection=='Air') & (Catalyst_bed_avg>=699.0):
        step = 'Calcination'
    #if gas selection is hydrogen then the Step is considered to be Reduction
    elif GasSelection == 'Hydrogen':
        
        step = 'Reduction'
    
    #if gas selection is Argon then the Step is considered to be Ramp down
    elif GasSelection == 'Argon':
        
        step = 'Ramp Down'
    
    #if gas selection is Air then the Step is considered to be Ramp up
    elif GasSelection == 'Air':
        
        step = 'Ramp up'
    #if gas selection is Helium then the Step is considered to be Leak Test    
    elif GasSelection == "Helium":
        step = "Leak Test"
    #if none previous conditions apply label step as 0   
    else:
        step = 0
    
    
    return step

def step_determiner_setpoint(GasSelection,HeaterSetpoint_diff,Catalyst_bed_avg):
    """ Helper function that creates a step value based on the GasSelection column or the difference of the previous heater setpoint"""
    print(HeaterSetpoint_diff)
    
    #if gas selection is air and the heater setpoint is at 0.0 is considered to be Calcination
    if (GasSelection == 'Air') & (HeaterSetpoint_diff == 0.0):
        step = 'Calcination'
    #if gas selection is hydrogen then the Step is considered to be Reduction
    elif GasSelection == 'Hydrogen':
        
        step = 'Reduction'
    
    #if gas selection is Argon and heater setpoint is negative and average catalyst bed is less than 50 C consider it completed ramp down
    elif (GasSelection == 'Argon') & (HeaterSetpoint_diff <= 0) & (Catalyst_bed_avg<=50.0):
        
        step = "Completed Ramp down"
    
    #if gas selection is Air then the Step is considered to be Ramp up
    elif (GasSelection == 'Air') & (HeaterSetpoint_diff > 0):
        step = 'Ramp up'
    #if gas selection is Helium then the Step is considered to be Leak Test    
    elif GasSelection == "Helium":
        step = "Leak Test"

    #if Gas selection and heater setpoint is going down consider the step to be ramping down
    elif (GasSelection == 'Argon') & (HeaterSetpoint_diff <=0.0):
    
        step = "Ramp down"
    #if none previous conditions apply label step as Uncategorized
    else:
        step = "Uncategorized"
    
    
    return step


test1=  ("Air",7,0)
test2 = []
print(determineStep(test1[0],test1[1],test1[2]))
print(step_determiner(test1[0],test1[1]))
print(step_determiner_setpoint(test1[0],test1[1],test1[2]))