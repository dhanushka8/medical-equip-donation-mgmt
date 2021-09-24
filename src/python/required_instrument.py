import json
import logging
import pymysql

logger = logging.getLogger()
logger.setLevel(logging.INFO)

endpoint = 'xxxx'
username = 'xxxx'
password = 'xxxx'
database_name = 'covid_hospital_equipments'

connection = pymysql.connect(host=endpoint, user=username,passwd=password, db=database_name)

def lambda_handler(event, context):
    
    operation=event['requestContext']['operationName']
    
    if(operation == 'getAllRequiredInstruments'):
        return getAllRequiredInstruments()
    
    elif (operation == 'getRequiredInstrumentByID'):
        return getRequiredInstrumentByID(event['pathParameters']['instrumentCategoryId'])
    
    elif (operation == 'getRequiredIntrumentsForHospital'):
        return getRequiredIntrumentsForHospital(event['pathParameters']['hospitalId'])
    
    elif (operation == 'updateRequiredInstrumentsForHospital'):
        return updateRequiredInstrumentsForHospital(event)
        
    elif (operation == 'addRequiredInstrumentForHospital'):
        return addRequiredInstrumentForHospital(event)
 
    else:
        return {
            'statusCode': 501,
            'headers': {
                'Access-Control-Allow-Headers': 'Content-Type',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
            },
            'body': json.dumps('Operation Unknown')
        }
    

#Get All Required Instruments
def getAllRequiredInstruments():
    
    cursor = connection.cursor()
    cursor.execute('SELECT * from required_instrument inner join instrument_category on required_instrument.instrumentCode = instrument_category.code')
    
    rows = cursor.fetchall()
    cursor.close()
    
    instrumentList=[]
    
    for row in rows:
        category_str = {'instrumentCategoryId': row[14], 'code': row[15], 'name': row[16], 'image': row[17], 
        'imageurl': row[18], 'status': row[19]}
        
        instrument={'reqInsId': row[0], 'instrumentCategory' : category_str, 'veryUrgentQty': row[2], 'urgentQty':row[3], 
        'regularNeedQty': row[4], 'currentQty': row[5], 'excessQty': row[6], 'updatedUser': row[7], 'updatedTime': row[8], 'additionalStatusAQty': row[9],
        'additionalStatusBQty': row[10], 'comments': row[11], 'hospitalId': row[12], 'additionalComment': row[13]}
        instrumentList.append(instrument)
    
    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        'body': json.dumps(instrumentList, indent=4, sort_keys=False, default=str)
    }


#Get Required Instruments by Instrument Id
def getRequiredInstrumentByID(h_id):
    
    cursor = connection.cursor()
    cursor.execute('SELECT * from required_instrument inner join instrument_category on required_instrument.instrumentCode = instrument_category.code AND catId='+str(h_id))
    
    rows = cursor.fetchall()
    cursor.close()
    
    instrumentList=[]
    
    for row in rows:
        category_str = {'instrumentCategoryId': row[14], 'code': row[15], 'name': row[16], 'image': row[17], 
        'imageurl': row[18], 'status': row[19]}
        
        instrument={'reqInsId': row[0], 'instrumentCategory' : category_str, 'veryUrgentQty': row[2], 'urgentQty':row[3],
        'regularNeedQty': row[4], 'currentQty': row[5], 'excessQty': row[6], 'updatedUser': row[7], 'updatedTime': row[8], 'additionalStatusAQty': row[9],
        'additionalStatusBQty': row[10], 'comments': row[11], 'hospitalId': row[12], 'additionalComment': row[13]}
        instrumentList.append(instrument)
    
    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        'body': json.dumps(instrumentList, indent=4, sort_keys=False, default=str)
    }
    
    
#Get Required Instruments for a Hospital
def getRequiredIntrumentsForHospital(hospitalId):
    cursor = connection.cursor()
    cursor.execute('SELECT req.instrumentCode, cat.name, req.veryUrgentQty, req.urgentQty, req.regularNeedQty, req.currentQty, req.excessQty, req.updatedTime from required_instrument req inner join instrument_category cat on req.instrumentCode = cat.code AND req.hospitalId='+str(hospitalId))
    
    rows = cursor.fetchall()
    cursor.close()
    
    instrumentList=[]
    for row in rows:
        instrument={'instrumentCode': row[0], 'instrumentName': row[1], 'veryUrgentNeedQuantity': row[2], 'urgentNeedQuantity':row[3],
        'regularNeedQuantity': row[4], 'currentAtHandQuantity': row[5], 'excessQuantity': row[6], 'updatedDateTime': row[7]}
        instrumentList.append(instrument)
    
    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        'body': json.dumps(instrumentList, indent=4, sort_keys=False, default=str)
    }

#Update Required Instruments for Hospital
def updateRequiredInstrumentsForHospital(event):
    hospitalId = event['pathParameters']['hospitalId']
    cBody = json.loads(event['body'])
    
    #Update Each Required Instrument
    for reqInstrument in cBody:
        #logger.info("Instrument: %s", reqInstrument['instrumentCode'])
        
        try:
            cursor = connection.cursor()
            
            sql_update = 'UPDATE required_instrument req SET req.veryUrgentQty = %s, req.urgentQty = %s, req.regularNeedQty = %s, req.currentQty = %s, req.excessQty = %s WHERE req.hospitalId=%s AND req.instrumentCode=%s '
            cursor.execute(sql_update, (reqInstrument['veryUrgentNeedQuantity'], reqInstrument['urgentNeedQuantity'], reqInstrument['regularNeedQuantity'], reqInstrument['currentAtHandQuantity'], reqInstrument['excessQuantity'], hospitalId, reqInstrument['instrumentCode']))
            connection.commit()
        except Exception as e:
            logger.error("Error: %s", e)
            connection.rollback()
        finally:
            cursor.close()
            
    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,PUT,GET'
        },
        'body': json.dumps(cBody)
    }
    
#Add Required Instruments for Hospital
def addRequiredInstrumentForHospital(event):
    hospitalId = event['pathParameters']['hospitalId']
    cBody = json.loads(event['body'])
    
    #Update Each Required Instrument
    for reqInstrument in cBody:
        #logger.info("Instrument: %s", reqInstrument['instrumentCode'])
        
        try:
            cursor = connection.cursor()
            
    
            sql_insert = 'INSERT INTO required_instrument (hospitalId, instrumentCode, veryUrgentQty, urgentQty, regularNeedQty, currentQty, excessQty) VALUES (%s, %s, %s, %s, %s, %s, %s)'
            cursor.execute(sql_insert, (hospitalId, reqInstrument['instrumentCode'], reqInstrument['veryUrgentNeedQuantity'], reqInstrument['urgentNeedQuantity'], reqInstrument['regularNeedQuantity'], reqInstrument['currentAtHandQuantity'], reqInstrument['excessQuantity']))
            connection.commit()
        except Exception as e:
            logger.error("Error: %s", e)
            connection.rollback()
        finally:
            cursor.close()
    
    return {
        'statusCode': 201,
        'headers': {
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        'body': json.dumps(cBody)
    }
