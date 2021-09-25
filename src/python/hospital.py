import json
import logging
import pymysql
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    #logger.info("Request: %s", event)
    
    operation = event['requestContext']['operationName']
    
    if(operation == 'getAllHospitals'):
        return getAllHospitals (event)
    
    elif(operation == 'getHospitalDetails'):
        return getHospitalById (event)
        
    elif(operation == 'addHospitals'):
        return addHospitals (event)
    
    elif(operation == 'updateHospital'):
        return updateHospital (event)
     
    else: 
        return {
            'statusCode': 501,
            'headers': retrieveHeaders (),
            'body': json.dumps('Operation Unknown')
        }


# Get DB Connection
def getDBConnection(event):
    env = event['requestContext']['stage']
    #logger.info("Stage: %s", env)
    
    dbHost = os.environ[env + '_db_host']
    dbName = os.environ[env + '_db_name']
    dbUser = os.environ[env + '_db_user']
    dbPwd= os.environ[env + '_db_pwd']
    #logger.info("DB Host: %s", dbHost)
    
    return pymysql.connect(host=dbHost, db=dbName, user=dbUser, passwd=dbPwd)

# Common HTTP Headers
def retrieveHeaders ():
   return {
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'OPTIONS,POST,PUT,GET'
   }

#Get All Hospitals
def getAllHospitals (event):
    connection = getDBConnection(event)
    cursor = connection.cursor()
    cursor.execute('SELECT * from hospital')
    
    rows = cursor.fetchall()
    
    hospitalList=[]
    
    for row in rows:
        
        cursor.execute('SELECT * from address WHERE hospitalId='+str(row[0]))
        address1 = cursor.fetchone()
       
        
        address_str={}
        
        if address1 is not None:
            address_str={'addressId': address1[0], 'name': address1[2], 'street': address1[3], 'city': address1[4],
            'districtName': address1[5],'state': address1[6], 'zipcode': address1[7], 'country': address1[8]
            }
            
        hospital={'hospitalId': row[0], 'hospitalName' : row[1], 'district': row[2], 'status':row[3],
            'updatedDate': row[4], 'updatedUser': row[5], 
            'address':address_str}
        hospitalList.append(hospital)
    
    cursor.close()
    connection.close()
    
    return {
        'statusCode': 200,
        'headers': retrieveHeaders (),
        'body': json.dumps(hospitalList, indent=4, sort_keys=False, default=str)
    }
    

# Get Hospital Details by Id
def getHospitalById (event):
    h_id = event['pathParameters']['hospitalId']
    connection = getDBConnection(event)
    cursor = connection.cursor()
    cursor.execute('SELECT * from hospital WHERE hId='+h_id)
    
    rows = cursor.fetchall()
    cursor.close()
    
    hospitalList=[]
    
    for row in rows:
        
        cursor = connection.cursor()
        cursor.execute('SELECT * from address WHERE hospitalId='+str(row[0]))
        address1 = cursor.fetchone()
        cursor.close()
        
        address_str={}
        
        if address1 is not None:
            address_str={'addressId': address1[0], 'name': address1[2], 'street': address1[3], 'city': address1[4],
            'districtName': address1[5],'state': address1[6], 'zipcode': address1[7], 'country': address1[8]
            }
            
        hospital={'hospitalId': row[0], 'hospitalName' : row[1], 'district': row[2], 'status':row[3],
            'updatedDate': row[4], 'updatedUser': row[5],
            'address':address_str}
        hospitalList.append(hospital)
    
    
    connection.close()
    
    return {
        'statusCode': 200,
        'headers': retrieveHeaders (),
        'body': json.dumps(hospitalList, indent=4, sort_keys=False, default=str)
    }
    

#Add one or more Hospitals
def addHospitals (event):
    cBody = json.loads(event['body'])
    
    return {
            'statusCode': 201,
            'headers': retrieveHeaders (),
            'body': json.dumps(cBody)
    }

#Update one Hospital
def updateHospital (event):
    h_id = event['pathParameters']['hospitalId'],
    cBody = json.loads(event['body'])
    
    return {
            'statusCode': 200,
            'headers': retrieveHeaders (),
            'body': json.dumps(cBody)
    } 
    
