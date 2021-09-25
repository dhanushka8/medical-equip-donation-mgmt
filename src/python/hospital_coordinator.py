import json
import logging
import pymysql
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    print('event values', event,)
    print('Context values', context,)
    #print('hospitalId: >>', event['pathParameters']['hospitalId'])
    #print('coordinatorId: >>', event['pathParameters']['coordinatorId'])
    print('method: ', event['httpMethod'])
    
    operationName = event['requestContext']['operationName']
    
    if operationName == 'getCoordinators':
        return getCoordinators(event)
    elif operationName == 'getCoordinator':
        return getCoordinatorById(event)
    elif operationName == 'addCoordinators':
        return createCoordinatorById(event)
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

# Common HTTP Header
def retrieveHeaders ():
   return {
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'OPTIONS,POST,PUT,GET'
   }
  
#Get Coordinators for a hospital
def getCoordinators(event):
    hospitalId = event['pathParameters']['hospitalId']
    
    connection = getDBConnection(event)
    cursor = connection.cursor()

    cursor.execute('SELECT u.* from hospital_contact hc INNER JOIN user u ON hc.userId=u.uid WHERE userRole=2 AND hospitalId='+str(hospitalId))
    userRows = cursor.fetchall()
    usersList=[]
    
    for uRow in userRows:
        cursor.execute('SELECT a.* FROM address a WHERE a.userId='+str(uRow[0]))
        addressRows = cursor.fetchall()
        addressList = []
        
        for aRow in addressRows:
            address={'addressId': aRow[0], 'name': aRow[2], 'street': aRow[3], 'city' : aRow[4], 'district': aRow[5], 'state': aRow[6], 'zipcode': aRow[7], 'country': aRow[8]}
            addressList.append(address)
        
        user={'coordinatorId': uRow[0], 'firstName' : uRow[1], 'lastName': uRow[2], 'phone':uRow[3], 'email': uRow[6],
            'designation': uRow[13], 'secondContact': uRow[14] , 'address': addressList}
        usersList.append(user)
    
    cursor.close()
    connection.close()
    
    return {
        'statusCode': 200,
        'headers': retrieveHeaders (),
        'body': json.dumps(usersList)
    }


#Get a Coordinator by Coodinator ID
def getCoordinatorById(event):
    hospitalId = event['pathParameters']['hospitalId']
    coordinatorId = event['pathParameters']['coordinatorId']
    
    connection = getDBConnection(event)
    cursor = connection.cursor()

    cursor.execute('SELECT u.* from hospital_contact hc INNER JOIN user u ON hc.userId=u.uid WHERE userRole=2 AND hospitalId='+str(hospitalId) +' AND u.uid='+str(coordinatorId))
    userRows = cursor.fetchall()
    usersList=[]
    user= {}
    for uRow in userRows:
        cursor.execute('SELECT * FROM address WHERE userId='+str(uRow[0]))
        addressRows = cursor.fetchall()
        addressList = []
        
        for aRow in addressRows:
            address={'addressId': aRow[0], 'locationName': aRow[1], 'street': aRow[2], 'city' : aRow[3], 'districtName': aRow[4], 'state': aRow[5],'country': aRow[6], 'zipCode' : aRow[7]}
            addressList.append(address)
        
        user={'coordinatorId': uRow[0], 'firstName' : uRow[1], 'lastName': uRow[2], 'phone':uRow[3], 'email': uRow[6],
            'designation': uRow[13], 'secondContact': uRow[14] , 'address': addressList}
        #usersList.append(user)
    
    cursor.close()
    connection.close()
    
    return {
        'statusCode': 200,
        'headers': retrieveHeaders (),
        'body': json.dumps(user)
    }


def getValue(obj,val):
    return obj.get(val, None)
    
#Create Coordinator
def createCoordinatorById(event):
    print('hospitalId: >>', event['pathParameters']['hospitalId'])
    print('coordinatorId: >>', event['pathParameters']['coordinatorId'])
    cBody= json.loads(event['body'])
    hospitalId= event['pathParameters']['hospitalId'];
    print('req body: ', cBody)
    coordInsertquery = 'INSERT INTO user (firstName, lastName,contactNumber,userName,password,emailAddress,userRole,image,imageUrl,status,designation, secondContactNumber,salutation) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'
    hspContctInsertquery = 'INSERT INTO hospital_contact (hospitalId, userId) VALUES (%s,%s);'
    
    connection = getDBConnection(event)
    cursor = connection.cursor()
    try:
        cursor.execute(coordInsertquery, (getValue(cBody,'firstName'), getValue(cBody,'lastName') , getValue(cBody,'phone'), None, None, getValue(cBody,'email'), 2, None, None, 'ACTIVE', getValue(cBody,'designation'),  getValue(cBody,'secondContactNumber'), getValue(cBody,'salutation') ))
        userPkey = cursor.lastrowid
        cursor.execute(hspContctInsertquery, ( hospitalId,  hospitalId))
        hspContactId = cursor.lastrowid
        cAddress = getValue(cBody,'address')
        if cAddress:
            #cAddress= json.loads(event['body'])
            print('addres not null ', cAddress)
            cAddrsInsertQry = 'INSERT INTO address (addressType,name,street,city,district,state,zipcode,country,userId,hospitalId) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'
            cursor.execute(cAddrsInsertQry, ( getValue(cAddress,'addressType'), getValue(cAddress,'name'), getValue(cAddress,'street'), getValue(cAddress,'city'), getValue(cAddress,'district'), getValue(cAddress,'state'), getValue(cAddress,'zipcode'), getValue(cAddress,'country'), userPkey, hospitalId))
            adrsId = cursor.lastrowid
            cAddress['addressId'] = adrsId
            cBody['address'] = cAddress
        cBody['uid'] = userPkey 
        cBody['hospitalContactId'] = hspContactId 
        print('user added id: ', userPkey)
        connection.commit()
        
        return {
            'statusCode': 201,
            'headers': retrieveHeaders (),
            'body': json.dumps(cBody)
        }
    except Exception as e:
        print('Error Occured while saving: ',e)
        returnError = {'errorMessage': str(e), 'errorType': 'OperationalError'}
        connection.rollback()
        
        return {
            'statusCode': 500,
            'headers': retrieveHeaders (),
            'body': json.dumps(returnError)
        }
    finally:
        cursor.close()
        connection.close()
