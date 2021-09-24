import json
import pymysql

endpoint = 'xxxx'
username = 'xxxx'
password = 'xxxx'
database_name = 'covid_hospital_equipments'

connection = pymysql.connect(host=endpoint, user=username,passwd=password, db=database_name)

def lambda_handler(event, context):
    print('event values', event,)
    print('Context values', context,)
    #print('hospitalId: >>', event['pathParameters']['hospitalId'])
    print('coordinatorId: >>', event['pathParameters']['coordinatorId'])
    print('method: ', event['httpMethod'])
    
    if 'GET' == event['httpMethod']:
        return getCoordinatorById(event)
    elif 'POST' == event['httpMethod']:
        return createCoordinatorById(event)
    else:
        return {
        'statusCode': 501,
        'headers': {
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST'
        },
        'body': json.dumps({'errorMessage': 'Method Unimplemented'})
        }
  
    
def getCoordinatorById(event):
    hospitalId = event['pathParameters']['hospitalId']
    coordinatorId = event['pathParameters']['coordinatorId']
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
    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS, GET, POST'
        },
        'body': json.dumps(user)
        }

def getValue(obj,val):
    return obj.get(val, None)
    
def createCoordinatorById(event):
    print('hospitalId: >>', event['pathParameters']['hospitalId'])
    print('coordinatorId: >>', event['pathParameters']['coordinatorId'])
    cBody= json.loads(event['body'])
    hospitalId= event['pathParameters']['hospitalId'];
    print('req body: ', cBody)
    coordInsertquery = 'INSERT INTO user (firstName, lastName,contactNumber,userName,password,emailAddress,userRole,image,imageUrl,status,designation, secondContactNumber,salutation) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'
    hspContctInsertquery = 'INSERT INTO hospital_contact (hospitalId, userId) VALUES (%s,%s);'
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
    except Exception as e:
        print('Error Occured while saving: ',e)
        connection.rollback()
        raise e
    finally:
        cursor.close()
    return {
        'statusCode': 201,
        'headers': {
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST'
        },
        'body': json.dumps(cBody)
    }
    
