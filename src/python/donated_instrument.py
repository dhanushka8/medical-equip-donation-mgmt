import json
import logging
import pymysql

logger = logging.getLogger()
logger.setLevel(logging.INFO)

endpoint = 'mysqldb-dev-1.cvseqwgjxncy.ap-southeast-1.rds.amazonaws.com'
username = 'admin'
password = 'nhep4SL!'
database_name = 'covid_hospital_equipments'

connection = pymysql.connect(host=endpoint, user=username,passwd=password, db=database_name)

def lambda_handler(event, context):
    #logger.info("Request: %s", event)
    
    hospitalId= event['pathParameters']['hospitalId']
    operationName = event['requestContext']['operationName']
    
    if operationName == 'getDonatedIntrumentsForHospital':
        response_body = getDonatedIntrumentsForHospital (hospitalId, event)
        response_code = 200
    elif operationName == 'addDonatedInstrumentsForHospital':
        response_body = addDonatedInstrumentsForHospital (hospitalId, event)
        response_code = 200
    elif operationName == 'updateDonatedInstrumentsForHospital':
        response_body = updateDonatedInstrumentsForHospital (hospitalId, event)
        response_code = 200
    else:
        response_body = json.dumps('Operation Unknown')
        response_code = 200
        
    
    return {
        'statusCode': response_code,
        'headers': {
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        'body': response_body
    }

def getDonatedIntrumentsForHospital (hospitalId, event):
    cursor = connection.cursor()
    cursor.execute('SELECT cat.code, cat.name, don.qty, don.donatedDate, don.status, don.comments, hos.hospitalName, org.orgName FROM donated_instrument AS don LEFT JOIN instrument_category AS cat ON don.instrumentCode=cat.code LEFT JOIN hospital AS hos ON don.hospitalId=hos.hid LEFT JOIN donor_organization AS org ON don.donorOrgId=org.doId WHERE don.hospitalId='+hospitalId)
   
    rows = cursor.fetchall()
    
    donationList=[]
    for row in rows:
        donation={'instrumentCode': row[0], 'instrumentName' : row[1], 'quantity': row[2], 'donatedDate':row[3], 
        'status': row[4], 'comments': row[5], 'hospital': row[6], 'donorOrg': row[7]
        }
        donationList.append(donation)
    return json.dumps(donationList, indent=4, default=str)
    
    
def addDonatedIntrumentsForHospital (hospitalId, event):
    return json.dumps('Hello from Lambda - addDonatedIntrumentsForHospital!')
    
def updateDonatedInstrumentsForHospital (hospitalId, event):
    return json.dumps('Hello from Lambda - updateDonatedInstrumentsForHospital!')