import json
import logging
import pymysql

logger = logging.getLogger()
logger.setLevel(logging.INFO)

endpoint = 'xxx'
username = 'xxx'
password = 'xxx'
database_name = 'covid_hospital_equipments'

connection = pymysql.connect(host=endpoint, user=username,passwd=password, db=database_name)

def lambda_handler(event, context):
    #logger.info("Request: %s", event)
    
    hospitalId = event['pathParameters']['hospitalId']
    operationName = event['requestContext']['operationName']
    
    if operationName == 'getDonatedIntrumentsForHospital':
        return getDonatedIntrumentsForHospital (hospitalId, event)
    elif operationName == 'addDonatedInstrumentsForHospital':
        return addDonatedInstrumentsForHospital (hospitalId, event)
    elif operationName == 'updateDonatedInstrumentsForHospital':
        return updateDonatedInstrumentsForHospital (hospitalId, event)
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


#Get Donated Instruments
def getDonatedIntrumentsForHospital (hospitalId, event):
    cursor = connection.cursor()
    cursor.execute('SELECT cat.code, cat.name, don.qty, don.donatedDate, don.status, don.comments, hos.hospitalName, org.orgName FROM donated_instrument AS don LEFT JOIN instrument_category AS cat ON don.instrumentCode=cat.code LEFT JOIN hospital AS hos ON don.hospitalId=hos.hid LEFT JOIN donor_organization AS org ON don.donorOrgId=org.doId WHERE don.hospitalId='+hospitalId)
   
    rows = cursor.fetchall()
    cursor.close()
    
    donationList=[]
    for row in rows:
        donation={'instrumentCode': row[0], 'instrumentName' : row[1], 'quantity': row[2], 'donatedDate':row[3], 
        'status': row[4], 'comments': row[5], 'hospital': row[6], 'donorOrg': row[7]
        }
        donationList.append(donation)
    
    
    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        'body': json.dumps(donationList, indent=4, default=str)
    }
    
    
# Add Donated Instruments
def addDonatedInstrumentsForHospital (hospitalId, event):
    
    cBody = json.loads(event['body'])
    logger.info("Request Body: %s", cBody)
    
    try:
        cursor = connection.cursor()
        #add_donated_instruments(IN hospitalId INT, IN instrumentCode VARCHAR(45), IN donateQty INT,IN donatedDate TIMESTAMP, IN status VARCHAR(45), IN comments VARCHAR(45), IN donorOrgId INT, IN donorUserId INT)
        args = [hospitalId, cBody['instrumentCode'], cBody['quantity'], cBody['donatedDate'], cBody['status'], cBody['comments'], cBody['donorOrganizationId'], cBody['donorUserId']]
        cursor.callproc('add_donated_instruments', args)
        connection.commit()
    except Exception as e:
        logger.error("Error: %s", e)
        connection.rollback()
        raise e
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
    
def updateDonatedInstrumentsForHospital (hospitalId, event):
    return json.dumps('Hello from Lambda - updateDonatedInstrumentsForHospital!')
