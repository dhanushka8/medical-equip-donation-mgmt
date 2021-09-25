import json
import logging
import pymysql
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    #logger.info("Request: %s", event)
    

    operationName = event['requestContext']['operationName']
    
    if operationName == 'getDonatedIntrumentsForHospital':
        return getDonatedIntrumentsForHospital (event)
    elif operationName == 'addDonatedInstrumentsForHospital':
        return addDonatedInstrumentsForHospital (event)

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

#Get Donated Instruments
def getDonatedIntrumentsForHospital (event):
    hospitalId = event['pathParameters']['hospitalId']
    connection = getDBConnection(event)
    cursor = connection.cursor()
    cursor.execute('SELECT cat.code, cat.name, don.qty, don.donatedDate, don.status, don.comments, hos.hospitalName, org.orgName FROM donated_instrument AS don LEFT JOIN instrument_category AS cat ON don.instrumentCode=cat.code LEFT JOIN hospital AS hos ON don.hospitalId=hos.hid LEFT JOIN donor_organization AS org ON don.donorOrgId=org.doId WHERE don.hospitalId='+hospitalId)
   
    rows = cursor.fetchall()
    cursor.close()
    connection.close()
    
    donationList=[]
    for row in rows:
        donation={'instrumentCode': row[0], 'instrumentName' : row[1], 'quantity': row[2], 'donatedDate':row[3], 
        'status': row[4], 'comments': row[5], 'hospital': row[6], 'donorOrg': row[7]
        }
        donationList.append(donation)
    
    
    return {
        'statusCode': 200,
        'headers': retrieveHeaders (),
        'body': json.dumps(donationList, indent=4, default=str)
    }
    
    
# Add Donated Instruments
def addDonatedInstrumentsForHospital (event):
    hospitalId = event['pathParameters']['hospitalId']
    cBody = json.loads(event['body'])
    #logger.info("Request Body: %s", cBody)
    
    try:
        connection = getDBConnection(event)
        cursor = connection.cursor()
        #add_donated_instruments(IN hospitalId INT, IN instrumentCode VARCHAR(45), IN donateQty INT,IN donatedDate TIMESTAMP, IN status VARCHAR(45), IN comments VARCHAR(45), IN donorOrgId INT, IN donorUserId INT)
        args = [hospitalId, cBody['instrumentCode'], cBody['quantity'], cBody['donatedDate'], cBody['status'], cBody['comments'], cBody['donorOrganizationId'], cBody['donorUserId']]
        cursor.callproc('add_donated_instruments', args)
        connection.commit()
        return {
            'statusCode': 201,
            'headers': retrieveHeaders (),
            'body': json.dumps(cBody)
        }
    except Exception as e:
        logger.error("Error: %s", e)
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
