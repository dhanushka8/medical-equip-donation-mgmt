import json
import logging
import pymysql
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    #logger.info("Request: %s", event)

    claims = event['requestContext']['authorizer']['claims']
    operationName = event['requestContext']['operationName']
    role = claims['custom:role']

    if role == 'ADMIN':
        if operationName == 'getAllDonorOrganizations':
            return getAllDonorOrgs (event)
        elif operationName == 'getDonorOrganization':
            return getDonorOrg (event)
        elif operationName == 'addDonorOrganizations':
            return addDonorOrganizations(event)
        elif operationName == 'updateDonorOrganization':
            return updateDonorOrganization(event)
        else:
            return {
                'statusCode': 501,
                'headers': retrieveHeaders (),
                'body': json.dumps('Operation Unknown')
            }
    else:
        return {
            'statusCode': 403,
            'headers': retrieveHeaders (),
            'body': json.dumps('Not Authorized')
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

# Get All Donor Organization Details
def getAllDonorOrgs (event):
    connection = getDBConnection(event)
    cursor = connection.cursor()
    
    cursor.execute('SELECT org.doId, org.orgName, org.phone, org.email, ad.addressId, ad.name, ad.street, ad.city, ad.district, ad.state, ad.zipcode, ad.country, usr.uid, usr.firstName, usr.lastName, usr.emailAddress FROM donor_organization AS org LEFT JOIN address AS ad ON org.addressId=ad.addressId LEFT JOIN user AS usr ON org.primary_contactId=usr.uid')
    rows = cursor.fetchall()
    
    cursor.close()
    connection.close()
    return {
        'statusCode': 200,
        'headers': retrieveHeaders (),
        'body': constructGetResponse (rows)
    }
     
# Get One Donor Org
def getDonorOrg (event): 
    donororgid = event['pathParameters']['donorOrganizationId']
    connection = getDBConnection(event)
    cursor = connection.cursor()
    
    cursor.execute('SELECT org.doId, org.orgName, org.phone, org.email, ad.addressId, ad.name, ad.street, ad.city, ad.district, ad.state, ad.zipcode, ad.country, usr.uid, usr.firstName, usr.lastName, usr.emailAddress FROM donor_organization AS org LEFT JOIN address AS ad ON org.addressId=ad.addressId LEFT JOIN user AS usr ON org.primary_contactId=usr.uid WHERE doId='+donororgid)
    rows = cursor.fetchall()
    
    cursor.close()
    connection.close()
    
    return {
        'statusCode': 200,
        'headers': retrieveHeaders (),
        'body': constructGetResponse (rows)
    }

#Construct the Get Response
def constructGetResponse (rows):
    orgList=[]
    for row in rows:
        org={'donorOrgId': row[0], 'name' : row[1], 'phone': row[2], 'email':row[3], 
        'address': { 'addressId': row[4], 'name': row[5], 'street': row[6], 'city': row[7], 'district': row[8], 'state': row[9], 'zipcode': row[10], 'country': row[11]},
        'primary_contact': { 'contactId': row[12], 'firstName': row[13], 'lastName': row[14], 'email': row[15] }
        }
        orgList.append(org)
    
    return json.dumps(orgList, indent=4, default=str)
    

#Add Donor Org
def addDonorOrganizations(event):
    return {
        'statusCode': 201,
        'headers': retrieveHeaders(),
        'body': json.dumps(event)
    }
    

#Update Donor Org
def updateDonorOrganization(event):
    return {
        'statusCode': 200,
        'headers': retrieveHeaders(),
        'body': json.dumps(event)
    }
