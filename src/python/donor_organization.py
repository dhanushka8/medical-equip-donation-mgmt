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

    claims = event['requestContext']['authorizer']['claims']
    operationName = event['requestContext']['operationName']
    role = claims['custom:role']

    if role == 'ADMIN':
        if operationName == 'getAllDonorOrganizations':
            response_body = getAllDonorOrgs (event)
            response_code = 200
        elif operationName == 'getDonorOrganization':
            donororgid = event['pathParameters']['donorOrganizationId']
            response_body = getDonorOrg (donororgid, event)
            response_code = 200
        else:
            response_body = json.dumps('Operation Unknown')
            response_code = 200
    else:
        response_code = 403
        response_body = json.dumps('Not Authorized')

    return {
        'statusCode': response_code,
        'headers': {
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        'body': response_body
    }

# Get All Donor Organization Details
def getAllDonorOrgs (event):
    cursor = connection.cursor()
    cursor.execute('SELECT org.doId, org.orgName, org.phone, org.email, ad.addressId, ad.name, ad.street, ad.city, ad.district, ad.state, ad.zipcode, ad.country, usr.uid, usr.firstName, usr.lastName, usr.emailAddress FROM donor_organization AS org LEFT JOIN address AS ad ON org.addressId=ad.addressId LEFT JOIN user AS usr ON org.primary_contactId=usr.uid')
   
    rows = cursor.fetchall()
    cursor.close()
    return constructGetResponse (rows)
     
# Get One Donor Org
def getDonorOrg (donororgid, event): 
    cursor = connection.cursor()
    cursor.execute('SELECT org.doId, org.orgName, org.phone, org.email, ad.addressId, ad.name, ad.street, ad.city, ad.district, ad.state, ad.zipcode, ad.country, usr.uid, usr.firstName, usr.lastName, usr.emailAddress FROM donor_organization AS org LEFT JOIN address AS ad ON org.addressId=ad.addressId LEFT JOIN user AS usr ON org.primary_contactId=usr.uid WHERE doId='+donororgid)
   
    rows = cursor.fetchall()
    cursor.close()
    return constructGetResponse (rows)

#Construct the Get response
def constructGetResponse (rows):
    orgList=[]
    for row in rows:
        org={'donorOrgId': row[0], 'name' : row[1], 'phone': row[2], 'email':row[3], 
        'address': { 'addressId': row[4], 'name': row[5], 'street': row[6], 'city': row[7], 'district': row[8], 'state': row[9], 'zipcode': row[10], 'country': row[11]},
        'primary_contact': { 'contactId': row[12], 'firstName': row[13], 'lastName': row[14], 'email': row[15] }
        }
        orgList.append(org)
    
    return json.dumps(orgList, indent=4, default=str)
    
