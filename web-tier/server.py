from flask import Flask, request, Response
import boto3
import os
import csv

ASU_ID = '1220103989'
BUCKET_NAME = f"{ASU_ID}-in-bucket"
DOMAIN_NAME = f"{ASU_ID}-simpleDB"
s3 = boto3.client('s3', region_name='us-east-1')
sdb = boto3.client('sdb', region_name='us-east-1')

app = Flask(__name__)

def create_and_populate_sdb_domain(csv_file):
    #creating a domain
    response = sdb.create_domain(DomainName=DOMAIN_NAME)

    #reading throught the provided csv file and populating simpleDB
    with open(csv_file, newline='') as csvfile:
        dict = csv.DictReader(csvfile)
        for row in dict:
            image_file = row['Image']   
            recongnition_result = row['Results']  

            #populating the DB
            operation_response = sdb.put_attributes(
                DomainName=DOMAIN_NAME,
                ItemName=image_file,
                Attributes=[
                    {
                        'Name': 'result',
                        'Value': recongnition_result,
                        'Replace': True
                    }
                ]
            )

def upload_to_S3(file):
    try:
        s3.upload_fileobj(file, BUCKET_NAME, file.filename)
    except Exception as e:
        print(f"Error uploading {file.filename} to S3: {e}")
        return Response("Error uploading file to S3", status=500)

def get_recognition_result(filename):
    #searching the simpleDB 
    search_result = sdb.get_attributes(
        DomainName=DOMAIN_NAME,
        ItemName=filename,
        ConsistentRead=True
    )

    # Looping through each attribute in the 'Attributes' list from the search result.
    # If an attribute's 'Name' equals 'result', return its corresponding 'Value'.
    for attr in search_result.get('Attributes', []):
        if attr.get('Name') == 'result':
            return attr.get('Value')
        

@app.route('/', methods=['POST'])
def handle_post_request():

    file = request.files['inputFile']
    
    upload_to_S3(file)

    file_name = os.path.splitext(file.filename)

    #Query SimpleDB for the recognition result
    recognition_result = get_recognition_result(file_name[0]) 
    response_text = f"{file_name[0]}:{recognition_result}"
    return Response(response_text, status=200, mimetype='text/plain')


if __name__ == '__main__':
    # csv_file = 'images.csv'
    # create_and_populate_sdb_domain(csv_file)
    app.run(host='0.0.0.0', port=8000, threaded=True)
