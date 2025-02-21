import csv
import boto3


ASU_ID = '1220103989'
DOMAIN_NAME = f"{ASU_ID}-simpleDB"

#SimpleDB client in US-East-1 region
sdb = boto3.client('sdb', region_name='us-east-1')

def create_and_populate_domain(csv_file):
    #creating a domain
    response = sdb.create_domain(DomainName=DOMAIN_NAME)
    print(f"Created SimpleDB domain '{DOMAIN_NAME}': {response}")

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

            print(f"Populated item '{image_file}' with recognition '{recongnition_result}': {operation_response}")


if __name__ == '__main__':
    csv_file = 'images.csv'
    create_and_populate_domain(csv_file)
