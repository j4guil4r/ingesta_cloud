import boto3
import csv

# Crear cliente DynamoDB
dynamodb = boto3.client('dynamodb', region_name='us-east-1')

# Configuración S3
s3 = boto3.client('s3')
bucket_name = 'jaguilar-ds-proyf-cloud-ingesta'
folder_name = 'dev'

# Nombre de la tabla de DynamoDB a la que se conectará
table_name = 't_visitas'

# Función para cargar los datos desde DynamoDB
def cargar_datos():
    data = []
    response = dynamodb.scan(TableName=table_name)
    for item in response['Items']:
        cinema_id = item['cinema_id']['S']
        user_id = item['user_id']['S']
        show_id = item['show_id']['S']
        date = item['date']['S']
        data.append([cinema_id, user_id, show_id, date])
    return data

# Función para guardar los datos en un archivo CSV
def guardar_en_s3(data, filename):
    # Guardar en archivo CSV localmente primero
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["cinema_id", "user_id", "show_id", "date"])
        writer.writerows(data)
    
    # Subir el archivo CSV a S3
    s3.upload_file(filename, bucket_name, f"{folder_name}/{filename}")

# Función principal de ingesta
def ingesta_datos():
    data = cargar_datos()
    guardar_en_s3(data, 't_visitas_data.csv')

if __name__ == "__main__":
    ingesta_datos()
