import boto3
import csv

# Crear cliente DynamoDB
dynamodb = boto3.client('dynamodb', region_name='us-east-1')

# Configuración S3
s3 = boto3.client('s3')
bucket_name = 'jaguilar-ds-proyf-cloud-ingesta'
folder_name = 'dev'

# Nombre de la tabla de DynamoDB a la que se conectará
table_name = 't_cines'

# Función para cargar los datos desde DynamoDB
def cargar_datos():
    data = []
    # Realizamos un scan de la tabla DynamoDB
    response = dynamodb.scan(TableName=table_name)
    for item in response['Items']:
        cinema_id = item['cinema_id']['S']
        cinema_name = item['cinema_name']['S']
        # Reemplazamos saltos de línea en las direcciones por un espacio
        address = item['address']['S'].replace("\n", " ")
        number_of_halls = item['number_of_halls']['N']
        data.append([cinema_id, cinema_name, address, number_of_halls])
    
    return data

# Función para guardar los datos en un archivo CSV
def guardar_en_s3(data, filename):
    # Guardar en archivo CSV localmente primero
    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(["cinema_id", "cinema_name", "address", "number_of_halls"])  # Cabecera
        
        # Escribir los datos asegurándose de que las direcciones con saltos de línea se manejen correctamente
        for row in data:
            # Asegurarse de que las direcciones con saltos de línea se mantengan en una sola celda
            writer.writerow(row)
    
    # Subir el archivo CSV a S3
    s3.upload_file(filename, bucket_name, f"{folder_name}/{filename}")

# Función principal de ingesta
def ingesta_datos():
    data = cargar_datos()
    guardar_en_s3(data, 't_cines_data.csv')

if __name__ == "__main__":
    ingesta_datos()
