import requests
from bs4 import BeautifulSoup
import boto3
import uuid
import json


def lambda_handler(event, context):
    # URL de la API real del IGP
    url = "https://ultimosismo.igp.gob.pe/api/ultimo-sismo/ajaxb/2025"

    headers_request = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json'
    }

    try:
        response = requests.get(url, headers=headers_request, timeout=20)
        if response.status_code != 200:
            return {
                'statusCode': response.status_code,
                'body': json.dumps({'error': f'Error al acceder a la API: {response.status_code}'})
            }

        # Parsear la respuesta JSON
        data = response.json()

        # Verificar si hay datos
        if not data or len(data) == 0:
            return {
                'statusCode': 404,
                'body': json.dumps({'error': 'No se encontraron datos sísmicos'})
            }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': f'Error en la solicitud: {str(e)}'})
        }

    # Procesar los datos
    rows = []
    for item in data[:10]:  # Solo tomar los primeros 10 elementos
        row_data = {
            'Reporte_sismico': item.get('codigo', '') or item.get('id', ''),
            'Referencia': item.get('referencia', ''),
            'Fecha_hora_local': item.get('fecha_hora_local', '') or item.get('fecha_local', ''),
            'Magnitud': str(item.get('magnitud', '')),
            'Profundidad': str(item.get('profundidad', '')),
            'Latitud': str(item.get('latitud', '')),
            'Longitud': str(item.get('longitud', ''))
        }
        rows.append(row_data)

    # Guardar los datos en DynamoDB
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('TablaWebScrappingIgp')

    # Eliminar todos los elementos de la tabla antes de agregar los nuevos
    scan = table.scan()
    with table.batch_writer() as batch:
        for each in scan['Items']:
            batch.delete_item(
                Key={
                    'id': each['id']
                }
            )

    # Insertar los nuevos datos
    for i, row in enumerate(rows, start=1):
        row['numero'] = str(i)
        row['id'] = str(uuid.uuid4())  # Generar un ID único para cada entrada
        table.put_item(Item=row)

    # Retornar el resultado como JSON
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': f'Se insertaron {len(rows)} registros sísmicos',
            'total': len(rows),
            'data': rows
        }, ensure_ascii=False)
    }
