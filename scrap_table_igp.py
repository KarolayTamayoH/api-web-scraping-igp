import requests
from bs4 import BeautifulSoup
import boto3
import uuid
import json

def lambda_handler(event, context):
    # URL de la página web que contiene la tabla
    url = "https://ultimosismo.igp.gob.pe/ultimo-sismo/sismos-reportados"

    # Realizar la solicitud HTTP a la página web con headers
    headers_request = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    try:
        response = requests.get(url, headers=headers_request, timeout=20)
        if response.status_code != 200:
            return {
                'statusCode': response.status_code,
                'body': json.dumps({'error': 'Error al acceder a la página web'})
            }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': f'Error en la solicitud: {str(e)}'})
        }

    # Parsear el contenido HTML de la página web
    soup = BeautifulSoup(response.content, 'html.parser')

    # Encontrar la tabla en el HTML - buscar por clase o estructura
    table = soup.find('table', class_='table')
    if not table:
        # Intentar buscar cualquier tabla
        table = soup.find('table')

    if not table:
        return {
            'statusCode': 404,
            'body': json.dumps({
                'error': 'No se encontró la tabla en la página web',
                'html_preview': str(soup)[:500]  # Ver parte del HTML para debug
            })
        }

    # Extraer los encabezados de la tabla
    headers = [header.text.strip() for header in table.find_all('th')]

    # Extraer las filas de la tabla
    rows = []
    for row in table.find_all('tr')[1:]:  # Omitir el encabezado
        cells = row.find_all('td')
        if len(cells) > 0:  # Verificar que la fila tenga celdas
            row_data = {}
            for i, cell in enumerate(cells):
                if i < len(headers):
                    row_data[headers[i]] = cell.text.strip()
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
