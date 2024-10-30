import logging
import azure.functions as func
import fastavro
import io

AVRO_SCHEMA = {
    "type": "record",
    "name": "ProductosAvro",
    "namespace": "org.productos.avro",
    "fields": [
        {
            "name": "Id",
            "type": "string"
        },
        {
            "name": "nombre",
            "type": "string"
        },
        {
            "name": "fecha",
            "type": "string"
        },
        {
            "name": "unidades",
            "type": "int"
        },
        {
            "name": "categoria",
            "type": {
                "type": "enum",
                "name": "CategoriaEnumAvro",
                "symbols": [
                    "GRANOS",
                    "LACTEOS",
                    "FRUTAS"
                ]
            }
        },
        {
            "name": "disponible",
            "type": "boolean"
        }
    ]
}

# Definir la función de Azure
app = func.FunctionApp()

@app.function_name(name="procesar_avro")
@app.route(route="procesar_avro")
def procesar_avro(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Received request to process Avro data.')

    try:
        # Obtener el cuerpo del mensaje
        avro_data = req.get_body()
        logging.info(f'Received Avro data: {avro_data}')

        # Leer el mensaje Avro
        bytes_reader = io.BytesIO(avro_data)
        reader = fastavro.reader(bytes_reader, reader_schema=AVRO_SCHEMA)
        logging.info('Avro data read successfully.')

        # Procesar cada registro
        for record in reader:
            logging.info(f'Received record: {record}')

        return func.HttpResponse("Processed Avro data successfully.", status_code=200)

    except Exception as e:
        logging.error(f"Error processing Avro data: {e}")
        return func.HttpResponse("Failed to process Avro data.", status_code=500)