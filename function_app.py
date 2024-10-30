import logging
import azure.functions as func
import fastavro
import io

# Asegúrate de tener un esquema Avro que coincida con el que envías desde tu aplicación
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

        # Leer el mensaje Avro
        bytes_reader = io.BytesIO(avro_data)
        reader = fastavro.reader(bytes_reader, reader_options={'schema': AVRO_SCHEMA})
        logging.info('Avro data read successfully.', reader)
        # Procesar cada registro
        for record in reader:
            logging.info(f'Received record: {record}')

        return func.HttpResponse("Processed Avro data successfully.", status_code=200)

    except Exception as e:
        logging.error(f"Error processing Avro data: {e}")
        return func.HttpResponse("Failed to process Avro data.", status_code=500)
