from google.cloud import bigquery
from decimal import Decimal

def retrieveSchema(projectID, datasetID, tableID):
    """
    Retrieve the schema of a BigQuery table and convert it to a list of dictionaries.

    Args:
        project_id (str): The ID of the GCP project.
        dataset_id (str): The ID of the BigQuery dataset.
        table_id (str): The ID of the BigQuery table.

    Returns:
        list: A list of dictionaries representing the schema of the table.
    """
    client = bigquery.Client(project=projectID)
    tableRef = f"{projectID}.{datasetID}.{tableID}"
    table = client.get_table(tableRef)

    schema = table.schema

    schemaDict = {
        field.name: {'type': field.field_type.upper()} for field in schema
        }
    
    typeToDtype = {
        'STRING': 'str',
        'INTEGER': 'int',
        'FLOAT': 'float',
        'BOOLEAN': 'bool',
        'TIMESTAMP': 'datetime',
        'NUMERIC': Decimal,
    }

    typeToDefault = {
        'STRING': '',
        'INTEGER': 0,
        'FLOAT': 0.0,
        'BOOLEAN': False,
        'TIMESTAMP': None,
        'NUMERIC': Decimal(0),
    }

    for field, props in schemaDict.items():
        dtype = typeToDtype.get(props['type'].upper(), str)
        defaultValue = typeToDefault.get(props['type'].upper(), '')
        schemaDict[field]['dtype'] = dtype
        schemaDict[field]['default'] = defaultValue
    
    

    return schema