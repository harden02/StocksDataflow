from google.cloud import bigquery
from decimal import Decimal

def retrieveSchema(projectID, bqTableID):
    """
    Retrieve the schema of a BigQuery table and convert it to a list of dictionaries.

    Args:
        projectID (str): The GCP project ID.
        bqTableID (str): The BigQuery table ID in the format 'dataset.table'.

    Returns:
        list: A list of dictionaries representing the schema of the table.
    """
    client = bigquery.Client(project=projectID)
    tableRef = f"{projectID}.{bqTableID}"
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