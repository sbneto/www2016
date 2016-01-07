__author__ = 'Samuel'

import numpy as np
import ujson as json
import uuid

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from oauth2client.client import GoogleCredentials

NUMPY_CONVERSIONS = {int: ('i8', int),
                     float: ('f8', float),
                     str: (np.dtype(object), str),
                     bool: ('b', bool),
                     'INTEGER': ('i8', int),
                     'STRING': (np.dtype(object), str),
                     'FLOAT': ('f8', float),
                     'BOOLEAN': ('b', bool),
                     'RECORD': (np.dtype(object), str),
                     'TIMESTAMP': (np.dtype(object), str),
                     'small_str': ('U128', str),  # limited size, more efficient string array
                     'json': (np.dtype(object), lambda x: json.loads(x)),
                     'array_json': (np.dtype(object), lambda x: np.array(json.loads(x)))
                     }


def get_service():
    """returns an initialized and authorized bigquery client"""
    credentials = GoogleCredentials.get_application_default()
    if credentials.create_scoped_required():
        credentials = credentials.create_scoped(
            'https://www.googleapis.com/auth/bigquery')
    return build('bigquery', 'v2', credentials=credentials)


def add_kwkey(key, source, destination):
    if key in source:
        destination[key] = source[key]


def get_rows_processor(columns_type):
    def get_rows(response, data, i=0):
        if response['jobComplete']:
            if 'rows' in response:
                for row in response['rows']:
                    for j, cell in enumerate(row['f']):
                        data[j][i] = NUMPY_CONVERSIONS[columns_type[j]][1](cell['v'])
                    i += 1
        else:
            raise RuntimeError('Query execution timeout.')
        return i
    return get_rows


def run_query(project, raw_query, columns_type=None, target=None, iterate='all', max_results=10000, **kwargs):
    """
      "query": { # [Pick one] Configures a query job.
        "flattenResults": true, # [Optional] Flattens all nested and repeated fields in the query results. The default value is true. allowLargeResults must be true if this is set to false.
        "useQueryCache": true, # [Optional] Whether to look for the result in the query cache. The query cache is a best-effort cache that will be flushed whenever tables in the query are modified. Moreover, the query cache is only available when a query does not have a destination table specified. The default value is true.
        "defaultDataset": { # [Optional] Specifies the default dataset to use for unqualified table names in the query.
          "projectId": "A String", # [Optional] The ID of the project containing this dataset.
          "datasetId": "A String", # [Required] A unique ID for this dataset, without the project name. The ID must contain only letters (a-z, A-Z), numbers (0-9), or underscores (_). The maximum length is 1,024 characters.
        },
        "destinationTable": { # [Optional] Describes the table where the query results should be stored. If not present, a new table will be created to store the results.
          "projectId": "A String", # [Required] The ID of the project containing this table.
          "tableId": "A String", # [Required] The ID of the table. The ID must contain only letters (a-z, A-Z), numbers (0-9), or underscores (_). The maximum length is 1,024 characters.
          "datasetId": "A String", # [Required] The ID of the dataset containing this table.
        },
        "priority": "A String", # [Optional] Specifies a priority for the query. Possible values include INTERACTIVE and BATCH. The default value is INTERACTIVE.
        "writeDisposition": "A String", # [Optional] Specifies the action that occurs if the destination table already exists. The following values are supported: WRITE_TRUNCATE: If the table already exists, BigQuery overwrites the table data. WRITE_APPEND: If the table already exists, BigQuery appends the data to the table. WRITE_EMPTY: If the table already exists and contains data, a 'duplicate' error is returned in the job result. The default value is WRITE_EMPTY. Each action is atomic and only occurs if BigQuery is able to complete the job successfully. Creation, truncation and append actions occur as one atomic update upon job completion.
        "allowLargeResults": True or False, # If true, allows the query to produce arbitrarily large result tables at a slight cost in performance. Requires destinationTable to be set.
        "createDisposition": "A String", # [Optional] Specifies whether the job is allowed to create new tables. The following values are supported: CREATE_IF_NEEDED: If the table does not exist, BigQuery creates the table. CREATE_NEVER: The table must already exist. If it does not, a 'notFound' error is returned in the job result. The default value is CREATE_IF_NEEDED. Creation, truncation and append actions occur as one atomic update upon job completion.
        "query": "A String", # [Required] BigQuery SQL query to execute.
        "preserveNulls": True or False, # [Deprecated] This property is deprecated.
        "tableDefinitions": { # [Experimental] If querying an external data source outside of BigQuery, describes the data format, location and other properties of the data source. By defining these properties, the data source can then be queried as if it were a standard BigQuery table.
          "a_key": {
            "compression": "A String", # [Optional] The compression type of the data source. Possible values include GZIP and NONE. The default value is NONE. This setting is ignored for Google Cloud Datastore backups.
            "csvOptions": { # Additional properties to set if sourceFormat is set to CSV.
              "encoding": "A String", # [Optional] The character encoding of the data. The supported values are UTF-8 or ISO-8859-1. The default value is UTF-8. BigQuery decodes the data after the raw, binary data has been split using the values of the quote and fieldDelimiter properties.
              "fieldDelimiter": "A String", # [Optional] The separator for fields in a CSV file. BigQuery converts the string to ISO-8859-1 encoding, and then uses the first byte of the encoded string to split the data in its raw, binary state. BigQuery also supports the escape sequence "\t" to specify a tab separator. The default value is a comma (',').
              "allowJaggedRows": True or False, # [Optional] Indicates if BigQuery should accept rows that are missing trailing optional columns. If true, BigQuery treats missing trailing columns as null values. If false, records with missing trailing columns are treated as bad records, and if there are too many bad records, an invalid error is returned in the job result. The default value is false.
              "quote": "\"", # [Optional] The value that is used to quote data sections in a CSV file. BigQuery converts the string to ISO-8859-1 encoding, and then uses the first byte of the encoded string to split the data in its raw, binary state. The default value is a double-quote ('"'). If your data does not contain quoted sections, set the property value to an empty string. If your data contains quoted newline characters, you must also set the allowQuotedNewlines property to true.
              "skipLeadingRows": 42, # [Optional] The number of rows at the top of a CSV file that BigQuery will skip when reading the data. The default value is 0. This property is useful if you have header rows in the file that should be skipped.
              "allowQuotedNewlines": True or False, # [Optional] Indicates if BigQuery should allow quoted data sections that contain newline characters in a CSV file. The default value is false.
            },
            "sourceFormat": "A String", # [Required] The data format. For CSV files, specify "CSV". For newline-delimited JSON, specify "NEWLINE_DELIMITED_JSON". For Google Cloud Datastore backups, specify "DATASTORE_BACKUP".
            "maxBadRecords": 42, # [Optional] The maximum number of bad records that BigQuery can ignore when reading data. If the number of bad records exceeds this value, an invalid error is returned in the job result. The default value is 0, which requires that all records are valid. This setting is ignored for Google Cloud Datastore backups.
            "ignoreUnknownValues": True or False, # [Optional] Indicates if BigQuery should allow extra values that are not represented in the table schema. If true, the extra values are ignored. If false, records with extra columns are treated as bad records, and if there are too many bad records, an invalid error is returned in the job result. The default value is false. The sourceFormat property determines what BigQuery treats as an extra value: CSV: Trailing columns JSON: Named values that don't match any column names Google Cloud Datastore backups: This setting is ignored.
            "sourceUris": [ # [Required] The fully-qualified URIs that point to your data in Google Cloud Storage. Each URI can contain one '*' wildcard character and it must come after the 'bucket' name. Size limits related to load jobs apply to external data sources, plus an additional limit of 10 GB maximum size across all URIs. For Google Cloud Datastore backups, exactly one URI can be specified, and it must end with '.backup_info'. Also, the '*' wildcard character is not allowed.
              "A String",
            ],
            "schema": { # [Optional] The schema for the data. Schema is required for CSV and JSON formats. Schema is disallowed for Google Cloud Datastore backups.
              "fields": [ # Describes the fields in a table.
                {
                  "fields": [ # [Optional] Describes the nested schema fields if the type property is set to RECORD.
                    # Object with schema name: TableFieldSchema
                  ],
                  "mode": "A String", # [Optional] The field mode. Possible values include NULLABLE, REQUIRED and REPEATED. The default value is NULLABLE.
                  "type": "A String", # [Required] The field data type. Possible values include STRING, INTEGER, FLOAT, BOOLEAN, TIMESTAMP or RECORD (where RECORD indicates that the field contains a nested schema).
                  "description": "A String", # [Optional] The field description. The maximum length is 16K characters.
                  "name": "A String", # [Required] The field name. The name must contain only letters (a-z, A-Z), numbers (0-9), or underscores (_), and must start with a letter or underscore. The maximum length is 128 characters.
                },
              ],
            },
          },
        }
    :param max_results:
    :param project:
    :param raw_query:
    :param columns_type:
    :param target:
    :param iterate: 'all', 'lazy' or 'none' (any other value is treated as none)
    :param kwargs:
    :return:
    """

    job = get_service().jobs()
    body = {
        'jobReference': {
            'projectId': project,
            'job_id': str(uuid.uuid4())
        },
        'configuration': {
            'query': {
                'query': raw_query,
                'priority': 'INTERACTIVE'
            }
        }
    }
    add_kwkey('priority', kwargs, body['configuration']['query'])

    if target:
        dataset, table = target.split('.')
        destination = {
          "projectId": project,
          "tableId": table,
          "datasetId": dataset,
        }
        add_kwkey('allowLargeResults', kwargs, body['configuration']['query'])
        add_kwkey('writeDisposition', kwargs, body['configuration']['query'])
        body['configuration']['query']['destinationTable'] = destination
    try:
        response = job.insert(projectId=project, body=body).execute()
        results_args = {"projectId": project,
                        "jobId": response['jobReference']['jobId'],
                        "maxResults": max_results}
        response = job.getQueryResults(**results_args).execute()
        while not response['jobComplete']:
            response = job.getQueryResults(**results_args).execute()

        labels = [f['name'] for f in response['schema']['fields']]
        try:
            iter(columns_type)
        except TypeError:
            columns_type = [u['type'] for u in response['schema']['fields']]
        row_processor = get_rows_processor(columns_type)
        if iterate == 'all':
            data = []
            for column_type in columns_type:
                data.append(np.zeros(int(response['totalRows']), dtype=NUMPY_CONVERSIONS[column_type][0]))
            start_index = 0
            start_index = row_processor(response, data, start_index)
            while "pageToken" in response:
                results_args["pageToken"] = response['pageToken']
                response = job.getQueryResults(**results_args).execute()
                start_index = row_processor(response, data, start_index)
            return data, labels
        elif iterate == 'lazy':
            def lazy_iterate():
                nonlocal response
                nonlocal results_args
                data = []
                for column_type in columns_type:
                    data.append(np.zeros(len(response['rows']), dtype=NUMPY_CONVERSIONS[column_type][0]))
                row_processor(response, data)
                yield data, labels
                while "pageToken" in response:
                    results_args["pageToken"] = response['pageToken']
                    response = job.getQueryResults(**results_args).execute()
                    data = []
                    for column_type in columns_type:
                        data.append(np.zeros(len(response['rows']), dtype=NUMPY_CONVERSIONS[column_type][0]))
                    row_processor(response, data)
                    yield data, labels
            return lazy_iterate
        else:
            return
    except HttpError as err:
        print("Error executing query:\n{}".format(err.content))
