import datetime
import pymysql
import json
import time
import sys
import os

# Elasticsearch information 
es_url = os.environ['ES_URL'] + '_bulk'  # Upload multiple documents
es_username = os.environ['ES_USERNAME']
es_password = os.environ['ES_PASSWORD']

def connect_to_mysql(db_host, db_user, db_password, db_name):
    return pymysql.connect(
        host=db_host,
        user=db_user,
        password=db_password,
        database=db_name
    )


def get_table_names(mydb):
    cursor = mydb.cursor()
    cursor.execute(f"SHOW TABLES")
    tables = [table[0] for table in cursor.fetchall() if table[0] not in ['Structure']]
    cursor.close()
    print(f'tables:{tables}')
    return tables


def get_table_schema(mydb, table_name):
    cursor = mydb.cursor()
    cursor.execute(f"DESCRIBE {table_name}")
    columns = [column[0] for column in cursor.fetchall()]
    cursor.close()
    return columns


def get_index(table_schema, column_name):
    my_index = None
    try:
        my_index = table_schema.index(column_name)
    except:
        pass
    return my_index


def execute_query(mydb, table_name, start, step):
    cursor = mydb.cursor()
    cursor.execute(f"SELECT * FROM {table_name} LIMIT {start}, {step}")
    rows = cursor.fetchall()
    cursor.close()
    return rows


def define_index_name(database_name, table_name):
    return f'{database_name}_{table_name.lower()}'


def define_filename(index_name):
    return 'bulk_' + index_name + ".json"


def write_rows_to_file(index_name, columns, rows , uid_index):
    with open(define_filename(index_name), "w") as file:
        for row in rows:
            uid = row[uid_index]
            # Check for date or other formatting issues
            date_index = get_index(columns, 'date')
            doc = {"index": {"_index": index_name, "_id": uid}}
            file.write(json.dumps(doc) + "\n")
            doc = {}
            for i in range(len(columns)):
                if i == date_index:
                    doc[columns[i]] = str(row[i])
                else:
                    doc[columns[i]] = row[i]
            doc.update({
                "type": "add"
            })
            file.write(json.dumps(doc) + "\n")
        file.write("\n")


def execute_curl_command(es_url, es_username, es_password, schema, index_name, start, step):
    filename = define_filename(index_name)
    response = os.popen(f'curl -XPOST -u "{es_username}:{es_password}" {es_url} --data-binary "@{filename}" -H "Content-Type: application/json"')
    response_text = response.read()
    response.close()
    if 'errors":false' in response_text:
        pass
    else:
        retry_count = 1
        while retry_count <= 3:
            print('error found!')
            if retry_count==1:
                print(f'response text: {response_text}')
            time.sleep(3)
            response = os.popen(f'curl -XPOST -u "{es_username}:{es_password}" {es_url} --data-binary "@{filename}" -H "Content-Type: application/json"')
            response_text = response.read()
            response.close()
            if 'errors":false' in response_text:
                print(f'success')
                break
            retry_count += 1
            if retry_count == 3 and 'errors":false' not in response_text:
                print(f'retry attempts failed, log and move on')
                log_message = f"Date:{datetime.datetime.now()}, Index:{index_name}, Query_Params:{str(start)},{str(step)}\n"
                try:
                    with open(f'error_log_{schema}.txt', 'a') as file:
                        # Write error message to file
                        file.write(log_message)
                except FileNotFoundError:
                    # Create file and write error message
                    with open(f'error_log_{schema}.txt', 'w') as file:
                        file.write(log_message)
                break


def main(host, user, passwd, schema):
    # Establish mysql connection
    mydb = connect_to_mysql(host, user, passwd, schema)
    # Query for list of tables names or change to req list here
    table_names = get_table_names(mydb)
    # list of preferred order
    priority_tables = ['Buffalo', 'St_Pete', 'Tampa', ]

    # sort table_names based on whether they're in priority_tables
    table_names = sorted(table_names, key=lambda x: priority_tables.index(x) if x in priority_tables else len(priority_tables))
    print(f'table_names: {table_names}')

    for table in table_names:
        print(f'uploading...: {table}')
        # Assign naming conventions
        index_name = define_index_name(schema, table)
        # Get all fields
        columns = get_table_schema(mydb, table)
        # Assign UID from table as id
        uid_index = get_index(columns, 'uid')
        # Initialize variables for documents/upload
        start = 0
        # interval = 50 # Good for rough results!
        # interval = 5  # Good for fine results!
        # interval = 500  # Good for rough reviews!
        interval = 100 # Good for fine reviews
        while True:
            rows = execute_query(mydb, table, start, interval)
            if not rows:
                break
            write_rows_to_file(index_name, columns, rows, uid_index)
            execute_curl_command(es_url, es_username, es_password, schema, index_name, start, interval)
            start += interval


if __name__ == '__main__':
    db_host = sys.argv[1]
    db_user = sys.argv[2]
    db_password = sys.argv[3]
    db_name = sys.argv[4]
    main(db_host, db_user, db_password, db_name)

# python3 mysql_to_opensearch.py arg1 arg2 arg3 arg4
