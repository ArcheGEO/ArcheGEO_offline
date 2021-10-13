import psycopg2


def process_nciCuiMap(nciCuiMap_file, user, password, port, database):
    print(f'nciCuiMap_file={nciCuiMap_file}')
    print(f'user={user}, password={password}, port={port}, database={database}')
    f = open(nciCuiMap_file, 'r')
    sql_prefix = 'INSERT INTO public.nciterm(ncitid, ncimid, name) VALUES '
    sql_content = ''
    for x in f:
        split_arr = x.split('|')
        ncit_id = split_arr[0]
        ncim_id = split_arr[1]
        preferred_name = split_arr[2]
        if '\'' in preferred_name:
            preferred_name = preferred_name.replace('\'', '&apos;')
        sql_content = sql_content + f'(\'{ncit_id}\',\'{ncim_id}\',\'{preferred_name}\'),'
    sql = sql_prefix + sql_content.rstrip(',') + ' ON CONFLICT DO NOTHING;'
    # print(sql)
    try:
        # Connect to an existing database
        connection = psycopg2.connect(user=user,
                                      password=password,
                                      port=port,
                                      database=database)

        # Create a cursor to perform database operations
        cursor = connection.cursor()
        # Print PostgreSQL details
        print("PostgreSQL server information")
        # Executing a SQL query
        cursor.execute(sql)
        connection.commit()
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")
