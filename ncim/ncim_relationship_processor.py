import psycopg2
import os


anatomy_file = 'anatomy_file.txt'
parent_file = 'parent_file.txt'
child_file = 'child_file.txt'
anatomy_primary = 'Is_Primary_Anatomic_Site_Of_Disease'
anatomy_associated = 'Is_Associated_Anatomic_Site_Of'
relationship_other = 'RO'
relationship_parent = 'PAR'
relationship_child = 'CHD'
source_nci = 'NCI'
anatomy_primary_type = 'PRIMARY'
anatomy_associated_type = 'ASSOCIATED'


def extract_data(file, filepath):
    anatomy_arr = []
    parent_arr = []
    child_arr = []
    f = open(file, 'r')
    for x in f:
        split_arr = x.split('|')
        relationship = split_arr[3]  # looking for PAR, CHD or RO
        source = split_arr[10]
        anatomic_site = split_arr[7]
        if relationship == relationship_parent and source == source_nci:
            parent_arr.append(x)
        if relationship == relationship_child and source == source_nci:
            child_arr.append(x)
        if relationship == relationship_other:
            if anatomic_site == anatomy_primary or anatomic_site == anatomy_associated:
                anatomy_arr.append(x)
    f.close()
    if not os.path.exists(filepath + '/generatedFile/'):
        os.makedirs(filepath + '/generatedFile/')
    f = open(filepath + '/generatedFile/' + anatomy_file, 'w')
    for x in anatomy_arr:
        f.write(x)
    f.close()
    f = open(filepath + '/generatedFile/' + child_file, 'w')
    for x in child_arr:
        f.write(x)
    f.close()
    f = open(filepath + '/generatedFile/' + parent_file, 'w')
    for x in parent_arr:
        f.write(x)
    f.close()


def create_tables(user, password, port, database):
    # Establishing the connection
    conn = psycopg2.connect(database=database, user=user, password=password, port=port)
    # Creating a cursor object using the cursor() method
    cursor = conn.cursor()
    conn.autocommit = True

    # NCITERM_ANATOMY TABLE (anatomy of NCITTERM - not all nciterms has a corresponding anatomy)
    sql = '''CREATE TABLE IF NOT EXISTS NCITERM_ANATOMY(
               NCITERM_NCIMID character varying(20) NOT NULL,
               ANATOMY_NCIMID character varying(20) NOT NULL,
               TYPE character varying(20),
               PRIMARY KEY (NCITERM_NCIMID, ANATOMY_NCIMID)
            )'''
    cursor.execute(sql)

    # NCITERM_PARENT TABLE (parent of NCITTERM - not all nciterms has a corresponding parent)
    sql = '''CREATE TABLE IF NOT EXISTS NCITERM_PARENT(
                   NCITERM_NCIMID character varying(20) NOT NULL,
                   PARENT_NCIMID character varying(20) NOT NULL,
                   PRIMARY KEY (NCITERM_NCIMID, PARENT_NCIMID)
                )'''
    cursor.execute(sql)

    # NCITERM_CHILD TABLE (parent of NCITTERM - not all nciterms has a corresponding child)
    sql = '''CREATE TABLE IF NOT EXISTS NCITERM_CHILD(
                   NCITERM_NCIMID character varying(20) NOT NULL,
                   CHILD_NCIMID character varying(20) NOT NULL,
                   PRIMARY KEY (NCITERM_NCIMID, CHILD_NCIMID)
                )'''
    cursor.execute(sql)
    print("Tables created successfully........")
    # Closing the connection
    conn.close()


# def postgreSQL_retrieve_ncitid_from_ncimid(ncim_id, tablename, connection):
#     ncitid_arr = []
#     # Creating a cursor object using the cursor() method
#     cursor = connection.cursor()
#     connection.autocommit = True
#     sql = f'SELECT ncitid FROM public.{tablename} WHERE ncimid=\'{ncim_id}\';'
#     cursor.execute(sql)
#     row = cursor.fetchone()
#     while row is not None:
#         ncitid_arr.append(str(row[0]))
#         row = cursor.fetchone()
#     return ncitid_arr


def populate_anatomy(filepath, user, password, port, database):
    # Establishing the connection
    conn = psycopg2.connect(database=database, user=user, password=password, port=port)
    conn.autocommit = True

    f = open(filepath + '/generatedFile/' + anatomy_file, 'r')
    sql_prefix = 'INSERT INTO public.nciterm_anatomy(nciterm_ncimid, anatomy_ncimid, type) VALUES '
    sql_content = ''
    for x in f:
        x_arr = x.split('|')
        disease_ncim_id = x_arr[0]
        anatomy_ncim_id = x_arr[4]
        anatomy_type = x_arr[7]
        # disease_ncit_id_arr = postgreSQL_retrieve_ncitid_from_ncimid(disease_ncim_id, 'nciterm', conn)
        # anatomy_ncim_id_arr = postgreSQL_retrieve_ncitid_from_ncimid(anatomy_ncim_id, 'anatomy', conn)
        if anatomy_type == anatomy_primary:
            anatomy_type_name = anatomy_primary_type
        else:
            anatomy_type_name = anatomy_associated_type
        # if len(disease_ncit_id_arr) > 0 and len(anatomy_ncim_id_arr) > 0:
        #    for disease_ncitid in disease_ncit_id_arr:
        #        for anatomy_ncitid in anatomy_ncim_id_arr:
        #            sql_content = sql_content + f'(\'{disease_ncitid}\',\'{anatomy_ncitid}\',\'{anatomy_type_name}\'),'
        sql_content = sql_content + f'(\'{disease_ncim_id}\',\'{anatomy_ncim_id}\',\'{anatomy_type_name}\'),'
    sql = sql_prefix + sql_content.rstrip(',') + ' ON CONFLICT DO NOTHING;'
    # print(sql)
    cursor = conn.cursor()
    cursor.execute(sql)
    f.close()
    conn.close()


def populate_parent(filepath, user, password, port, database):
    # Establishing the connection
    conn = psycopg2.connect(database=database, user=user, password=password, port=port)
    conn.autocommit = True

    f = open(filepath + '/generatedFile/' + parent_file, 'r')
    sql_prefix = 'INSERT INTO public.nciterm_parent(nciterm_ncimid, parent_ncimid) VALUES '
    sql_content = ''
    for x in f:
        x_arr = x.split('|')
        disease_ncim_id = x_arr[0]
        parent_ncim_id = x_arr[4]
        # disease_ncit_id_arr = postgreSQL_retrieve_ncitid_from_ncimid(disease_ncim_id, 'nciterm', conn)
        # parent_ncim_id_arr = postgreSQL_retrieve_ncitid_from_ncimid(parent_ncim_id, 'nciterm', conn)
        # if len(disease_ncit_id_arr) > 0 and len(parent_ncim_id_arr) > 0:
        #     for disease_ncitid in disease_ncit_id_arr:
        #         for parent_ncitid in parent_ncim_id_arr:
        #             sql_content = sql_content + f'(\'{disease_ncitid}\',\'{parent_ncitid}\'),'
        sql_content = sql_content + f'(\'{disease_ncim_id}\',\'{parent_ncim_id}\'),'
    sql = sql_prefix + sql_content.rstrip(',') + ' ON CONFLICT DO NOTHING;'
    # print(sql)
    cursor = conn.cursor()
    cursor.execute(sql)
    f.close()
    conn.close()


def populate_child(filepath, user, password, port, database):
    # Establishing the connection
    conn = psycopg2.connect(database=database, user=user, password=password, port=port)
    conn.autocommit = True

    f = open(filepath + '/generatedFile/' + child_file, 'r')
    sql_prefix = 'INSERT INTO public.nciterm_child(nciterm_ncimid, child_ncimid) VALUES '
    sql_content = ''
    for x in f:
        x_arr = x.split('|')
        disease_ncim_id = x_arr[0]
        child_ncim_id = x_arr[4]
        # disease_ncit_id_arr = postgreSQL_retrieve_ncitid_from_ncimid(disease_ncim_id, 'nciterm', conn)
        # child_ncim_id_arr = postgreSQL_retrieve_ncitid_from_ncimid(child_ncim_id, 'nciterm', conn)
        # if len(disease_ncit_id_arr) > 0 and len(child_ncim_id_arr) > 0:
        #     for disease_ncitid in disease_ncit_id_arr:
        #         for child_ncitid in child_ncim_id_arr:
        #             sql_content = sql_content + f'(\'{disease_ncitid}\',\'{child_ncitid}\'),'
        sql_content = sql_content + f'(\'{disease_ncim_id}\',\'{child_ncim_id}\'),'
    sql = sql_prefix + sql_content.rstrip(',') + ' ON CONFLICT DO NOTHING;'
    # print(sql)
    cursor = conn.cursor()
    cursor.execute(sql)
    f.close()
    conn.close()


def process_relationship(relationship_file, user, password, port, database, path):
    print(f'relationship_file={relationship_file}')
    print(f'user={user}, password={password}, port={port}, database={database}')
    extract_data(relationship_file, path)
    create_tables(user, password, port, database)
    populate_anatomy(path, user, password, port, database)
    print('finish populating anatomy table...')
    populate_parent(path, user, password, port, database)
    print('finish populating parent table...')
    populate_child(path, user, password, port, database)
    print('finish populating child table...')
