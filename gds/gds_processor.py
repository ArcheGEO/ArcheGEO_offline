import psycopg2
import gds.nlp.nlp_disease as nlp_disease
import gds.nlp.nlp_cellline as nlp_cellline
import gds.ftp as ftp
from gds.nlp import nlp_helper


def check_postgres_tables_exist(user, password, port, database):
    # Establishing the connection
    conn = psycopg2.connect(database=database, user=user, password=password, port=port)
    # Creating a cursor object using the cursor() method
    cursor = conn.cursor()
    conn.autocommit = True

    # Creating table as per requirement
    # UMLS_MAPTO_NCIT TABLE
    sql = '''CREATE TABLE IF NOT EXISTS UMLS_MAPTO_NCIT(
                    UMLSID character varying(40), NCITID character varying(40),
                    PRIMARY KEY (UMLSID, NCITID)
                )'''
    cursor.execute(sql)
    # GDS_TITLE_MAPTP_DISEASE TABLE
    sql = '''CREATE TABLE IF NOT EXISTS GDS_TITLE_MAPTO_DISEASE(
                GDSID integer, DISEASE character varying(255),
                PRIMARY KEY (GDSID, DISEASE)
            )'''
    cursor.execute(sql)
    # GDS_DESCRIPTION_MAPTP_DISEASE TABLE
    sql = '''CREATE TABLE IF NOT EXISTS GDS_DESCRIPTION_MAPTO_DISEASE(
                   GDSID integer, DISEASE character varying(255),
                    PRIMARY KEY (GDSID, DISEASE)
                )'''
    cursor.execute(sql)
    # GDS_TITLEDESCRIPTION_MAPTP_DISEASE TABLE
    sql = '''CREATE TABLE IF NOT EXISTS GDS_TITLEDESCRIPTION_MAPTO_DISEASE(
                       GDSID integer, DISEASE character varying(255),
                        PRIMARY KEY (GDSID, DISEASE)
                    )'''
    cursor.execute(sql)

    # GDS_TITLE_MAPTP_UMLS TABLE
    sql = '''CREATE TABLE IF NOT EXISTS GDS_TITLE_MAPTO_UMLS(
                    GDSID integer, UMLSID character varying(40), UMLSNAME character varying(255),
                    PRIMARY KEY (GDSID, UMLSID)
                )'''
    cursor.execute(sql)
    # GDS_DESCRIPTION_MAPTP_UMLS TABLE
    sql = '''CREATE TABLE IF NOT EXISTS GDS_DESCRIPTION_MAPTO_UMLS(
                       GDSID integer, UMLSID character varying(40), UMLSNAME character varying(255),
                        PRIMARY KEY (GDSID, UMLSID)
                    )'''
    cursor.execute(sql)
    # GDS_TITLEDESCRIPTION_MAPTP_UMLS TABLE
    sql = '''CREATE TABLE IF NOT EXISTS GDS_TITLEDESCRIPTION_MAPTO_UMLS(
                           GDSID integer, UMLSID character varying(40), UMLSNAME character varying(255),
                            PRIMARY KEY (GDSID, UMLSID)
                        )'''
    cursor.execute(sql)

    # GDS_TITLE_MAPTP_CELLOSAURUS TABLE
    sql = '''CREATE TABLE IF NOT EXISTS GDS_TITLE_MAPTO_CELLOSAURUS(
            GDSID integer, AC character varying(10),
            PRIMARY KEY (GDSID, AC)
        )'''
    cursor.execute(sql)
    # GDS_DESCRIPTION_MAPTP_CELLOSAURUS TABLE
    sql = '''CREATE TABLE IF NOT EXISTS GDS_DESCRIPTION_MAPTO_CELLOSAURUS(
               GDSID integer, AC character varying(10),
                PRIMARY KEY (GDSID, AC)
            )'''
    cursor.execute(sql)
    # GDS_TITLEDESCRIPTION_MAPTP_CELLOSAURUS TABLE
    sql = '''CREATE TABLE IF NOT EXISTS GDS_TITLEDESCRIPTION_MAPTO_CELLOSAURUS(
                   GDSID integer, AC character varying(10),
                    PRIMARY KEY (GDSID, AC)
                )'''
    cursor.execute(sql)
    print("Tables created successfully........")
    # Closing the connection
    conn.close()


def process_gds(user, password, port, database, apikey, path, spiderOutput_file, gds_file, ftp_file, summary_file,
                title_UMLS_file, description_UMLS_file, title_cellLine_file, description_cellLine_file):
    # ---------to download, decompress and extract data from gzfiles
    print('generating gdslist ...')
    ftp.generate_gdslist_ftplist(path + spiderOutput_file, path + gds_file, path + ftp_file)
    print('downloading ftplist ...')
    ftp.download_ftplist(path, path + gds_file, path + ftp_file)
    print('decompressing gzfiles ...')
    ftp.decompress_gzfiles(path, path + ftp_file)
    print('extracting data from gzfiles ...')
    ftp.extract_data_gzfiles(path, path + ftp_file, path + summary_file)

    print('create postgreSQL tables for GDS  ...')
    check_postgres_tables_exist(user, password, port, database)
    # install models (uncomment the line below if the models have not been installed)
    print('install NLP models ...')
    nlp_helper.install_models()
    # ---------for performing nlp to extract disease related terms using scispacy on title and description
    print('run scispacy disease ...')
    nlp_disease.run_scispacy_disease(path + summary_file, path + title_UMLS_file, path + description_UMLS_file)
    print('run multiprocess_utr_rest for title ...')
    nlp_disease.multiprocess_utr_rest(apikey, path + title_UMLS_file, 'title', user, password, port, database)
    print('run multiprocess_utr_rest for description ...')
    nlp_disease.multiprocess_utr_rest(apikey, path + description_UMLS_file, 'description', user, password, port,
                                      database)
    print('run union_title_description_table_disease ...')
    nlp_disease.union_title_description_table_disease(user, password, port, database)

    # ---------for performing nlp to extract cellline related terms using scispacy on title and description
    print('run scispacy cellline ...')
    nlp_cellline.run_scispacy_cellline(path + summary_file, path + title_cellLine_file,
                                       path + description_cellLine_file, user, password, port, database)
    print('run save_cellline_to_db for title ...')
    nlp_cellline.save_cellline_to_db(path + title_cellLine_file, 'title', user, password, port, database)
    print('run save_cellline_to_db for description ...')
    nlp_cellline.save_cellline_to_db(path + description_cellLine_file, 'description', user, password, port, database)
    print('run union_title_description_table_cellline ...')
    nlp_cellline.union_title_description_table_cellline(user, password, port, database)
