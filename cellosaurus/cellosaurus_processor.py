# this project will open the cellosaurus.txt file to extract the accession number (AC), synonyms (SY), taxonomy (OX)
# and disease (DI), save them into a file (cellosaurus_details.txt) and then use the file to populate tables in
# postgres DB
# IMPT: We remove the headers for cellosaurus.txt to get the actual cellosaurus_data.txt
import psycopg2 as psycopg2
from cellosaurus import preprocessor
from cellosaurus import extractor


def check_postgres_tables_exist(user, password, port, database):
    # Establishing the connection
    conn = psycopg2.connect(database=database, user=user, password=password, port=port)
    # Creating a cursor object using the cursor() method
    cursor = conn.cursor()
    conn.autocommit = True

    # Creating table as per requirement
    # CELLOSAURUS TABLE (cellosaurus_accession, nciterm_ncitid)
    sql = '''CREATE TABLE IF NOT EXISTS CELLOSAURUS(
                   AC character varying(10),
                   NCITERM_NCITID character varying(20),
                   PRIMARY KEY (AC, NCITERM_NCITID),
                   CONSTRAINT fk_cellosaurus FOREIGN KEY(nciterm_ncitid) REFERENCES nciterm(ncitid)
                )'''
    cursor.execute(sql)

    # CELLOSAURUS_TAXONOMY TABLE (cellosaurus_accession, taxonomy_ncitid)
    sql = '''CREATE TABLE IF NOT EXISTS CELLOSAURUS_TAXONOMY(
               AC character varying(10),
               TAXONOMY_NCITID character varying(20),
               PRIMARY KEY (AC, TAXONOMY_NCITID),
               CONSTRAINT fk_cellosaurustaxonomy FOREIGN KEY(taxonomy_ncitid) REFERENCES taxonomy(ncitid)
            )'''
    cursor.execute(sql)

    # CELLOSAURUS_SYNONYM TABLE (synonym of cell line)
    sql = '''CREATE TABLE IF NOT EXISTS CELLOSAURUS_SYNONYM(
               SYNONYMNAME character varying(600) NOT NULL,
               AC character varying(10),
               NCITERM_NCITID character varying(20),
               PRIMARY KEY (SYNONYMNAME, AC, NCITERM_NCITID),
               CONSTRAINT fk_cellosaurussynonym FOREIGN KEY(ac, nciterm_ncitid) REFERENCES cellosaurus(ac, nciterm_ncitid)
            )'''
    cursor.execute(sql)

    # CELLOSAURUS_ANATOMY TABLE (synonym of cell line)
    sql = '''CREATE TABLE IF NOT EXISTS CELLOSAURUS_ANATOMY(
                   AC character varying(10),
                   NCITERM_NCITID character varying(20),
                   ANATOMY_SYNONYM character varying(255),
                   PRIMARY KEY (AC, NCITERM_NCITID, ANATOMY_SYNONYM),
                   CONSTRAINT fk_cellosaurussynonym FOREIGN KEY(ac, nciterm_ncitid) REFERENCES cellosaurus(ac, nciterm_ncitid)
                )'''
    cursor.execute(sql)

    print("Tables created successfully........")

    # Closing the connection
    conn.close()


def process_cellosaurus(user, password, port, database, path, cellInput_file, cellOutput_file):
    check_postgres_tables_exist(user, password, port, database)
    preprocessor.preprocess_cellosaurus(path + cellInput_file, path + cellOutput_file)
    extractor.extract_details(user, password, port, database, path + cellOutput_file)


