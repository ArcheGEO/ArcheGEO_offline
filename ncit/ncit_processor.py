import psycopg2
import ncit.ncit_extractor as ncit_extractor
import ncit.ncit_populator as ncit_populator


def create_tables(user, password, port, database):
    # Establishing the connection
    conn = psycopg2.connect(database=database, user=user, password=password, port=port)
    # Creating a cursor object using the cursor() method
    cursor = conn.cursor()
    conn.autocommit = True

    # Creating table as per requirement
    # ANATOMY TABLE
    sql = '''CREATE TABLE IF NOT EXISTS ANATOMY(
                NCITID character varying(20) NOT NULL PRIMARY KEY,
                NCIMID character varying(20),
                NAME character varying(100)
            )'''
    cursor.execute(sql)
    # ANATOMY_SYNONYM TABLE (synonym of anatomy)
    sql = '''CREATE TABLE IF NOT EXISTS ANATOMY_SYNONYM(
                       SYNONYMNAME character varying(600) NOT NULL,
                       NCITID character varying(20),
                       PRIMARY KEY (SYNONYMNAME, NCITID),
                       CONSTRAINT fk_anatomysynonym FOREIGN KEY(ncitid) REFERENCES anatomy(ncitid)
                    )'''
    cursor.execute(sql)
    # -------------------------------------------------------
    # TAXONOMY TABLE (taxonomy)
    sql = '''CREATE TABLE IF NOT EXISTS TAXONOMY(
                   NCITID character varying(20) NOT NULL PRIMARY KEY,
                   NCBITAXONID character varying(20),
                   NAME character varying(400)
                )'''
    cursor.execute(sql)
    # TAXONOMY_SYNONYM TABLE (synonym of taxonomy)
    sql = '''CREATE TABLE IF NOT EXISTS TAXONOMY_SYNONYM(
                   SYNONYMNAME character varying(600) NOT NULL,
                   NCITID character varying(20),
                   PRIMARY KEY (SYNONYMNAME, NCITID),
                   CONSTRAINT fk_taxonomysynonym FOREIGN KEY(ncitid) REFERENCES taxonomy(ncitid)
                )'''
    cursor.execute(sql)
    # -------------------------------------------------------
    # NCITERM TABLE (contains ncitid, ncimid, preferredname) ....ncimid is the NCIMetathesaurus ID.
    # the NCIMetathesaurus contains information about the
    sql = '''CREATE TABLE IF NOT EXISTS NCITERM(
           NCITID character varying(20) NOT NULL PRIMARY KEY,
           NCIMID character varying(20),
           NAME character varying(400)
        )'''
    cursor.execute(sql)
    # NCITERM_SYNONYM TABLE (synonym of NCITTERM)
    sql = '''CREATE TABLE IF NOT EXISTS NCITERM_SYNONYM(
           SYNONYMNAME character varying(600) NOT NULL,
           NCITID character varying(20),
           PRIMARY KEY (SYNONYMNAME, NCITID),
           CONSTRAINT fk_synonym FOREIGN KEY(ncitid) REFERENCES nciterm(ncitid)
        )'''
    cursor.execute(sql)
    print("Tables created successfully........")

    # Closing the connection
    conn.close()


def process_ncit(user, password, port, database, path, owl_file, out_summary, err_summary,
                                    out_anatomy, err_anatomy, out_taxonomy, err_taxonomy):
    ncit_file = path + owl_file
    print(f'ncit_file={ncit_file}')
    print(f'user={user}, password={password}, port={port}, database={database}')
    create_tables(user, password, port, database)
    ncit_extractor.extract_thesaurus_summary(path, ncit_file, path + out_summary, path + err_summary)
    ncit_extractor.extract_thesaurus_summary_anatomy(ncit_file, path + out_anatomy, path + err_anatomy)
    ncit_extractor.extract_thesaurus_summary_taxonomy(ncit_file, path + out_taxonomy, path + err_taxonomy)
    ncit_populator.populate_thesaurus_taxonomy(user, password, port, database, path + out_taxonomy)
    ncit_populator.populate_thesaurus_anatomy(user, password, port, database, path + out_anatomy)
    ncit_populator.populate_thesaurus(user, password, port, database, path + out_summary)
