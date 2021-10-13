import os
import config_extractor.file_extractor as file_extractor
import ncim.ncim_ncicuimap_processor as map_processor
import ncim.ncim_relationship_processor as rel_processor
import ncit.ncit_processor as ncit_processor
import cellosaurus.cellosaurus_processor as cellosaurus_processor
import gds.gds_processor as gds_processor
import scrapy
from scrapy.crawler import CrawlerProcess
from gds.scraper.spiders.gds_spider import GDSSpider
import psycopg2

configuration_file = '\\configuration\\configuration_file.txt'
configuration_credentials = '\\configuration\\configuration_credentials.txt'


# obtain the parameters of the database in postgreSQL and also the apikey for UMLS
def get_credentials(filepath):
    user_val = ''
    password_val = ''
    port_val = ''
    database_val = ''
    apikey_val = ''
    user_tag = 'PostgreSQL_user'
    password_tag = 'PostgreSQL_password'
    port_tag = 'PostgreSQL_port'
    database_tag = 'PostgreSQL_database'
    apikey_tag = 'UMLS_apikey'
    f = open(filepath + configuration_credentials, 'r')
    found_user = False
    found_password = False
    found_port = False
    found_database = False
    found_apikey = False
    for x in f:
        split_arr = x.split('=')
        if split_arr[0] == user_tag:
            user_val = split_arr[1].rstrip()
            found_user = True
        if split_arr[0] == password_tag:
            password_val = split_arr[1].rstrip()
            found_password = True
        if split_arr[0] == port_tag:
            port_val = split_arr[1].rstrip()
            found_port = True
        if split_arr[0] == database_tag:
            database_val = split_arr[1].rstrip()
            found_database = True
        if split_arr[0] == apikey_tag:
            apikey_val = split_arr[1].rstrip()
            found_apikey = True
        if found_user and found_password and found_port and found_database and found_apikey:
            f.close()
            return user_val, password_val, port_val, database_val, apikey_val
    f.close()
    return user_val, password_val, port_val, database_val, apikey_val


def check_postgres_ncithesaurus_exists(user, password, port, database):
    connection = None
    connection = psycopg2.connect(database='postgres', user=user, password=password, port=port)
    if connection is not None:
        connection.autocommit = True
        cur = connection.cursor()
        cur.execute('SELECT datname FROM pg_database;')
        list_database = cur.fetchall()
        if (database,) in list_database:
            print(f'{database} database already exist')
        else:
            sql = f'CREATE database {database};'
            # Creating a database
            print('sql=' + sql)
            cur.execute(sql)
            print(f'{database} database created successfully........')
        connection.close()
        print('Done')


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    path = os.path.dirname(os.path.abspath(__file__))
    # get all the file paths
    relationship_file, nciCuiMap_file = file_extractor.get_NCIM_File(path, configuration_file)
    owl_file, out_summary, err_summary, out_anatomy, err_anatomy, out_taxonomy, err_taxonomy \
        = file_extractor.get_NCIT_File(path, configuration_file)
    cellInput_file, cellOutput_file = file_extractor.get_Cellosaurus_File(path, configuration_file)
    spiderOutput_file = file_extractor.get_Spider_File(path, configuration_file)
    gds_file, ftp_file, summary_file, title_umls_file, description_umls_file, title_cellline_file, \
        description_cellline_file = file_extractor.get_GDS_File(path, configuration_file)
    # get postgreSQL credentials
    postgreSQL_user, postgreSQL_password, postgreSQL_port, postgreSQL_database, umls_apikey = get_credentials(path)
    print(
        f'user={postgreSQL_user}, password={postgreSQL_password}, port={postgreSQL_port}, database={postgreSQL_database}')
    # ensure that HOGWARTS database is present in postgreSQL
    check_postgres_ncithesaurus_exists(postgreSQL_user, postgreSQL_password, postgreSQL_port, postgreSQL_database)

    # process files related to NCI Thesaurus
    if len(owl_file) == 0 or len(out_summary) == 0 or len(err_summary) == 0 or len(out_anatomy) == 0 or \
            len(err_anatomy) == 0 or len(out_taxonomy) == 0 or len(err_taxonomy) == 0:
        print('Thesaurus.owl MISSING or paths of output and error files for thesaurus summary, anatomy and ' +
              'taxonomy NOT SPECIFIED!')
    else:
        print('Processing NCI Thesaurus...')
    ncit_processor.process_ncit(postgreSQL_user, postgreSQL_password, postgreSQL_port, postgreSQL_database, path,
                                owl_file, out_summary, err_summary, out_anatomy, err_anatomy,
                                out_taxonomy, err_taxonomy)
    # process files related to NCI Metathesaurus
    if len(relationship_file) == 0 or len(nciCuiMap_file) == 0:
        print('MRREL.RRF relationship and nci_code_cui_map_XXXXXX.dat files MISSING!')
    else:
        print('Processing NCI Metathesaurus - NCI2CUI map...')
        map_processor.process_nciCuiMap(path + nciCuiMap_file, postgreSQL_user, postgreSQL_password, postgreSQL_port,
                                        postgreSQL_database)
        print('Processing NCI Metathesaurus - relationships...')
        rel_processor.process_relationship(path + relationship_file, postgreSQL_user, postgreSQL_password,
                                           postgreSQL_port, postgreSQL_database, path)
    # process files related to Cellosaurus
    if len(cellInput_file) == 0 or len(cellOutput_file) == 0:
        print('Cellosaurus input and output files MISSING!')
    else:
        print('Processing Cellosaurus')
        cellosaurus_processor.process_cellosaurus(postgreSQL_user, postgreSQL_password, postgreSQL_port,
                                                  postgreSQL_database, path, cellInput_file, cellOutput_file)
    # crawl files from GDS
    if len(spiderOutput_file) == 0:
        print('Spider output file MISSING!')
    else:
        print('Running GDS Spider')
        process = CrawlerProcess()
        process.crawl(GDSSpider, outputfile=f'{path + spiderOutput_file}')
        process.start()
    # process crawled GDS filelist
    if len(gds_file) == 0 or len(ftp_file) == 0:
        print('GDS output files MISSING!')
    else:
        print('Running GDS processor')
        print(f'offline.py summary_file={summary_file}')
        gds_processor.process_gds(postgreSQL_user, postgreSQL_password, postgreSQL_port, postgreSQL_database,
                                  umls_apikey, path, spiderOutput_file, gds_file, ftp_file,
                                  summary_file, title_umls_file, description_umls_file, title_cellline_file,
                                  description_cellline_file)
    print('offline pipeline finished!')
