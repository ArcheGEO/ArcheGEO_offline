import re
import psycopg2
import cellosaurus.string_constants as const


def get_existing_list(allin_list, column_name, table_name, user, password, port, database):
    existing_list = []
    # Establishing the connection
    conn = psycopg2.connect(database=database, user=user, password=password, port=port)
    # Creating a cursor object using the cursor() method
    cursor = conn.cursor()
    conn.autocommit = True
    prefix = 'SELECT ' + column_name + ' FROM public.' + table_name + ' WHERE ' + column_name + ' IN ('
    content = ''
    sql = ''
    if len(allin_list) > 0:
        for element in allin_list:
            content = content + '\'' + element + '\','
        sql = prefix + content.rstrip(',') + ');'
        # print('sql = ' + sql)
        cursor.execute(sql)
        row = cursor.fetchone()
        while row is not None:
            existing_list.append(str(row[0]))
            row = cursor.fetchone()
    # print(str(len(allin_list)) + ':' + str(allin_list))
    # print('++++++++++++++++++++++++++++++')
    # print(str(len(existing_list)) + ':' + str(existing_list))
    return existing_list


def preprocess_cellosaurus_data(output_filename, user, password, port, database):
    taxonomy_list = []
    disease_list = []
    with open(output_filename, encoding='utf-8') as reader:
        lines = reader.readlines()
    for line in lines:
        if const.taxonomy in line:
            result = re.search(const.taxonomy + '(.*);', line)
            taxonomy = result.group(1)
            if taxonomy not in taxonomy_list:
                taxonomy_list.append(result.group(1))
        if const.disease in line:
            result = re.search(const.disease + '(.*?);', line)
            disease = result.group(1)
            if disease not in disease_list:
                disease_list.append(result.group(1))
    reader.close()
    print('done extracting taxonomy and disease list')
    # get list of taxonomy and disease ncitid that actually exists in postgres DB
    existing_taxonomy_list = get_existing_list(taxonomy_list, 'ncbitaxonid', 'taxonomy', user, password, port, database)
    existing_disease_list = get_existing_list(disease_list, 'ncitid', 'nciterm', user, password, port, database)
    return existing_taxonomy_list, existing_disease_list


def clean_list(dirty_list, reference_list):
    clean_list = []
    for element in dirty_list:
        if element in reference_list:
            clean_list.append(element)
    return clean_list


def get_ncitid_list(taxonomy_taxonid, conn):
    ncitid_list = []
    sql = 'SELECT ncitid FROM public.taxonomy WHERE ncbitaxonid=\'' + taxonomy_taxonid + '\';'
    cursor = conn.cursor()
    cursor.execute(sql)
    row = cursor.fetchone()
    while row is not None:
        ncitid_list.append(str(row[0]))
        row = cursor.fetchone()
    return ncitid_list


def extract_details(user, password, port, database, output_filename):
    identifier = ''
    accession = ''
    synonym_list = []
    taxonomy_list = []
    disease_list = []
    anatomy_list = []
    cleaned_taxonomy_list = []
    cleaned_disease_list = []

    existing_taxonomy_list, existing_disease_list = preprocess_cellosaurus_data(output_filename, user, password,
                                                                                port, database)

    # Establishing the connection
    conn = psycopg2.connect(database=database, user=user, password=password, port=port)
    # Creating a cursor object using the cursor() method
    cursor = conn.cursor()
    conn.autocommit = True

    cellosaurus_insert = 'INSERT INTO public.cellosaurus(ac, nciterm_ncitid) VALUES '
    cellosaurus_content = ''
    cellosaurus_sql = ''

    synonym_insert = 'INSERT INTO public.cellosaurus_synonym(synonymname, ac, nciterm_ncitid) VALUES '
    synonym_content = ''
    synonym_sql = ''

    taxonomy_insert = 'INSERT INTO public.cellosaurus_taxonomy(ac, taxonomy_ncitid) VALUES '
    taxonomy_content = ''
    taxonomy_sql = ''

    anatomy_insert = 'INSERT INTO public.cellosaurus_anatomy(ac, nciterm_ncitid, anatomy_synonym) VALUES '
    anatomy_content = ''
    anatomy_sql = ''

    with open(output_filename, encoding='utf-8') as reader:
        lines = reader.readlines()
    for line in lines:
        if const.identifier in line:
            result = re.search(const.identifier+'(.*)', line)
            identifier = result.group(1)
        if const.accession in line:
            result = re.search(const.accession + '(.*)', line)
            accession = result.group(1)
        if const.synonym in line:
            result = re.search(const.synonym + '(.*)', line)
            synonym_list = result.group(1).split('; ')
        if const.taxonomy in line:
            result = re.search(const.taxonomy + '(.*);', line)
            taxonomy_list.append(result.group(1))
        if const.disease in line:
            result = re.search(const.disease + '(.*?);', line)
            disease_list.append(result.group(1))
        if const.anatomy_1 in line:
            result = re.search(const.anatomy_1 + '(.*).', line)
            if ';' in result.group(1):
                anatomy_list = result.group(1).split('; ')
            else:
                anatomy_list.append(result.group(1))
            # print(f'anatomy_list 1 = {anatomy_list}')
        if const.anatomy_2 in line:
            result = re.search(const.anatomy_2 + '(.*).', line)
            if ';' in result.group(1):
                anatomy_list = result.group(1).split('; ')
            else:
                anatomy_list.append(result.group(1))
            # print(f'anatomy_list 2 = {anatomy_list}')
        if const.terminate in line:
            # clean taxonomy and disease list
            cleaned_taxonomy_list = clean_list(taxonomy_list, existing_taxonomy_list)
            cleaned_disease_list = clean_list(disease_list, existing_disease_list)
            formattedIdentifier = identifier.replace('\'', '\'\'').upper()
            # we only proceed to save this entry there are ncit_id of disease found in the Postgres DB
            if len(cleaned_disease_list) > 0:
                for disease in cleaned_disease_list:
                    formattedDisease = disease.replace('\'', '\'\'')
                    cellosaurus_content = cellosaurus_content + f'(\'{accession}\',\'{formattedDisease}\'),'
                    synonym_content = synonym_content + f'(\'{formattedIdentifier}\',\'{accession}\',\'{formattedDisease}\'),'
                    if len(synonym_list) > 0:
                        for synonym in synonym_list:
                            formattedSynonym = synonym.replace('\'', '\'\'').upper()
                            synonym_content = synonym_content + f'(\'{formattedSynonym}\',\'{accession}\',\'{formattedDisease}\'),'
                    if len(cleaned_taxonomy_list) > 0:
                        for taxonomy_taxon in cleaned_taxonomy_list:
                            taxonomy_ncitid_list = get_ncitid_list(taxonomy_taxon, conn)
                            for taxonomy_ncitid in taxonomy_ncitid_list:
                                taxonomy_content = taxonomy_content + f'(\'{accession}\',\'{taxonomy_ncitid}\'),'
                    if len(anatomy_list) > 0:
                        for anatomy in anatomy_list:
                            # print(f'anatomy={anatomy}')
                            formattedAnatomy = anatomy.replace('\'', '\'\'').upper()
                            # print(f'formattedAnatomy={formattedAnatomy}')
                            anatomy_content = anatomy_content + f'(\'{accession}\',\'{formattedDisease}\',\'{formattedAnatomy}\'),'
            identifier = ''
            accession = ''
            synonym_list = []
            taxonomy_list = []
            disease_list = []
            anatomy_list = []
    reader.close()
    cellosaurus_sql = cellosaurus_insert + cellosaurus_content.rstrip(',') + ' ON CONFLICT DO NOTHING;'
    # print('cellosaurus_sql = '+cellosaurus_sql)
    cursor.execute(cellosaurus_sql)
    print("cellosaurus table updated successfully........")
    synonym_sql = synonym_insert + synonym_content.rstrip(',') + ' ON CONFLICT DO NOTHING;'
    # print('synonym_sql = ' + synonym_sql)
    cursor.execute(synonym_sql)
    print("cellosaurus_synonym table updated successfully........")
    taxonomy_sql = taxonomy_insert + taxonomy_content.rstrip(',') + ' ON CONFLICT DO NOTHING;'
    # print('taxonomy_sql = ' + taxonomy_sql)
    cursor.execute(taxonomy_sql)
    print("cellosaurus_taxonomy table updated successfully........")
    anatomy_sql = anatomy_insert + anatomy_content.rstrip(',') + ' ON CONFLICT DO NOTHING;'
    # print('anatomy_sql = ' + anatomy_sql)
    cursor.execute(anatomy_sql)
    print("cellosaurus_anatomy table updated successfully........")
