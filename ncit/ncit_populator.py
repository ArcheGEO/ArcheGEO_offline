import re
import psycopg2


def populate_thesaurus(user, password, port, database, filename):
    # Establishing the connection
    conn = psycopg2.connect(database=database, user=user, password=password, port=port)
    # Creating a cursor object using the cursor() method
    cursor = conn.cursor()
    conn.autocommit = True

    nciterm_insert = 'INSERT INTO public.nciterm(ncitid, ncimid, name) VALUES '
    nciterm_content = ''
    nciterm_sql = ''

    ncitermsynonym_insert = 'INSERT INTO public.nciterm_synonym(synonymname, ncitid) VALUES '
    ncitermsynonym_content = ''
    ncitermsynonym_sql = ''

    ncitermanatomy_insert = 'INSERT INTO public.nciterm_anatomy(nciterm_ncitid, anatomy_ncitid) VALUES '
    ncitermanatomy_content = ''
    ncitermanatomy_sql = ''

    with open(filename, encoding='utf-8') as reader:
        lines = reader.readlines()
        string_to_write = ''
        ncit_id = ''
        ncim_id = ''
        preferred_name = ''
        anatomy_id = None
        syn_list = []

        # Strips the newline character
        for line in lines:
            thisline = line.strip()
            if '<NHC0>' in thisline:
                result = re.search('<NHC0>(.*)</NHC0>', thisline)
                ncit_id = result.group(1)
            if '<P108>' in thisline:
                result = re.search('<P108>(.*)</P108>', thisline)
                preferred_name = result.group(1)
            if '<P207>' in thisline:
                result = re.search('<P207>(.*)</P207>', thisline)
                ncim_id = result.group(1)
            if '<P208>' in thisline:
                result = re.search('<P208>(.*)</P208>', thisline)
                ncim_id = result.group(1)
            if '<P90>' in thisline:
                result = re.search('<P90>(.*)</P90>', thisline)
                syn = result.group(1)
                syn = syn.upper()
                if syn not in syn_list:
                    syn_list.append(syn)
            if '<owl:Class rdf:about=\"http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#C' in thisline:
                ncit_id = ''
                preferred_name = ''
                ncim_id = ''
                anatomy_id = None
                syn_list = []
            if '<owl:someValuesFrom rdf:resource=\"http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#' in thisline:
                result = re.search('<owl:someValuesFrom rdf:resource=\"http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#(.*)\"/>', thisline)
                anatomy_id = result.group(1)
            if '<DONE>' in thisline:
                #completed entire entry, output the results
                nciterm_content = nciterm_content + '(\'%s\',\'%s\',\'%s\'),' % (ncit_id, ncim_id, preferred_name)
                for syn in syn_list:
                    ncitermsynonym_content = ncitermsynonym_content + '(\'%s\',\'%s\'),' % (syn, ncit_id)
                if anatomy_id is not None:
                    ncitermanatomy_content = ncitermanatomy_content + '(\'%s\',\'%s\'),' % (ncit_id, anatomy_id)
    print("finish populate_thesaurus")
    nciterm_sql = nciterm_insert + nciterm_content.rstrip(',') + ' ON CONFLICT DO NOTHING;'
    cursor.execute(nciterm_sql)
    print("nciterm table updated successfully........")

    ncitermsynonym_sql = ncitermsynonym_insert + ncitermsynonym_content.rstrip(',') + ' ON CONFLICT DO NOTHING;'
    cursor.execute(ncitermsynonym_sql)
    print("ncitermsynonym table updated successfully........")

    reader.close()

    # Closing the connection
    conn.close()


def populate_thesaurus_anatomy(user, password, port, database, filename):
    # Establishing the connection
    conn = psycopg2.connect(database=database, user=user, password=password, port=port)
    # Creating a cursor object using the cursor() method
    cursor = conn.cursor()
    conn.autocommit = True

    anatomy_insert = 'INSERT INTO public.anatomy(ncitid, ncimid, name) VALUES '
    anatomy_content = ''
    anatomy_sql = ''

    anatomysynonym_insert = 'INSERT INTO public.anatomy_synonym(synonymname, ncitid) VALUES '
    anatomysynonym_content = ''
    anatomysynonym_sql = ''

    with open(filename, encoding='utf-8') as reader:
        lines = reader.readlines()
        string_to_write = ''
        ncit_id = ''
        ncim_id = ''
        synonym_list = []

        # Strips the newline character
        for line in lines:
            thisline = line.strip()
            if '<NHC0>' in thisline:
                result = re.search('<NHC0>(.*)</NHC0>', thisline)
                ncit_id = result.group(1)
            if '<P108>' in thisline:
                result = re.search('<P108>(.*)</P108>', thisline)
                preferred_name = result.group(1)
                sn = preferred_name.upper()
                if sn not in synonym_list:
                    synonym_list.append(sn)
            if '<P207>' in thisline:
                result = re.search('<P207>(.*)</P207>', thisline)
                ncim_id = result.group(1)
            if '<P208>' in thisline:
                result = re.search('<P208>(.*)</P208>', thisline)
                ncim_id = result.group(1)
            if '<P90>' in thisline:
                result = re.search('<P90>(.*)</P90>', thisline)
                sn = result.group(1).upper()
                if sn not in synonym_list:
                    synonym_list.append(sn)
            if '<owl:Class rdf:about=\"http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#C' in thisline:
                ncit_id = ''
                ncim_id = ''
                synonym_list = []
            if '<DONE>' in thisline:
                # completed entire entry, output the results
                anatomy_content = anatomy_content + '(\'%s\',\'%s\',\'%s\'),' % (ncit_id, ncim_id, preferred_name)
                for sn in synonym_list:
                    anatomysynonym_content = anatomysynonym_content + '(\'%s\',\'%s\'),' % (sn, ncit_id)
    print("finish populate_thesaurus_anatomy")
    anatomy_sql = anatomy_insert + anatomy_content.rstrip(',') + ' ON CONFLICT DO NOTHING;'
    cursor.execute(anatomy_sql)
    print("anatomy table updated successfully........")
    anatomysynonym_sql = anatomysynonym_insert + anatomysynonym_content.rstrip(',') + ' ON CONFLICT DO NOTHING;'
    cursor.execute(anatomysynonym_sql)
    print("anatomy synonym table updated successfully........")
    reader.close()

    # Closing the connection
    conn.close()


def populate_thesaurus_taxonomy(user, password, port, database, filename):
    # Establishing the connection
    conn = psycopg2.connect(database=database, user=user, password=password, port=port)
    # Creating a cursor object using the cursor() method
    cursor = conn.cursor()
    conn.autocommit = True

    taxonomy_insert = 'INSERT INTO public.taxonomy(ncitid, ncbitaxonid, name) VALUES '
    taxonomy_content = ''
    taxonomy_sql = ''

    taxsynonym_insert = 'INSERT INTO public.taxonomy_synonym(synonymname, ncitid) VALUES '
    taxsynonym_content = ''
    taxsynonym_sql = ''

    with open(filename, encoding='utf-8') as reader:
        lines = reader.readlines()
        string_to_write = ''
        ncit_id = ''
        preferred_name = ''
        ncbitaxonid = ''
        syn_list = []

        # Strips the newline character
        for line in lines:
            thisline = line.strip()
            if '<NHC0>' in thisline:
                # print(thisline)
                result = re.search('<NHC0>(.*)</NHC0>', thisline)
                ncit_id = result.group(1)
            if '<P108>' in thisline:
                result = re.search('<P108>(.*)</P108>', thisline)
                preferred_name = result.group(1)
            if '<P331>' in thisline:
                result = re.search('<P331>(.*)</P331>', thisline)
                ncbitaxonid = result.group(1)
            if '<P90>' in thisline:
                result = re.search('<P90>(.*)</P90>', thisline)
                syn = result.group(1)
                syn = syn.upper()
                if syn not in syn_list:
                    syn_list.append(syn)
            if '<owl:Class rdf:about=\"http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#C' in thisline:
                ncit_id = ''
                preferred_name = ''
                syn_list = []
            if '<DONE>' in thisline:
                # completed entire entry, output the results
                taxonomy_content = taxonomy_content + '(\'%s\',\'%s\',\'%s\'),' % (ncit_id, ncbitaxonid, preferred_name)
                for syn in syn_list:
                    taxsynonym_content = taxsynonym_content + '(\'%s\',\'%s\'),' % (syn, ncit_id)
    print("finish populate_thesaurus")
    taxonomy_sql = taxonomy_insert + taxonomy_content.rstrip(',') + ' ON CONFLICT DO NOTHING;'
    cursor.execute(taxonomy_sql)
    print("taxonomy table updated successfully........")

    taxsynonym_sql = taxsynonym_insert + taxsynonym_content.rstrip(',') + ' ON CONFLICT DO NOTHING;'
    cursor.execute(taxsynonym_sql)
    print("tax synonym table updated successfully........")

    reader.close()

    # Closing the connection
    conn.close()
