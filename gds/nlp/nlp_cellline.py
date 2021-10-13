from scispacy.linking import EntityLinker
import gds.nlp.nlp_helper as scispacy_helper
import psycopg2
import json
import re

# gdsid_reference = ['GDS4511', 'GDS4544']
gdsid_reference = []


def run_scispacy_cellline(data_summary_filename, title_cellLine_file, description_cellLine_file, user, password,
                          port, database):
    # load models
    # for scispacy version 0.3.0
    nlp_cellline = scispacy_helper.load_models('en_ner_jnlpba_md')
    # scispacy v0.4.0
    # linker = nlp_cell_line.get_pipe('scispacy_linker')
    # scispacy v0.3.0
    linker = EntityLinker(resolve_abbreviations=True, name="umls", threshold=0.75, max_entities_per_mention=3)
    nlp_cellline.add_pipe(linker)

    rfile = open(data_summary_filename, 'r', encoding='utf-8')
    Lines = rfile.readlines()
    count_line = 0
    count_cellline = 0

    # Establishing the connection
    conn = psycopg2.connect(database=database, user=user, password=password, port=port)
    # Creating a cursor object using the cursor() method
    cursor = conn.cursor()
    conn.autocommit = True

    # nested dictionary for storing the processed nlp data
    gds_title_nlp_dict = {}
    gds_description_nlp_dict = {}

    for line in Lines:
        # gdsid string
        if count_line % 4 == 0:
            gdsid = line.rstrip('\n')
            title = ''
            description = ''
        # title string
        if count_line % 4 == 1:
            title = line.rstrip('\n')
            # scispacy v0.4.0
            # gds_title_nlp_dict = extract_ncitid_cellline(nlp_cellline, cursor, gds_title_nlp_dict, gdsid, title)
            # scispacy v0.3.0
            gds_title_nlp_dict = extract_ncitid_cellline(nlp_cellline, cursor, linker, gds_title_nlp_dict, gdsid,
                                                         title, 'title')
        if count_line % 4 == 2:
            description = line.rstrip('\n')
            # scispacy v0.4.0
            # gds_description_nlp_dict = extract_ncitid_cellline(nlp_cellline, cursor, gds_description_nlp_dict,
            # gdsid, description)
            # scispacy v0.3.0
            gds_description_nlp_dict = extract_ncitid_cellline(nlp_cellline, cursor, linker, gds_description_nlp_dict,
                                                               gdsid, description, 'description')
        if count_line % 4 == 3:
            count_cellline += 1
        count_line += 1
    rfile.close()
    # output_s1 = pprint.pformat(gds_title_nlp_dict)
    # output_s2 = pprint.pformat(gds_description_nlp_dict)

    with open(title_cellLine_file, 'w', encoding='utf-8') as file1:
        json.dump(gds_title_nlp_dict, file1, indent=4, sort_keys=True)
    with open(description_cellLine_file, 'w', encoding='utf-8') as file2:
        json.dump(gds_description_nlp_dict, file2, indent=4, sort_keys=True)


def get_cellline_accession_list(cursor, cellline_string_array):
    accession_list = []
    sql_prefix = 'SELECT ac FROM public.cellosaurus_synonym WHERE synonymname IN ('
    sql_content = ''
    sql = ''
    # get sql_content string by looping through the cellline_string_array
    # we assume a match if cellosaurus synonym name matches exactly at least one word in the extracted cellline_string
    for str in cellline_string_array:
        formatted_str = str.replace('\'', '\'\'')
        sql_content = sql_content + f'\'{formatted_str}\','
    sql = sql_prefix + sql_content.rstrip(',')+');'

    cursor.execute(sql)
    row = cursor.fetchone()
    while row is not None:
        if row[0] not in accession_list:
            accession_list.append(row[0])
        row = cursor.fetchone()
    return accession_list


# scispacy v0.4.0
# def extract_ncitid_cellline(nlp_cellline, cursor, gds_nlp_dict, gdsid, title):
# scispacy v0.3.0
def extract_ncitid_cellline(nlp_cellline, cursor, linker, gds_nlp_dict, gdsid, sentence, type):
    # stores the processed nlp data for current title
    all_link = {}
    doc1 = nlp_cellline(sentence)

    if gdsid in gdsid_reference:
        print(gdsid + f' <{type}> ' + sentence)

    cellline_term_list, gds_nlp_dict = get_celllineList(doc1, gds_nlp_dict, gdsid)
    # all entries in doc1.ents are not labelled as CELL_LINE

    if len(cellline_term_list) == 0:
        gds_nlp_dict[gdsid] = {}
    else:
        cellline_entry = {}
        for index in range(len(cellline_term_list)):
            cellline_string = cellline_term_list[index]
            cellline_string_array = cellline_string.split(' ')
            cellline_accession_list = get_cellline_accession_list(cursor, cellline_string_array)
            if len(cellline_accession_list) > 0:
                for innerindex in range(len(cellline_accession_list)):
                    accession = cellline_accession_list[innerindex]
                    cellline_entry[cellline_string] = {'accession': accession}
            if gdsid in gdsid_reference:
                print('-----cellline_entry dictionary ---------------')
                print(json.dumps(cellline_entry, indent=2, default=str))
        gds_nlp_dict[gdsid] = cellline_entry
    return gds_nlp_dict


def get_celllineList(doc1, gds_nlp_dict, gdsid):
    cellline_term_list = []
    # if empty then store EMPTY umls_id entry
    if len(doc1.ents) == 0:
        gds_nlp_dict[gdsid] = {}
    else:
        for ent in doc1.ents:
            if gdsid in gdsid_reference:
                print(ent)
            # continue to process the text only if the label is DISEASE
            if 'CELL_LINE' in ent.label_:
                if ent.text not in cellline_term_list:
                    cellline_term_list.append(ent.text)
        if gdsid in gdsid_reference:
            print(cellline_term_list)
    return cellline_term_list, gds_nlp_dict


def save_cellline_to_db(cellline_summary_filename, table_name, user, password, port, database):
    # Establishing the connection
    conn = psycopg2.connect(database=database, user=user, password=password, port=port)
    # Creating a cursor object using the cursor() method
    cursor = conn.cursor()
    conn.autocommit = True

    insert = 'INSERT INTO public.gds_' + table_name + '_mapto_cellosaurus(gdsid, ac) VALUES '
    content = ''
    sql = ''

    data = json.load(open(cellline_summary_filename, 'r', encoding='utf-8', errors='ignore'))
    for key_gds, value_gds in data.items():
        m = re.search(r'(?<=GDS)\w+', key_gds)
        gdsid = m.group(0)
        if len(value_gds) > 0:
            for key_cellline, value_cellline in value_gds.items():
                if len(value_cellline) > 0:
                    accession = value_cellline['accession']
                    content = content + f'({gdsid},\'{accession}\'),'
    if len(content) > 0:
        sql = insert + content.rstrip(',') + ' ON CONFLICT DO NOTHING;'
        # print(sql)
        cursor.execute(sql)
        cursor.close()


def union_title_description_table_cellline(user, password, port, database):
    # Establishing the connection
    conn = psycopg2.connect(database=database, user=user, password=password, port=port)
    # Creating a cursor object using the cursor() method
    cursor = conn.cursor()
    conn.autocommit = True

    gds2cellline_insert = 'INSERT INTO public.gds_titledescription_mapto_cellosaurus(gdsid, ac) VALUES '
    gds2cellline_content = ''
    gds2cellline_sql = ''

    union_sql = 'SELECT * FROM public.gds_description_mapto_cellosaurus UNION SELECT *	FROM ' \
                'public.gds_title_mapto_cellosaurus;'
    cursor.execute(union_sql)

    row = cursor.fetchone()
    while row is not None:
        gds2cellline_content = gds2cellline_content + '('+str(row[0])+',\''+str(row[1])+'\'),'
        row = cursor.fetchone()

    gds2cellline_sql = gds2cellline_insert + gds2cellline_content.rstrip(',') + ' ON CONFLICT DO NOTHING;'
    cursor.execute(gds2cellline_sql)
    cursor.close()
