import gds.nlp.nlp_helper as scispacy_helper
from scispacy.linking import EntityLinker
import json
import re
import psycopg2
from multiprocessing import Process
from gds.nlp.Authentication import *

# gdsid_reference = ['GDS4511', 'GDS4544']
gdsid_reference = []


def create_and_append_entry_currdiseasedict(curr_disease, all_link, index):
    curr_disease_entry = {'name': all_link[index]['name']}
    curr_disease[all_link[index]['umls_id']] = curr_disease_entry
    return curr_disease


def get_diseaseList(doc1, gds_nlp_dict, gdsid):
    disease_term_list = []
    disease_details_list = []

    # if empty then store EMPTY umls_id entry
    if len(doc1.ents) == 0:
        gds_nlp_dict[gdsid] = {}
    else:
        for ent in doc1.ents:
            if gdsid in gdsid_reference:
                print(ent)
            # continue to process the text only if the label is DISEASE
            if 'DISEASE' in ent.label_:
                if ent.text not in disease_term_list:
                    disease_term_list.append(ent.text)
                    disease_details_list.append(ent._.kb_ents)
        if gdsid in gdsid_reference:
            print(disease_term_list)
    return disease_term_list, disease_details_list, gds_nlp_dict


# scispacy v0.4.0
# def extract_umls_link_disease(nlp_disease, gds_nlp_dict, gdsid, title):
# scispacy v0.3.0
def extract_umls_link_disease(nlp_disease, linker, gds_nlp_dict, gdsid, sentence, structure_type):
    # stores the processed nlp data for current title
    all_link = {}
    doc1 = nlp_disease(sentence)

    if gdsid in gdsid_reference:
        print(gdsid + f' <{structure_type}> ' + sentence)

    disease_term_list, disease_details_list, gds_nlp_dict = get_diseaseList(doc1, gds_nlp_dict, gdsid)
    # all entries in doc1.ents are not labelled as DISEASE
    if len(disease_term_list) == 0:
        gds_nlp_dict[gdsid] = {}
    else:
        disease_entry = {}
        for index in range(len(disease_term_list)):
            disease = disease_term_list[index]
            disease_entry[disease] = {}
            # get the umls data for the text
            entity_counter = 0
            uppercase_name_list = []
            disease_details = disease_details_list[index]
            for umls_ent in disease_details:
                umls_entity = linker.kb.cui_to_entity[umls_ent[0]]
                curr_link = {'score': umls_ent[1], 'name': umls_entity.canonical_name, 'umls_id': umls_ent[0],
                             'disease': disease}
                all_link[entity_counter] = curr_link
                uppercase_name_list.append(str(umls_entity.canonical_name).upper())
                entity_counter += 1
            if len(all_link) > 0:
                # find max 'score' in all_link
                max_val = all_link[max(all_link, key=lambda k: all_link[k]['score'])]['score']
                entity_found = False
                entity_text = disease.upper()
                # check if ent.text is found in all_link dictionary
                if gdsid in gdsid_reference:
                    print('ent.text=' + entity_text)
                    print(*uppercase_name_list, sep='\n')
                if entity_text in uppercase_name_list:
                    entity_index = uppercase_name_list.index(entity_text)
                    if gdsid in gdsid_reference:
                        print('entity_index = ' + str(entity_index))
                    if all_link[entity_index]['score'] == max_val:
                        entity_found = True
                else:
                    entity_index = -1
                # print('entity_found = ' + str(entity_found))
                # if entity_found, we just retain the entity. Else, we take all entries with max_val
                curr_disease = {}
                related_disease_counter = 0
                if entity_found:
                    curr_disease = create_and_append_entry_currdiseasedict(curr_disease, all_link, entity_index)
                    disease_entry[disease] = curr_disease
                    gds_nlp_dict[gdsid] = disease_entry
                else:
                    all_link_index = 0
                    for outerkey, outervalue in all_link.items():
                        for key, value in outervalue.items():
                            if key == 'score' and value == max_val:
                                curr_disease = create_and_append_entry_currdiseasedict(curr_disease, all_link,
                                                                                       all_link_index)
                                if gdsid in gdsid_reference:
                                    print('-----curr_disease_entry dictionary ---------------')
                                    print(json.dumps(curr_disease, indent=2, default=str))
                                related_disease_counter += 1
                        all_link_index += 1
                    disease_entry[disease] = curr_disease
                    if gdsid in gdsid_reference:
                        print('-----curr_disease dictionary ---------------')
                        print(json.dumps(curr_disease, indent=2, default=str))
        gds_nlp_dict[gdsid] = disease_entry
    return gds_nlp_dict


def run_scispacy_disease(data_summary_filename, title_file, description_file):
    # load models
    # for scispacy version 0.3.0
    nlp_disease = scispacy_helper.load_models('en_ner_bc5cdr_md')
    # scispacy v0.4.0
    # linker = nlp_disease.get_pipe('scispacy_linker')
    # scispacy v0.3.0
    linker = EntityLinker(resolve_abbreviations=True, name="umls", threshold=0.75, max_entities_per_mention=3)
    nlp_disease.add_pipe(linker)

    rfile = open(data_summary_filename, 'r', encoding='utf-8')
    Lines = rfile.readlines()
    count_line = 0
    count_disease = 0

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
            # gds_title_nlp_dict = extract_umls_link_disease(nlp_disease, gds_title_nlp_dict, gdsid, title)
            # scispacy v0.3.0
            gds_title_nlp_dict = extract_umls_link_disease(nlp_disease, linker, gds_title_nlp_dict, gdsid, title,
                                                           'title')
        if count_line % 4 == 2:
            description = line.rstrip('\n')
            # scispacy v0.4.0
            # gds_description_nlp_dict = extract_umls_link_disease(nlp_disease, gds_description_nlp_dict, gdsid, description)
            # scispacy v0.3.0
            gds_description_nlp_dict = extract_umls_link_disease(nlp_disease, linker, gds_description_nlp_dict, gdsid,
                                                                 description, 'description')
        if count_line % 4 == 3:
            count_disease += 1
        count_line += 1
    rfile.close()
    # output_s1 = pprint.pformat(gds_title_nlp_dict)
    # output_s2 = pprint.pformat(gds_description_nlp_dict)

    with open(title_file, 'w', encoding='utf-8') as file1:
        json.dump(gds_title_nlp_dict, file1, indent=4, sort_keys=True)
    with open(description_file, 'w', encoding='utf-8') as file2:
        json.dump(gds_description_nlp_dict, file2, indent=4, sort_keys=True)


def get_bucket_size(num_processes, num_items):
    bucket_size = num_items / num_processes
    last_bucket_size = num_items % num_processes
    final_bucket_size_first_few = 0
    final_bucket_size_last = 0
    if last_bucket_size == 0:  # can be evenly divided
        final_bucket_size_first_few = bucket_size
        final_bucket_size_last = bucket_size
    else:
        final_bucket_size_first_few = num_items // num_processes + 1
        final_bucket_size_last = num_items % final_bucket_size_first_few
    return final_bucket_size_first_few, final_bucket_size_last


def get_list(this_list, process_index, num_processes, size_first, size_last):
    curr_list = []
    start_index = int(process_index * size_first)
    if process_index < num_processes - 1:
        end_index = int(start_index + size_first)
    else:
        end_index = int(start_index + size_last)
    curr_list = this_list[start_index:end_index]
    return curr_list


def multiprocess_utr_rest(apikey, filename, table_name, user, password, port, database):
    data = json.load(open(filename, 'r', encoding='utf-8', errors='ignore'))
    gds2umls_gdslist = []
    gds2umls_umlsidlist = []
    gds2umls_umlsnamelist = []
    gds2disease_gdslist = []
    gds2disease_diseaselist = []
    for key_gds, value_gds in data.items():
        if len(value_gds) > 0:
            m = re.search(r'(?<=GDS)\w+', key_gds)
            gdsid = m.group(0)
            for key_name, value_name in value_gds.items():
                disease_name = key_name
                if len(value_name) > 0:
                    for key_umls, value_umls in value_name.items():
                        gds2umls_gdslist.append(gdsid)
                        gds2umls_umlsidlist.append(key_umls)
                        gds2umls_umlsnamelist.append(value_umls['name'])
                else:
                    gds2disease_gdslist.append(gdsid)
                    gds2disease_diseaselist.append(disease_name)
    num_processes = 8
    bucket_size_first_few, bucket_size_last = get_bucket_size(num_processes, len(gds2umls_gdslist))
    print('gds2disease_gdslist size=')
    print(len(gds2disease_gdslist))
    if len(gds2disease_gdslist) > 0:
        save_gds_disease_table(gds2disease_gdslist, gds2disease_diseaselist, table_name, user, password, port, database)

    if bucket_size_first_few > 0:
        processes = []
        for index in range(num_processes):
            curr_gdsid_list = get_list(gds2umls_gdslist, index, num_processes, bucket_size_first_few, bucket_size_last)
            curr_umlsid_list = get_list(gds2umls_umlsidlist, index, num_processes, bucket_size_first_few,
                                        bucket_size_last)
            curr_umlsname_list = get_list(gds2umls_umlsnamelist, index, num_processes, bucket_size_first_few,
                                          bucket_size_last)
            process = Process(target=run_utr_rest, args=(apikey, curr_gdsid_list, curr_umlsid_list, curr_umlsname_list,
                                                         index, table_name, user, password, port, database))
            processes.append(process)
        for index in range(len(processes)):
            p = processes[index]
            print('process {} started'.format(index))
            p.start()
        for index in range(len(processes)):
            p = processes[index]
            print('process {} joined'.format(index))
            p.join()


def save_gds_disease_table(gdsid_list, disease_list, table_name, user, password, port, database):
    # Establishing the connection
    conn = psycopg2.connect(database=database, user=user, password=password, port=port)
    # Creating a cursor object using the cursor() method
    cursor = conn.cursor()
    conn.autocommit = True

    insert = 'INSERT INTO public.gds_' + table_name + '_mapto_disease(gdsid, disease) VALUES '
    content = ''
    sql = ''

    for index in range(len(gdsid_list)):
        gdsid = gdsid_list[index]
        disease = disease_list[index].replace('\'', '&APOS;')
        content = content + '(\'' + gdsid + '\',\'' + disease + '\'),'
    sql = insert + content.rstrip(',') + ' ON CONFLICT DO NOTHING;'
    # print(content)
    # print('%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%')
    cursor.execute(sql)
    cursor.close()
    conn.close


def find_nci_link(apikey, umls_id):
    # get a ticket granting ticket (tgt) for the session
    AuthClient = Authentication(apikey)
    tgt = AuthClient.gettgt()
    nci_link = []

    uri = 'https://uts-ws.nlm.nih.gov'
    content_endpoint = '/rest/content/current/CUI/' + umls_id + '/atoms?sabs=NCI'
    pageNumber = 1
    st = AuthClient.getst(tgt)

    query = {'ticket': st, 'pageNumber': pageNumber}
    r = requests.get(uri + content_endpoint, params=query)
    r.encoding = 'utf-8'
    if r.ok:
        items = json.loads(r.text)
        pageCount = items['pageCount']
        for result in items['result']:
            try:
                code = result['code']
                m = re.search(r'(?<=NCI/)\w+', code)
                if m.group(0) not in nci_link:
                    nci_link.append(m.group(0))
            except:
                NameError
        pageNumber += 1
    return nci_link


def run_utr_rest(apikey, curr_gdsid_list, curr_umlsid_list, curr_umlsname_list, process_index, table_name,
                 user, password, port, database):
    # Establishing the connection
    conn = psycopg2.connect(database=database, user=user, password=password, port=port)
    # Creating a cursor object using the cursor() method
    cursor = conn.cursor()
    conn.autocommit = True
    print(process_index)

    count = 0
    nci_link = []

    gds2umls_insert = 'INSERT INTO public.gds_' + table_name + '_mapto_umls(gdsid, umlsid, umlsname) VALUES '
    gds2umls_content = ''
    gds2umls_sql = ''

    umls2ncit_insert = 'INSERT INTO public.umls_mapto_ncit(umlsid, ncitid) VALUES '
    umls2ncit_content = ''
    umls2ncit_sql = ''

    for index in range(len(curr_gdsid_list)):
        gdsid = curr_gdsid_list[index]
        umlsid = curr_umlsid_list[index]
        umlsname = curr_umlsname_list[index].replace('\'', '&APOS;')
        gds2umls_content = gds2umls_content + '(\'' + str(gdsid) + '\',\'' + str(umlsid) + '\',\'' + str(
            umlsname).upper() + '\'),'
        nci_link = find_nci_link(apikey, umlsid)
        if len(nci_link) > 0:
            for n in nci_link:
                umls2ncit_content = umls2ncit_content + '(\'' + str(umlsid) + '\',\'' + str(n) + '\'),'
    gds2umls_sql = gds2umls_insert + gds2umls_content.rstrip(',') + ' ON CONFLICT DO NOTHING;'
    cursor.execute(gds2umls_sql)
    umls2ncit_sql = umls2ncit_insert + umls2ncit_content.rstrip(',') + ' ON CONFLICT DO NOTHING;'
    cursor.execute(umls2ncit_sql)
    cursor.close()
    conn.close


def union_title_description_table_disease(user, password, port, database):
    # Establishing the connection
    conn = psycopg2.connect(database=database, user=user, password=password, port=port)
    # Creating a cursor object using the cursor() method
    cursor = conn.cursor()
    conn.autocommit = True

    gds2umls_insert = 'INSERT INTO public.gds_titledescription_mapto_umls(gdsid, umlsid) VALUES '
    gds2umls_content = ''
    gds2umls_sql = ''

    union_sql = 'SELECT * FROM public.gds_description_mapto_umls UNION SELECT *	FROM public.gds_title_mapto_umls;'
    cursor.execute(union_sql)

    row = cursor.fetchone()
    while row is not None:
        gds2umls_content = gds2umls_content + '('+str(row[0])+',\''+str(row[1])+'\'),'
        row = cursor.fetchone()

    gds2umls_sql = gds2umls_insert + gds2umls_content.rstrip(',') + ' ON CONFLICT DO NOTHING;'
    cursor.execute(gds2umls_sql)
    cursor.close()
