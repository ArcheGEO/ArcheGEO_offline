# obtain the location of relationship file (MRREL.RRF) and nciCuiMap file (nci_code_cui_map_202008.dat)
def get_NCIM_File(filepath, configuration_file):
    config_ncim_tag = 'NCIMetaThesaurus'
    thesaurus_tag = 'Relationship'
    nciCuiMap_tag = 'NCI2CUI'
    relationship_filepath = ''
    nciCuiMap_filepath = ''
    f = open(filepath + configuration_file, 'r')
    found_relationship_file = False
    found_nciCuiMap_file = False
    for x in f:
        split_arr = x.split('|')
        if split_arr[0] == config_ncim_tag:
            if split_arr[1] == thesaurus_tag:
                # print(f'{split_arr[2]}')
                relationship_filepath = split_arr[2].rstrip()
                found_relationship_file = True
            if split_arr[1] == nciCuiMap_tag:
                nciCuiMap_filepath = split_arr[2].rstrip()
                found_nciCuiMap_file = True
            if found_relationship_file and found_nciCuiMap_file:
                f.close()
                return relationship_filepath, nciCuiMap_filepath
    f.close()
    return relationship_filepath, nciCuiMap_filepath


def get_NCIT_File(filepath, configuration_file):
    config_ncit_tag = 'NCIThesaurus'
    owl_tag = 'OWL'
    out_summary_tag = 'Output_Summary'
    err_summary_tag = 'Error_Summary'
    out_anatomy_tag = 'Output_Anatomy'
    err_anatomy_tag = 'Error_Anatomy'
    out_taxonomy_tag = 'Output_Taxonomy'
    err_taxonomy_tag = 'Error_Taxonomy'
    owl_filepath = ''
    out_summary_filepath = ''
    err_summary_filepath = ''
    out_anatomy_filepath = ''
    err_anatomy_filepath = ''
    out_taxonomy_filepath = ''
    err_taxonomy_filepath = ''
    f = open(filepath + configuration_file, 'r')
    found_owl_file = False
    found_out_summary_file = False
    found_err_summary_file = False
    found_out_anatomy_file = False
    found_err_anatomy_file = False
    found_out_taxonomy_file = False
    found_err_taxonomy_file = False
    for x in f:
        split_arr = x.split('|')
        if split_arr[0] == config_ncit_tag:
            if split_arr[1] == owl_tag:
                # print(f'{split_arr[2]}')
                owl_filepath = split_arr[2].rstrip()
                found_owl_file = True
            if split_arr[1] == out_summary_tag:
                # print(f'{split_arr[2]}')
                out_summary_filepath = split_arr[2].rstrip()
                found_out_summary_file = True
            if split_arr[1] == err_summary_tag:
                # print(f'{split_arr[2]}')
                err_summary_filepath = split_arr[2].rstrip()
                found_err_summary_file = True
            if split_arr[1] == out_anatomy_tag:
                # print(f'{split_arr[2]}')
                out_anatomy_filepath = split_arr[2].rstrip()
                found_out_anatomy_file = True
            if split_arr[1] == err_anatomy_tag:
                # print(f'{split_arr[2]}')
                err_anatomy_filepath = split_arr[2].rstrip()
                found_err_anatomy_file = True
            if split_arr[1] == out_taxonomy_tag:
                # print(f'{split_arr[2]}')
                out_taxonomy_filepath = split_arr[2].rstrip()
                found_out_taxonomy_file = True
            if split_arr[1] == err_taxonomy_tag:
                # print(f'{split_arr[2]}')
                err_taxonomy_filepath = split_arr[2].rstrip()
                found_err_taxonomy_file = True
            if found_owl_file and found_out_summary_file and found_err_summary_file and found_out_anatomy_file \
                    and found_err_anatomy_file and found_out_taxonomy_file and found_err_taxonomy_file:
                f.close()
                return (owl_filepath, out_summary_filepath, err_summary_filepath, out_anatomy_filepath,
                        err_anatomy_filepath, out_taxonomy_filepath, err_taxonomy_filepath)
    f.close()
    return (owl_filepath, out_summary_filepath, err_summary_filepath, out_anatomy_filepath,
            err_anatomy_filepath, out_taxonomy_filepath, err_taxonomy_filepath)


def get_Cellosaurus_File(filepath, configuration_file):
    config_cellosaurus_tag = 'Cellosaurus'
    input_tag = 'Input'
    output_tag = 'Output'
    input_filepath = ''
    output_filepath = ''
    f = open(filepath + configuration_file, 'r')
    found_input_file = False
    found_output_file = False
    for x in f:
        split_arr = x.split('|')
        if split_arr[0] == config_cellosaurus_tag:
            if split_arr[1] == input_tag:
                # print(f'{split_arr[2]}')
                input_filepath = split_arr[2].rstrip()
                found_input_file = True
            if split_arr[1] == output_tag:
                output_filepath = split_arr[2].rstrip()
                found_output_file = True
            if found_input_file and found_output_file:
                f.close()
                return input_filepath, output_filepath
    f.close()
    return input_filepath, output_filepath


def get_Spider_File(filepath, configuration_file):
    config_spider_tag = 'Spider'
    output_tag = 'Output'
    output_filepath = ''
    f = open(filepath + configuration_file, 'r')
    found_output_file = False
    for x in f:
        split_arr = x.split('|')
        if split_arr[0] == config_spider_tag:
            if split_arr[1] == output_tag:
                # print(f'{split_arr[2]}')
                output_filepath = split_arr[2].rstrip()
                found_output_file = True
            if found_output_file:
                f.close()
                return output_filepath
    f.close()
    return output_filepath


def get_GDS_File(filepath, configuration_file):
    config_gds_tag = 'GDS'
    gds_tag = 'GDS'
    ftp_tag = 'FTP'
    summary_tag = 'Summary'
    titleUMLS_tag = 'Output_Title_UMLS'
    descriptionUMLS_tag = 'Output_Description_UMLS'
    titleCellLine_tag = 'Output_Title_Cellline'
    descriptionCellLine_tag = 'Output_Description_Cellline'
    gds_filepath = ''
    ftp_filepath = ''
    summary_filepath = ''
    titleUMLS_filepath = ''
    descriptionUMLS_filepath = ''
    titleCellLine_filepath = ''
    descriptionCellLine_filepath = ''
    f = open(filepath + configuration_file, 'r')
    found_gds_file = False
    found_ftp_file = False
    found_summary_file = False
    found_titleUMLS_file = False
    found_descriptionUMLS_file = False
    found_titleCellLine_file = False
    found_descriptionCellLine_file = False
    for x in f:
        split_arr = x.split('|')
        if split_arr[0] == config_gds_tag:
            if split_arr[1] == gds_tag:
                # print(f'{split_arr[2]}')
                gds_filepath = split_arr[2].rstrip()
                found_gds_file = True
            if split_arr[1] == ftp_tag:
                # print(f'{split_arr[2]}')
                ftp_filepath = split_arr[2].rstrip()
                found_ftp_file = True
            if split_arr[1] == summary_tag:
                # print(f'{split_arr[2]}')
                summary_filepath = split_arr[2].rstrip()
                found_summary_file = True
            if split_arr[1] == titleUMLS_tag:
                # print(f'{split_arr[2]}')
                titleUMLS_filepath = split_arr[2].rstrip()
                found_titleUMLS_file = True
            if split_arr[1] == descriptionUMLS_tag:
                # print(f'{split_arr[2]}')
                descriptionUMLS_filepath = split_arr[2].rstrip()
                found_descriptionUMLS_file = True
            if split_arr[1] == titleCellLine_tag:
                # print(f'{split_arr[2]}')
                titleCellLine_filepath = split_arr[2].rstrip()
                found_titleCellLine_file = True
            if split_arr[1] == descriptionCellLine_tag:
                # print(f'{split_arr[2]}')
                descriptionCellLine_filepath = split_arr[2].rstrip()
                found_descriptionCellLine_file = True
            if found_gds_file and found_ftp_file and found_summary_file and found_titleUMLS_file and \
                    found_descriptionUMLS_file and found_titleCellLine_file and found_descriptionCellLine_file:
                f.close()
                return gds_filepath, ftp_filepath, summary_filepath, titleUMLS_filepath, descriptionUMLS_filepath, \
                       titleCellLine_filepath, descriptionCellLine_filepath
    f.close()
    return gds_filepath, ftp_filepath, summary_filepath, titleUMLS_filepath, descriptionUMLS_filepath, \
           titleCellLine_filepath, descriptionCellLine_filepath
