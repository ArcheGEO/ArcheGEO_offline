import cellosaurus.string_constants as const


def preprocess_cellosaurus(input_filename, output_filename):
    found_disease = False
    str_list_to_write = []

    with open(input_filename, encoding='utf-8') as reader:
        lines = reader.readlines()
    writer = open(output_filename, 'w', encoding='utf-8')
    for line in lines:
        if line.startswith(const.identifier) or line.startswith(const.accession) or line.startswith(const.synonym) or \
                line.startswith(const.taxonomy) or line.startswith(const.disease) or \
                line.startswith(const.terminate) or line.startswith(const.anatomy_1) or \
                line.startswith(const.anatomy_2):
            str_list_to_write.append(line)
            if line.startswith(const.disease):
                found_disease = True
            if line.startswith(const.terminate):
                if found_disease:
                    writer.writelines(str_list_to_write)
                    found_disease = False
                str_list_to_write = []
    writer.close()
    reader.close()
    print('All done!')

