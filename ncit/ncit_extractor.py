import os


def extract_thesaurus_summary(directory, filename, output_filename, error_filename):
    printline = False
    generatedFile_directory = directory + '\\generatedFile\\'
    print(f'generatedFile_directory={generatedFile_directory}')
    data_directory = directory + '\\data\\'
    print(f'data_directory={data_directory}')
    if not os.path.exists(generatedFile_directory):
        os.makedirs(generatedFile_directory)
    if not os.path.exists(data_directory):
        os.makedirs(data_directory)

    outputfile = open(output_filename, 'w+', encoding='utf-8')
    errorfile = open(error_filename, 'w+', encoding='utf-8')
    with open(filename, encoding='utf-8') as reader:
        # Further file processing goes here
        # Using readlines()
        lines = reader.readlines()
        count = 1
        ncitid = ''
        ncitid_found = False
        preferredname_found = False
        synonym_found = False
        retired = False
        taxonomy = False
        bodypart = True
        string_to_write = ''
        isbodypartline = False
        ncimid_found = False

        # Strips the newline character
        for line in lines:
            thisline = line.strip()
            if '<owl:Axiom>' in thisline:
                if printline is True:
                    if count > 0 and retired is False and taxonomy is False and bodypart is False:
                        outputfile.write(string_to_write)
                        outputfile.write('<DONE>' + '\n')
                        if ncitid_found is False or preferredname_found is False or synonym_found is False:
                            errorfile.write(ncitid + '\n')
                    count = count + 1
                printline = False
                string_to_write = ''
            if '<P106>Finding</P106>' in thisline or '<P106>Disease or Syndrome</P106>' in thisline\
                    or '<P106>Sign or Symptom</P106>' in thisline or '<P106>Neoplastic Process</P106>' in thisline\
                    or '<P106>Congenital Abnormality</P106>' in thisline or '<P106>Cell or Molecular Dysfunction</P106>' in thisline\
                    or '<P106>Mental or Behavioral Dysfunction</P106>' in thisline or '<P106>Pathologic Function</P106>' in thisline\
                    or '<P106>Injury or Poisoning</P106>' in thisline or '<P106>Anatomical Abnormality</P106>' in thisline\
                    or '<P106>Physiologic Function</P106>' in thisline or '<P106>Organ or Tissue Function</P106>' in thisline\
                    or '<P106>Acquired Abnormality</P106>' in thisline or '<P106>Environmental Effect of Humans</P106>' in thisline:
                bodypart = False
            if printline and count > 0:
                if len(thisline) > 0 and ('<NHC0>' in thisline or '<P108>' in thisline or '<P90>' in thisline
                                          or '<P310>Retired_Concept</P310>' in thisline
                                          or '<owl:onProperty rdf:resource=\"http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#R101\"/>' in thisline
                                          or isbodypartline is True
                                          or '<P106>' in thisline
                                          or '<P208>' in thisline
                                          or '<P207>' in thisline):
                    if isbodypartline is True:
                        string_to_write = string_to_write + thisline + '\n'
                        isbodypartline = False
                    if '<NHC0>' in thisline:
                        ncitid = thisline
                        ncitid_found = True
                    if '<P108>' in thisline:
                        preferredname_found = True
                    if '<P90>' in thisline:
                        thisline = thisline.upper()
                        synonym_found = True
                    if '<P207>' in thisline or '<P208>' in thisline:
                        ncimid_found = True
                    if '<P331>' in thisline:
                        taxonomy = True
                    if '<P310>Retired_Concept</P310>' in thisline:
                        retired = True
                    if '<owl:onProperty rdf:resource=\"http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#R101\"/>' in thisline:
                        isbodypartline = True
                    if '<NHC0>' in thisline or '<P108>' in thisline or '<P90>' in thisline or '<P106>' in thisline\
                            or '<P207>' in thisline or '<P208>' in thisline:
                        string_to_write = string_to_write + thisline + '\n'
            if '<owl:Class rdf:about=\"http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#C' in thisline:
                if count > 0 and len(thisline) > 0:
                    string_to_write = string_to_write + thisline + '\n'
                printline = True
                ncitid = ''
                ncitid_found = False
                preferredname_found = False
                synonym_found = False
                retired = False
                taxonomy = False
                bodypart = True
                ncimid_found = False
                isbodypartline = False
        print("Done file")
    reader.close()
    outputfile.close()
    errorfile.close()


def extract_thesaurus_summary_anatomy(filename, output_filename, error_filename):
    printline = False
    outputfile = open(output_filename, 'w', encoding='utf-8')
    errorfile = open(error_filename, 'w', encoding='utf-8')
    with open(filename, encoding='utf-8') as reader:
        # Further file processing goes here
        # Using readlines()
        lines = reader.readlines()
        count = 1
        ncitid = ''
        ncitid_found = False
        preferredname_found = False
        isbodypart_found = False
        retired = False
        string_to_write = ''

        # Strips the newline character
        for line in lines:
            thisline = line.strip()
            if '<owl:Axiom>' in thisline:
                if printline is True:
                    if count > 0 and retired is False and isbodypart_found is True:
                        outputfile.write(string_to_write)
                        outputfile.write('<DONE>' + '\n')
                        if ncitid_found is False or preferredname_found is False:
                            errorfile.write(ncitid + '\n')
                    count = count + 1
                printline = False
                string_to_write = ''
            if printline and count > 0:
                if len(thisline) > 0 and ('<NHC0>' in thisline or '<P108>' in thisline or '<P207>' in thisline
                                          or '<P208>' in thisline
                                          or '<P310>Retired_Concept</P310>' in thisline
                                          or '<P106>Body Part, Organ, or Organ Component</P106>' in thisline
                                          or '<P106>Body System</P106>' in thisline
                                          or '<P106>Anatomical Structure</P106>' in thisline
                                          or '<P106>Body Space or Junction</P106>' in thisline
                                          or '<P106>Tissue</P106>' in thisline
                                          or '<P106>Body Location or Region</P106>' in thisline
                                          or '<P106>Embryonic Structure</P106>' in thisline
                                          or '<P106>Spatial Concept</P106>' in thisline):
                    if '<NHC0>' in thisline:
                        ncitid = thisline
                        ncitid_found = True
                    if '<P106>Body Part, Organ, or Organ Component</P106>' in thisline or '<P106>Body System</P106>' in thisline\
                            or '<P106>Anatomical Structure</P106>' in thisline or '<P106>Body Space or Junction</P106>' in thisline\
                            or '<P106>Tissue</P106>' in thisline or '<P106>Body Location or Region</P106>' in thisline\
                            or '<P106>Embryonic Structure</P106>' in thisline or '<P106>Spatial Concept</P106>' in thisline:
                        isbodypart_found = True
                    if '<P108>' in thisline:
                        preferredname_found = True
                    if '<P310>Retired_Concept</P310>' in thisline:
                        retired = True
                    if '<NHC0>' in thisline or '<P108>' in thisline or '<P207>' in thisline or '<P208>' in thisline \
                            or '<P90>' in thisline:
                        string_to_write = string_to_write + thisline + '\n'
            if '<owl:Class rdf:about=\"http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#C' in thisline:
                if count > 0 and len(thisline) > 0:
                    string_to_write = string_to_write + thisline + '\n'
                printline = True
                ncitid = ''
                ncitid_found = False
                isbodypart_found = False
                preferredname_found = False
                retired = False
        print("Done file")
    reader.close()
    outputfile.close()
    errorfile.close()


# NCIThesaurus contains entries to NCBI Taxonomy repository (marked with NCBI_Taxon_ID <P331>)
def extract_thesaurus_summary_taxonomy(filename, output_filename, error_filename):
    printline = False
    outputfile = open(output_filename, 'w', encoding='utf-8')
    errorfile = open(error_filename, 'w', encoding='utf-8')
    with open(filename, encoding='utf-8') as reader:
        # Further file processing goes here
        # Using readlines()
        lines = reader.readlines()
        count = 1
        ncitid = ''
        ncitid_found = False
        preferredname_found = False
        istaxonomy_found = False
        synonymn_found = False
        retired = False
        string_to_write = ''

        # Strips the newline character
        for line in lines:
            thisline = line.strip()
            if '<owl:Axiom>' in thisline:
                if printline is True:
                    if count > 0 and retired is False and istaxonomy_found is True:
                        outputfile.write(string_to_write)
                        outputfile.write('<DONE>' + '\n')
                        # print("--------------------------------------------------")
                        if ncitid_found is False or preferredname_found is False or synonymn_found is False:
                            errorfile.write(ncitid + '\n')
                    count = count + 1
                printline = False
                string_to_write = ''
            if printline and count > 0:
                if len(thisline) > 0 and ('<NHC0>' in thisline or '<P108>' in thisline
                                          or '<P310>Retired_Concept</P310>' in thisline
                                          or '<P90>' in thisline
                                          or '<P331>' in thisline):
                    if '<NHC0>' in thisline:
                        ncitid = thisline
                        ncitid_found = True
                    if '<P331>' in thisline:
                        istaxonomy_found = True
                    if '<P108>' in thisline:
                        preferredname_found = True
                    if '<P90>' in thisline:
                        synonymn_found = True
                    if '<P310>Retired_Concept</P310>' in thisline:
                        retired = True
                    if '<NHC0>' in thisline or '<P108>' in thisline or '<P331>' in thisline or '<P90>' in thisline:
                        string_to_write = string_to_write + thisline + '\n'
            if '<owl:Class rdf:about=\"http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#C' in thisline:
                if count > 0 and len(thisline) > 0:
                    string_to_write = string_to_write + thisline + '\n'
                printline = True
                ncitid = ''
                ncitid_found = False
                istaxonomy_found = False
                preferredname_found = False
                retired = False
        print("Done file")
    reader.close()
    outputfile.close()
    errorfile.close()
