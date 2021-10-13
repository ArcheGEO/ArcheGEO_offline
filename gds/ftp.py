import re
import os
import urllib
import gzip
import shutil


def generate_gdslist_ftplist(data_filename, outputfile_gdslist, outputfile_ftplist):
    countline = 0
    # Using readlines()
    rfile = open(data_filename, 'r')
    Lines = rfile.readlines()

    # writing to file
    wfile_gds = open(outputfile_gdslist, 'w')
    wfile_gds.writelines(''+'\n')
    wfile_ftp = open(outputfile_ftplist, 'w')
    wfile_ftp.writelines(''+'\n')

    for line in Lines:
        if '.soft.gz' in line and '_full' not in line:
            countline += 1
            m = re.search('(.+?).soft.gz', line)
            if m:
                wfile_gds.writelines(m.group(1)+'\n')
            wfile_ftp.writelines(line)
    wfile_gds.close()
    wfile_ftp.close()
    rfile.close()

    print('number entries = '+str(countline))

    # write the number of entries now
    rfile_gds = open(outputfile_gdslist, 'r+')
    contents = rfile_gds.read()
    rfile_gds.seek(0, 0)
    rfile_gds.write(str(countline)+contents)
    rfile_gds.close()

    rfile_ftp = open(outputfile_ftplist, 'r+')
    contents = rfile_ftp.read()
    rfile_ftp.seek(0, 0)
    rfile_ftp.writelines(str(countline)+contents)
    rfile_ftp.close()


def download_ftplist(directory, inputfile_gdslist, inputfile_ftplist):
    ftp_directory = directory + '\\ftp\\'
    print(f'ftp_directory={ftp_directory}')

    if not os.path.exists(ftp_directory):
        os.makedirs(ftp_directory)

    ftp_link_prefix = 'https://ftp.ncbi.nlm.nih.gov/geo/datasets/GDS'
    count = 0

    rfile_gds = open(inputfile_gdslist, 'r')
    Lines_gds = rfile_gds.readlines()
    rfile_ftp = open(inputfile_ftplist, 'r')
    Lines_ftp = rfile_ftp.readlines()

    for line in Lines_gds:
        if count > 0:
            #get numerical ID value
            m = re.search('GDS(.+?)\n', line)
            if m:
                numerical_id = int(m.group(1))//1000
                if numerical_id == 0:
                    ftp_link_content = 'nnn/GDS'+m.group(1)+'/soft/'+Lines_ftp[count]
                else:
                    ftp_link_content = str(numerical_id) + 'nnn/GDS' + m.group(1) + '/soft/' + Lines_ftp[count]
                final_ftp_link = ftp_link_prefix + ftp_link_content.rstrip('\n')
                save_link = ftp_directory + Lines_ftp[count].rstrip('\n')
                urllib.request.urlretrieve(final_ftp_link, save_link)
        count += 1
    rfile_gds.close()
    rfile_ftp.close()


def decompress_gzfiles(directory, inputfile_ftplist):
    extract_directory = directory + '\\extractedFtp\\'
    if not os.path.exists(extract_directory):
        os.makedirs(extract_directory)

    rfile_ftp = open(inputfile_ftplist, 'r')
    Lines_ftp = rfile_ftp.readlines()
    count = 0

    for line in Lines_ftp:
        if count > 0:
            with gzip.open(f'{directory}\\ftp\\'+line.rstrip('\n'), 'rb') as f_in:
                with open(f'{directory}\\extractedFtp\\'+line.rstrip('.gz\n'), 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            f_in.close()
            f_out.close()
        count += 1


def extract_data_gzfiles(directory, inputfile_ftplist, data_summary_filename):
    print(f'inputfile_ftplist={inputfile_ftplist}')
    print(f'data_summary_filename={data_summary_filename}')
    rfile_ftp = open(inputfile_ftplist, 'r')
    Lines_ftp = rfile_ftp.readlines()
    wfile_ftp = open(data_summary_filename, 'w', encoding='utf-8')
    count = 0

    for line in Lines_ftp:
        gdsid = ''
        title = ''
        description = ''
        organism = ''
        if count > 0:
            filename = line.rstrip('.gz\n')
            f_in = open(f'{directory}\\extractedFtp\\'+filename, 'r', encoding='utf-8')
            Lines_fin = f_in.readlines()
            for l in Lines_fin:
                if len(gdsid) > 0 and len(title) > 0 and len(description) > 0 and len(organism) > 0:
                    break
                if '^DATASET = GDS' in l:
                    result = re.search('\^DATASET = (.*)\n', l)
                    gdsid = result.group(1)
                if '!dataset_title = ' in l:
                    result = re.search('!dataset_title = (.*)\n', l)
                    title = result.group(1)
                if '!dataset_description = ' in l:
                    result = re.search('!dataset_description = (.*)\n', l)
                    description = result.group(1)
                if '!dataset_platform_organism ' in l:
                    result = re.search('!dataset_platform_organism = (.*)\n', l)
                    organism = result.group(1)
            f_in.close()
            wfile_ftp.write(gdsid + '\n')
            wfile_ftp.write(title + '\n')
            wfile_ftp.write(description + '\n')
            wfile_ftp.write(organism + '\n')
        count += 1
    wfile_ftp.close()
