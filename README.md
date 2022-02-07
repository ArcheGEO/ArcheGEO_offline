Important: Although PROTEASE is an extension of ArcheGEO, the postgreSQL DB schema of PROTEASE is slightly different from ArcheGEO. Do not run PROTEASE using the postgreSQL DB populated by ArcheGEO_offline and vice versas. Instead, run PROTEASE_offline to populate the postgreSQL DB schema for PROTEASE.
_______________________________________________________________________________________________________________________________________________

ArcheGEO_offline is the offline pipeline of ArcheGEO and is responsible for setting up the PostgreSQL database with data from NCI Thesaurus, NCI Metathesaurus and Cellosaurus. 
It is coded using Python 3.8 (PyCharm Community 2020.2). Note that you should run ArcheGEO_offline first before using ArcheGEO_online.
Note that ArcheGEO_offline requires several large data files in order to run. These files are too large to upload to Github. Please refer to readme.docx for where to obtain the latest versions of these files and also for the architecture framework of ArcheGEO_offline and how to run it. After the large data files are obtained, they should be stored in a folder (named "data") that is created at the same level as the "configuration" folder in the ArcheGEO_offline project.

Current version of large data files used for ArcheGEO_offline:
1) [Cellosaurus.txt](https://www.mediafire.com/file/v21zem5h0o3om9c/cellosaurus.txt/file)
2) [MRREL.RRF](https://www.mediafire.com/file/so8pqal1mz19cjv/MRREL.RRF/file)
3) [nci_code_cui_map_202008.dat](https://www.mediafire.com/file/vo6i93rxrxiqhy5/nci_code_cui_map_202008.dat/file)
4) [Thesaurus.owl](https://www.mediafire.com/file/w3tvmiax72mmbsg/Thesaurus.owl/file)
