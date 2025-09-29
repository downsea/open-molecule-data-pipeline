I need to reorg the injestion module, please consider the following: 
1. the ZINC data download link is in `data/ZINC22-downloader-2D-smi.gz.wget`, here's the part of the example:
    ```
    mkdir -pv H04 && wget --user gpcr --password xtal https://files.docking.org/zinc22/2d/H04/H04M500.smi.gz -O H04/H04M500.smi.gz
    mkdir -pv H04 && wget --user gpcr --password xtal https://files.docking.org/zinc22/2d/H04/H04M400.smi.gz -O H04/H04M400.smi.gz
    mkdir -pv H04 && wget --user gpcr --password xtal https://files.docking.org/zinc22/2d/H04/H04M300.smi.gz -O H04/H04M300.smi.gz
    mkdir -pv H04 && wget --user gpcr --password xtal https://files.docking.org/zinc22/2d/H04/H04M200.smi.gz -O H04/H04M200.smi.gz
    ``` 
2. the PubChem data download link is in  `data/Index_of_pubchem_Compound_CURRENT-Full_SDF.html`, here's the part of the example:
    ```
    <h1>Index of /pubchem/Compound/CURRENT-Full/SDF</h1>
    <pre>Name                                    Last modified      Size  <hr><a href="https://ftp.ncbi.nlm.nih.gov/pubchem/Compound/CURRENT-Full/">Parent Directory</a>                                             -   
    <a href="https://ftp.ncbi.nlm.nih.gov/pubchem/Compound/CURRENT-Full/SDF/Compound_000000001_000500000.sdf.gz">Compound_000000001_000500000.sdf.gz</a>     2025-09-25 10:19  323M  
    <a href="https://ftp.ncbi.nlm.nih.gov/pubchem/Compound/CURRENT-Full/SDF/Compound_000000001_000500000.sdf.gz.md5">Compound_000000001_000500000.sdf.gz.md5</a> 2025-09-25 10:19   70   
    <a href="https://ftp.ncbi.nlm.nih.gov/pubchem/Compound/CURRENT-Full/SDF/Compound_000500001_001000000.sdf.gz">Compound_000500001_001000000.sdf.gz</a>     2025-09-23 17:51  282M  
    <a href="https://ftp.ncbi.nlm.nih.gov/pubchem/Compound/CURRENT-Full/SDF/Compound_000500001_001000000.sdf.gz.md5">Compound_000500001_001000000.sdf.gz.md5</a> 2025-09-23 17:51   70   
    ```
    I further processed the html file to extract the download link in `data/pubchem_sdf_link.txt`
    ```
    https://ftp.ncbi.nlm.nih.gov/pubchem/Compound/CURRENT-Full/SDF/Compound_000000001_000500000.sdf.gz
    https://ftp.ncbi.nlm.nih.gov/pubchem/Compound/CURRENT-Full/SDF/Compound_000500001_001000000.sdf.gz
    https://ftp.ncbi.nlm.nih.gov/pubchem/Compound/CURRENT-Full/SDF/Compound_001000001_001500000.sdf.gz
    https://ftp.ncbi.nlm.nih.gov/pubchem/Compound/CURRENT-Full/SDF/Compound_001500001_002000000.sdf.gz
    ```
3.  the chEMBL sdf download link is saved in `data/chEMBL_sdf_link.txt`:
    ```
    https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/latest/chembl_36.sdf.gz
    ```
4. using `aria2c` as the backend for all the download process, and add multi-threading , download resume, file check support.
5. in the injestion for ZINC, PubChem, chEMBL, first parse all the download link file as describe before, also extract the user and passwd if required. 