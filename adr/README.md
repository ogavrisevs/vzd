Intro 
======

This python script will lodad VZD adr. to sql db

Run 
=======

Download arh. : 

    wget https://data.gov.lv/dati/dataset/0c5e1a3b-0097-45a9-afa9-7f7262f3f623/resource/1d3cbdf2-ee7d-4743-90c7-97d38824d0bf/download/aw_csv.zip
    unzip aw_csv.zip -d aw_csv

Create db :

    sqlite3 vzd.db < create-tables.sql

Run python: 

    python3 main.py 

Merge into one table 
--------------------

Create table :

    sqlite3 vzd.db < create-merge.sql

Run python: 

    python3 merge.py 

Export table as csv
-------------------

    sqlite3 vzd.db
    .headers on
    .mode csv
    .output adr.csv
    SELECT * FROM aw_merge where statuss == 'EKS';
    .quit

Query sample 
---------------

Search house  

    select ae.STD, ai.STD, ac.STD, ap.STD, an.STD
      from aw_eka ae, aw_iela ai, aw_ciems ac, aw_pagasts ap, aw_novads an
    where 
        ae.VKUR_CD = ai.KODS and ae.VKUR_TIPS = ai.TIPS_CD and ai.STATUSS == 'EKS' and ai.APST_PAK  != 251
        and ai.VKUR_CD = ac.KODS and ai.VKUR_TIPS = ac.TIPS_CD and ac.STATUSS == 'EKS' and ac.APST_PAK  != 251
        and ac.VKUR_CD = ap.KODS and ac.VKUR_TIPS = ap.TIPS_CD and ap.STATUSS == 'EKS' and ap.APST_PAK  != 251
        and ap.VKUR_CD = an.KODS and ap.VKUR_TIPS = an.TIPS_CD and an.STATUSS == 'EKS' and an.APST_PAK  != 251

or apartment 

    select ad.STD, ae.STD, ai.STD, ap.STD
        from aw_dziv ad, aw_eka ae, aw_iela ai, aw_pilseta ap
    where 
            ad.VKUR_CD = ae.KODS and ad.VKUR_TIPS = ae.TIPS_CD and ae.STATUSS == 'EKS' and ae.APST_PAK != 251
        and ae.VKUR_CD = ai.KODS and ae.VKUR_TIPS = ai.TIPS_CD and ai.STATUSS == 'EKS' and ai.APST_PAK != 251         
        and ai.VKUR_CD = ap.KODS and ai.VKUR_TIPS = ap.TIPS_CD and ap.STATUSS == 'EKS' and ap.APST_PAK != 251

Adresācijas objektu tipu atšifrējums (TIPS_CD un VKUR_TIPS)
-----------------------------------------------------------

    101 Latvijas Republika
    102 Rajons
    104 Pilsēta
    105 Pagasts
    106 Ciems/mazciems
    107 Iela
    108 Ēka, apbūvei paredzēta zemes vienība
    109 Telpu grupa
    113 Novads

Adresācijas objektu apstiprinājuma pakāpju kodu atšifrējums (APST_PAK)
----------------------------------------------------------------------

    251 Kļūdains apstiprinājums
    252 Oficiāls apstiprinājums
    253 Daļējs apstiprinājums
    254 Citu reģistru apstiprinājums
    
Links
------

 - https://data.gov.lv/dati/lv/dataset/valsts-adresu-registra-informacijas-sistemas-atvertie-dati
