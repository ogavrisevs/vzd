import csv
import sqlite3

table_dict = {}
table_dict["aw_ciems"]= "KODS,TIPS_CD,NOSAUKUMS,VKUR_CD,VKUR_TIPS,APSTIPR,APST_PAK,STATUSS,SORT_NOS,DAT_SAK,DAT_MOD,DAT_BEIG,ATRIB,STD"
table_dict["aw_dziv"]= "KODS,TIPS_CD,STATUSS,APSTIPR,APST_PAK,VKUR_CD,VKUR_TIPS,NOSAUKUMS,SORT_NOS,ATRIB,DAT_SAK,DAT_MOD,DAT_BEIG,STD "
table_dict["aw_eka"]= "KODS,TIPS_CD,STATUSS,APSTIPR,APST_PAK,VKUR_CD,VKUR_TIPS,NOSAUKUMS,SORT_NOS,ATRIB,PNOD_CD,DAT_SAK,DAT_MOD,DAT_BEIG,FOR_BUILD,PLAN_ADR ,STD,KOORD_X,KOORD_Y,DD_N,DD_E"
table_dict["aw_iela"]= "KODS,TIPS_CD,NOSAUKUMS,VKUR_CD,VKUR_TIPS,APSTIPR,APST_PAK,STATUSS,SORT_NOS,DAT_SAK,DAT_MOD,DAT_BEIG,ATRIB,STD"
table_dict["aw_novads"]= "KODS,TIPS_CD,NOSAUKUMS,VKUR_CD,VKUR_TIPS,APSTIPR,APST_PAK,STATUSS,SORT_NOS,DAT_SAK,DAT_MOD,DAT_BEIG,ATRIB,STD"
table_dict["aw_pagasts"]= "KODS,TIPS_CD,NOSAUKUMS,VKUR_CD,VKUR_TIPS,APSTIPR,APST_PAK,STATUSS,SORT_NOS,DAT_SAK,DAT_MOD,DAT_BEIG,ATRIB,STD"
table_dict["aw_pilseta"]= "KODS,TIPS_CD,NOSAUKUMS,VKUR_CD,VKUR_TIPS,APSTIPR,APST_PAK,STATUSS,SORT_NOS,DAT_SAK,DAT_MOD,DAT_BEIG,ATRIB,STD"
table_dict["aw_ppils"]= "KODS,PPILS"
table_dict["aw_rajons"]= "KODS,TIPS_CD,NOSAUKUMS,VKUR_CD,VKUR_TIPS,APSTIPR,APST_PAK,STATUSS,SORT_NOS,DAT_SAK,DAT_MOD,DAT_BEIG,ATRIB"
table_dict["aw_vietu_centroidi"]= "KODS,TIPS_CD,NOSAUKUMS,VKUR_CD,VKUR_TIPS,STD,KOORD_X,KOORD_Y,DD_N,DD_E"

def load_file(file_name: str, db_name : str, skip_first : bool = True ):
    """
        Will read csv file line by line, clean up and insert to sqlite db
    """

    print(f"Loading file : {file_name}")
    file_name = f"./aw_csv/{file_name}"

    with open(file_name, 'r', encoding='utf-8-sig') as csvfile:
        if skip_first:
            next(csvfile)

        datareader = csv.reader(csvfile, delimiter=";",)
        for row in datareader:
            cels = []
            for cel  in row:
                cel_ser = cel.strip("#")
                cel_ser = cel_ser.replace('"','\'')
                cels.append("\"" + cel_ser.strip("#") + "\"")
            values = ",".join(cels)
            cursor.execute(f"INSERT INTO {db_name} ({table_dict[db_name]}) VALUES ({values})");

conn = sqlite3.connect('vzd.db')
print (f"Sqlite3 version {sqlite3.sqlite_version}")
cursor = conn.cursor()
for id_, name, file in conn.execute('PRAGMA database_list'):
    print (f"Connected to name : {name}, file_name : {file} ")

load_file("AW_CIEMS.CSV", "aw_ciems")
load_file("AW_DZIV.CSV", "aw_dziv")
load_file("AW_EKA.CSV", "aw_eka")
load_file("AW_IELA.CSV", "aw_iela")
load_file("AW_NOVADS.CSV", "aw_novads")
load_file("AW_PAGASTS.CSV", "aw_pagasts")
load_file("AW_PILSETA.CSV", "aw_pilseta")
load_file("AW_PPILS.CSV", "aw_ppils")
load_file("AW_RAJONS.CSV", "aw_rajons")
load_file("AW_VIETU_CENTROIDI.CSV", "aw_vietu_centroidi")

conn.commit()
conn.close()