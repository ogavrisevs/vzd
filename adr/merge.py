import sqlite3
import datetime

#check_same_thread=False, isolation_level=None, cached_statements=1024
connection = sqlite3.connect('vzd.db', isolation_level=None) 
seconds_in_day = 24 * 60 * 60
 
def tips_to_table_name(tips : int):
   if tips == 102:
    return "aw_rajons"
   elif tips == 104 :
    return "aw_pilseta"
   elif tips == 105:
    return "aw_pagasts"
   elif tips == 113:
    return "aw_novads"
   if tips == 106:
      return "aw_ciems"
   elif tips == 107:
    return "aw_iela"
   elif tips == 108:
    return "aw_eka"
   elif tips == 109:
    return "aw_dziv"
   else:
    return "aw_unknown"

def find_parrent( table_name :str, kods :str, tips_cd :str):
  statement = f"SELECT KODS, TIPS_CD, NOSAUKUMS, STATUSS, VKUR_CD, VKUR_TIPS, ATRIB FROM {table_name} where KODS = {kods} and TIPS_CD = {tips_cd}; "
  cursor = connection.execute(statement) 
  row = cursor.fetchone() 
  parrent = { "kods": row[0] , "tips_cd": row[1], "nosaukums": row[2], "status": row[3], "vkur_cd": row[4], "vkur_tips": row[5], "atrib": row[6] }
  return parrent

def get_parrents ( parrents: list, vkur_cd :str, vkur_tips :str ) :
    #print(f"search for vkur_cd: {vkur_cd} , vkur_tips: {vkur_tips}")
    if vkur_cd == 100000000 and vkur_tips == 101:
        parrents.append({  "nosaukums": "LV","kods" : vkur_cd , "tips" : vkur_tips})
        return 

    row = find_parrent(table_name = tips_to_table_name(vkur_tips), kods = vkur_cd, tips_cd = vkur_tips) 
    #print( f"found nosaukums: {row['nosaukums']} ,  kods: {row['kods']} , tips: {row['tips_cd']}")
    table_row = {"nosaukums": row['nosaukums'] , "kods": row['kods'], "tips": row['tips_cd'] , "table": tips_to_table_name(vkur_tips)}
    if row['tips_cd'] == 108:
        table_row['pasta_kods'] = row['atrib']

    parrents.append(table_row)
    get_parrents( parrents = parrents, vkur_cd = row['vkur_cd'], vkur_tips = row['vkur_tips'])
    return
        
def get_row_count(table_name :str):
    cursor = connection.execute(f"SELECT COUNT(*) FROM {table_name}") 
    row_count = cursor.fetchall()[0]
    return row_count[0]

def insert_merged(adr_rows : list):
    columns = []
    values = []
    selected = dict()
    for adr_row in  adr_rows: 
        if adr_row['tips'] == 101:
            continue
        if adr_row['tips'] == 108:
            columns.append("pasta_kods")
            values.append(adr_row["pasta_kods"])
        if "selected" in adr_row.keys() and adr_row['selected'] == True:
            selected = adr_row
            
        table_name = tips_to_table_name(adr_row['tips'])
        columns.append(f"{table_name.replace('aw_', '')}")
        values.append(adr_row['nosaukums'])

    columns.append("adr")
    values.append(selected["adr"])

    columns.append("kods")
    values.append(selected["kods"])

    columns.append("tips")
    values.append(selected["tips"])

    columns.append("statuss")
    values.append(selected["statuss"])

    columns_line = " , ".join(columns)
    values_line = ', '.join(f'"{w}"' for w in values)

    statement = f"INSERT INTO aw_merge ({ columns_line }) VALUES ( { values_line } );"
    connection.execute(statement)

def row_exists(kods :str, tips :str, table_name : str = "aw_merge"):
    data = connection.execute(f"SELECT 1 FROM { table_name } where kods = { kods } and tips = { tips }") 
    rez = data.fetchone()
    if rez is None: 
        return False
    if rez[0] == 1:
        return True
    
    return False

def process_table(table_name :str):
    print(f" Search for : {table_name}")
    statement = f"SELECT KODS, TIPS_CD, NOSAUKUMS, STATUSS, VKUR_CD, VKUR_TIPS, STD , ATRIB FROM { table_name }; "
    cursor = connection.execute(statement) 
    rows = cursor.fetchall() 
    total_count = get_row_count(table_name)
    print (f"Found : { total_count } rows for table : { table_name }")
    counter = 0 
    first_time = datetime.datetime.now()
    for row in rows: 
        counter += 1
        if counter % 1000 == 0:
            later_time = datetime.datetime.now()
            difference = later_time - first_time
            min, sec = divmod(difference.days * seconds_in_day + difference.seconds, 60)
            print(f"Inserted {counter} / {total_count} rows . . . ( min : { min } , sec : { sec } ) ")
            first_time = datetime.datetime.now()

        if row_exists(kods = row[0] , tips = row[1]):
            continue

        parrents = list()
        table_row = {
            "nosaukums": row[2] ,
            "kods": row[0], 
            "tips": row[1],
            "table": tips_to_table_name(row[1]), 
            "adr": row[6], 
            "statuss": row[3], 
            "pasta_kods": row[7],
            "selected" : True 
        }
        parrents.append(table_row)
        get_parrents(parrents = parrents , vkur_cd = row[4], vkur_tips = row[5])  
        insert_merged(adr_rows = parrents)
    print (f"Row Count after proc. : { get_row_count('aw_merge') } ")

print("Start ...")

process_table("aw_dziv")
process_table("aw_eka")
process_table("aw_iela")
process_table("aw_pilseta")
process_table("aw_ciems")
process_table("aw_pagasts")
process_table("aw_novads")

print("Done.")

