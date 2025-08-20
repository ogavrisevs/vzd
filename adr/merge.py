import sqlite3
import datetime

#check_same_thread=False, isolation_level=None, cached_statements=1024
connection = sqlite3.connect('adr.db', isolation_level=None)
seconds_in_day = 24 * 60 * 60
# PRAGMA settings for fast bulk insert
connection.execute("PRAGMA synchronous = OFF;")
connection.execute("PRAGMA journal_mode = MEMORY;")
connection.execute("PRAGMA temp_store = MEMORY;")
connection.execute("PRAGMA cache_size = 100000;")
connection.execute("PRAGMA foreign_keys = OFF;")

# Create helpful indexes (no-op if they already exist)
def ensure_indexes():
    idx_ddl = [
        # Base tables
        ("aw_dziv", "CREATE INDEX IF NOT EXISTS idx_aw_dziv_kods_tips ON aw_dziv(KODS, TIPS_CD)"),
        ("aw_dziv", "CREATE INDEX IF NOT EXISTS idx_aw_dziv_vkur ON aw_dziv(VKUR_CD, VKUR_TIPS)"),
        ("aw_eka", "CREATE INDEX IF NOT EXISTS idx_aw_eka_kods_tips ON aw_eka(KODS, TIPS_CD)"),
        ("aw_eka", "CREATE INDEX IF NOT EXISTS idx_aw_eka_vkur ON aw_eka(VKUR_CD, VKUR_TIPS)"),
        ("aw_iela", "CREATE INDEX IF NOT EXISTS idx_aw_iela_kods_tips ON aw_iela(KODS, TIPS_CD)"),
        ("aw_iela", "CREATE INDEX IF NOT EXISTS idx_aw_iela_vkur ON aw_iela(VKUR_CD, VKUR_TIPS)"),
        ("aw_ciems", "CREATE INDEX IF NOT EXISTS idx_aw_ciems_kods_tips ON aw_ciems(KODS, TIPS_CD)"),
        ("aw_ciems", "CREATE INDEX IF NOT EXISTS idx_aw_ciems_vkur ON aw_ciems(VKUR_CD, VKUR_TIPS)"),
        ("aw_pagasts", "CREATE INDEX IF NOT EXISTS idx_aw_pagasts_kods_tips ON aw_pagasts(KODS, TIPS_CD)"),
        ("aw_pilseta", "CREATE INDEX IF NOT EXISTS idx_aw_pilseta_kods_tips ON aw_pilseta(KODS, TIPS_CD)"),
        ("aw_novads", "CREATE INDEX IF NOT EXISTS idx_aw_novads_kods_tips ON aw_novads(KODS, TIPS_CD)"),
        ("aw_rajons", "CREATE INDEX IF NOT EXISTS idx_aw_rajons_kods_tips ON aw_rajons(KODS, TIPS_CD)"),
    ]
    for _, stmt in idx_ddl:
        try:
            connection.execute(stmt)
        except Exception:
            # If a table is missing, ignore index creation for it
            pass
    connection.commit()

ensure_indexes()

def create_merge_table():
    # Drop aw_merge table if exists
    #connection.execute("DROP TABLE IF EXISTS aw_merge;")
    #connection.commit()
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS aw_merge (
        adr varchar(256),
        kods int,
        tips smallint,
        statuss varchar(3),
        dziv varchar(128), 
        eka varchar(128), 
        iela varchar(128), 
        ciems varchar(128), 
        pilseta varchar(128), 
        pagasts varchar(128), 
        novads varchar(128), 
        rajons varchar(128),
        pasta_kods varchar(7),
        UNIQUE(kods, tips)
    );
    """
    connection.execute(create_table_sql)
    connection.commit()
    print("Dropped and created aw_merge table")
 
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

parent_cache = dict()

# Preload parent tables into memory maps to avoid per-row DB lookups
def build_parent_maps():
    tables = ["aw_eka", "aw_iela", "aw_ciems", "aw_pagasts", "aw_pilseta", "aw_novads", "aw_rajons"]
    maps = {}
    for t in tables:
        try:
            cur = connection.execute(f"SELECT KODS, TIPS_CD, NOSAUKUMS, STATUSS, VKUR_CD, VKUR_TIPS, ATRIB FROM {t}")
            d = {}
            for r in cur:
                d[(r[0], r[1])] = {"kods": r[0], "tips_cd": r[1], "nosaukums": r[2], "status": r[3], "vkur_cd": r[4], "vkur_tips": r[5], "atrib": r[6]}
            maps[t] = d
        except Exception:
            maps[t] = {}
    return maps

PARENT_MAPS = build_parent_maps()

def find_parrent(table_name: str, kods: str, tips_cd: str):
    cache_key = (table_name, kods, tips_cd)
    if cache_key in parent_cache:
        return parent_cache[cache_key]
    # Try memory map first
    m = PARENT_MAPS.get(table_name, {})
    p = m.get((kods, tips_cd))
    if p is not None:
        parent_cache[cache_key] = p
        return p
    # Fallback to DB lookup as a safety net
    try:
        statement = f"SELECT KODS, TIPS_CD, NOSAUKUMS, STATUSS, VKUR_CD, VKUR_TIPS, ATRIB FROM {table_name} where KODS = ? and TIPS_CD = ?;"
        cursor = connection.execute(statement, (kods, tips_cd))
        row = cursor.fetchone()
        if row is None:
            return None
        parrent = {"kods": row[0], "tips_cd": row[1], "nosaukums": row[2], "status": row[3], "vkur_cd": row[4], "vkur_tips": row[5], "atrib": row[6]}
        parent_cache[cache_key] = parrent
        return parrent
    except Exception:
        return None

def get_parrents ( parrents: list, vkur_cd :str, vkur_tips :str ) :
    #print(f"search for vkur_cd: {vkur_cd} , vkur_tips: {vkur_tips}")
    if vkur_cd == 100000000 and vkur_tips == 101:
        parrents.append({  "nosaukums": "LV","kods" : vkur_cd , "tips" : vkur_tips})
        return 

    row = find_parrent(table_name = tips_to_table_name(vkur_tips), kods = vkur_cd, tips_cd = vkur_tips) 
    if row is None:
        return
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

def row_exists(kods :str, tips :str, table_name : str = "aw_merge"):
    data = connection.execute(f"SELECT 1 FROM { table_name } where kods = { kods } and tips = { tips }") 
    rez = data.fetchone()
    if rez is None: 
        return False
    if rez[0] == 1:
        return True
    
    return False

def prepare_standardized_row(adr_rows):
    """Prepare a standardized row with all columns in fixed order"""
    # Initialize all columns with None
    row_data = [None] * 13  # 13 columns total
    selected = None
    
    for adr_row in adr_rows:
        if adr_row['tips'] == 101:
            continue
            
        if "selected" in adr_row and adr_row['selected']:
            selected = adr_row
            
        table_name = tips_to_table_name(adr_row['tips'])
        column_name = table_name.replace('aw_', '')
        
        # Map to standardized column positions
        column_map = {
            'dziv': 4, 'eka': 5, 'iela': 6, 'ciems': 7, 
            'pilseta': 8, 'pagasts': 9, 'novads': 10, 'rajons': 11
        }
        
        if column_name in column_map:
            row_data[column_map[column_name]] = adr_row['nosaukums']
            
        # Handle pasta_kods
        if adr_row['tips'] == 108 and 'pasta_kods' in adr_row:
            row_data[12] = adr_row['pasta_kods']
    
    if selected:
        row_data[0] = selected["adr"]      # adr
        row_data[1] = selected["kods"]     # kods  
        row_data[2] = selected["tips"]     # tips
        row_data[3] = selected["statuss"]  # statuss
        return row_data
    
    return None

def process_table_bulk(table_name: str):
    print(f" Search for : {table_name}")
    statement = f"SELECT KODS, TIPS_CD, NOSAUKUMS, STATUSS, VKUR_CD, VKUR_TIPS, STD , ATRIB FROM { table_name }; "
    cursor = connection.execute(statement) 
    rows = cursor.fetchall() 
    total_count = get_row_count(table_name)
    print (f"Found : { total_count } rows for table : { table_name }")
    
    counter = 0 
    batch_size = 10000
    batch_values = []
    first_time = datetime.datetime.now()
    
    # Prepare the INSERT statement once
    all_columns = ["adr", "kods", "tips", "statuss", "dziv", "eka", "iela", "ciems", "pilseta", "pagasts", "novads", "rajons", "pasta_kods"]
    columns_line = ", ".join(all_columns)
    placeholders = ", ".join(["?" for _ in all_columns])
    insert_statement = f"INSERT INTO aw_merge ({columns_line}) VALUES ({placeholders})"
    
    # Begin transaction for bulk insert
    connection.execute("BEGIN TRANSACTION;")
    try:
        for row in rows:
            counter += 1
            parrents = list()
            table_row = {
                "nosaukums": row[2],
                "kods": row[0],
                "tips": row[1],
                "table": tips_to_table_name(row[1]),
                "adr": row[6],
                "statuss": row[3],
                "pasta_kods": row[7],
                "selected": True
            }
            parrents.append(table_row)
            get_parrents(parrents=parrents, vkur_cd=row[4], vkur_tips=row[5])
            # Prepare standardized row data
            row_data = prepare_standardized_row(parrents)
            if row_data:
                batch_values.append(row_data)
            # Execute batch insert when batch is full
            if len(batch_values) >= batch_size:
                connection.executemany(insert_statement, batch_values)
                batch_values = []
                later_time = datetime.datetime.now()
                difference = later_time - first_time
                min, sec = divmod(difference.days * seconds_in_day + difference.seconds, 60)
                print(f"Processed {counter} / {total_count} rows . . . ( min : {min} , sec : {sec} ) ")
                first_time = datetime.datetime.now()
        # Execute remaining batch
        if batch_values:
            connection.executemany(insert_statement, batch_values)
    finally:
        connection.commit()

print("Start ...")

# Create the merge table first
create_merge_table()
process_table_bulk("aw_dziv")
process_table_bulk("aw_novads")
process_table_bulk("aw_pagasts")
process_table_bulk("aw_ciems")
process_table_bulk("aw_pilseta")
process_table_bulk("aw_iela")
process_table_bulk("aw_eka")

print("Done.")

