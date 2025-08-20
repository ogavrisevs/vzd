import os
import sqlite3
import csv

DB_PATH = 'adr.db'
CUR_DIR = './adr/all/cur'
HIS_DIR = './adr/all/his'

# Current ADR (AW_*.CSV) file-to-table mapping
CUR_FILE_TABLE_MAP = {
    'AW_CIEMS.CSV': 'aw_ciems',
    'AW_DZIV.CSV': 'aw_dziv',
    'AW_EKA.CSV': 'aw_eka',
    'AW_IELA.CSV': 'aw_iela',
    'AW_NOVADS.CSV': 'aw_novads',
    'AW_PAGASTS.CSV': 'aw_pagasts',
    'AW_PILSETA.CSV': 'aw_pilseta',
    'AW_PPILS.CSV': 'aw_ppils',
    'AW_RAJONS.CSV': 'aw_rajons',
    'AW_VIETU_CENTROIDI.CSV': 'aw_vietu_centroidi',
}

# History ADR (aw_*_his.csv) file-to-table mapping (supporting alternate naming for rajons)
HIS_FILE_TABLE_MAP = {
    'aw_ciems_his.csv': 'aw_ciems_his',
    'aw_dziv_his.csv': 'aw_dziv_his',
    'aw_eka_his.csv': 'aw_eka_his',
    'aw_iela_his.csv': 'aw_iela_his',
    'aw_novads_his.csv': 'aw_novads_his',
    'aw_pagasts_his.csv': 'aw_pagasts_his',
    'aw_pilseta_his.csv': 'aw_pilseta_his',
    'aw_rajons_his.csv': 'aw_rajons_his',
    'aw_rajons.csv': 'aw_rajons_his',
}


def ensure_tables(conn: sqlite3.Connection):
    """Create required ADR base and history tables if they don't exist (DRY schema)."""
    # Shared column schemas for base tables; _his variants reuse the same columns
    schemas = {
        'aw_ciems': [
            ('KODS','int'), ('TIPS_CD','smallint'), ('NOSAUKUMS','varchar(128)'),
            ('VKUR_CD','int'), ('VKUR_TIPS','smallint'), ('APSTIPR','varchar(1)'),
            ('APST_PAK','smallint'), ('STATUSS','varchar(3)'), ('SORT_NOS','varchar(516)'),
            ('DAT_SAK','varchar(32)'), ('DAT_MOD','varchar(32)'), ('DAT_BEIG','varchar(32)'),
            ('ATRIB','varchar(32)'), ('STD','varchar(256)')
        ],
        'aw_dziv': [
            ('KODS','int'), ('TIPS_CD','smallint'), ('STATUSS','varchar(3)'), ('APSTIPR','varchar(1)'),
            ('APST_PAK','smallint'), ('VKUR_CD','int'), ('VKUR_TIPS','smallint'), ('NOSAUKUMS','varchar(128)'),
            ('SORT_NOS','varchar(516)'), ('ATRIB','varchar(32)'), ('DAT_SAK','varchar(256)'),
            ('DAT_MOD','varchar(256)'), ('DAT_BEIG','varchar(256)'), ('STD','varchar(256)')
        ],
        'aw_eka': [
            ('KODS','int'), ('TIPS_CD','smallint'), ('STATUSS','varchar(3)'), ('APSTIPR','varchar(1)'),
            ('APST_PAK','smallint'), ('VKUR_CD','int'), ('VKUR_TIPS','smallint'), ('NOSAUKUMS','varchar(128)'),
            ('SORT_NOS','varchar(516)'), ('ATRIB','varchar(32)'), ('PNOD_CD','varchar(256)'),
            ('DAT_SAK','varchar(32)'), ('DAT_MOD','varchar(32)'), ('DAT_BEIG','varchar(32)'),
            ('FOR_BUILD','varchar(1)'), ('PLAN_ADR','varchar(10)'), ('STD','varchar(256)'),
            ('KOORD_X','real'), ('KOORD_Y','real'), ('DD_N','real'), ('DD_E','real')
        ],
        'aw_iela': [
            ('KODS','int'), ('TIPS_CD','smallint'), ('NOSAUKUMS','varchar(128)'), ('VKUR_CD','int'),
            ('VKUR_TIPS','smallint'), ('APSTIPR','varchar(1)'), ('APST_PAK','smallint'), ('STATUSS','varchar(3)'),
            ('SORT_NOS','varchar(516)'), ('DAT_SAK','varchar(32)'), ('DAT_MOD','varchar(32)'),
            ('DAT_BEIG','varchar(32)'), ('ATRIB','varchar(32)'), ('STD','varchar(256)')
        ],
        'aw_novads': [
            ('KODS','int'), ('TIPS_CD','smallint'), ('NOSAUKUMS','varchar(128)'), ('VKUR_CD','int'),
            ('VKUR_TIPS','smallint'), ('APSTIPR','varchar(1)'), ('APST_PAK','smallint'), ('STATUSS','varchar(3)'),
            ('SORT_NOS','varchar(516)'), ('DAT_SAK','varchar(32)'), ('DAT_MOD','varchar(32)'),
            ('DAT_BEIG','varchar(32)'), ('ATRIB','varchar(32)'), ('STD','varchar(256)')
        ],
        'aw_pagasts': [
            ('KODS','int'), ('TIPS_CD','smallint'), ('NOSAUKUMS','varchar(128)'), ('VKUR_CD','int'),
            ('VKUR_TIPS','smallint'), ('APSTIPR','varchar(1)'), ('APST_PAK','smallint'), ('STATUSS','varchar(3)'),
            ('SORT_NOS','varchar(516)'), ('DAT_SAK','varchar(32)'), ('DAT_MOD','varchar(32)'),
            ('DAT_BEIG','varchar(32)'), ('ATRIB','varchar(32)'), ('STD','varchar(256)')
        ],
        'aw_pilseta': [
            ('KODS','int'), ('TIPS_CD','smallint'), ('NOSAUKUMS','varchar(128)'), ('VKUR_CD','int'),
            ('VKUR_TIPS','smallint'), ('APSTIPR','varchar(1)'), ('APST_PAK','smallint'), ('STATUSS','varchar(3)'),
            ('SORT_NOS','varchar(516)'), ('DAT_SAK','varchar(32)'), ('DAT_MOD','varchar(32)'),
            ('DAT_BEIG','varchar(32)'), ('ATRIB','varchar(32)'), ('STD','varchar(256)')
        ],
        'aw_ppils': [
            ('KODS','int'), ('PPILS','varchar(256)')
        ],
        'aw_rajons': [
            ('KODS','int'), ('TIPS_CD','smallint'), ('NOSAUKUMS','varchar(128)'), ('VKUR_CD','int'),
            ('VKUR_TIPS','smallint'), ('APSTIPR','varchar(1)'), ('APST_PAK','smallint'), ('STATUSS','varchar(3)'),
            ('SORT_NOS','varchar(516)'), ('DAT_SAK','varchar(32)'), ('DAT_MOD','varchar(32)'),
            ('DAT_BEIG','varchar(32)'), ('ATRIB','varchar(32)')
        ],
        'aw_vietu_centroidi': [
            ('KODS','int'), ('TIPS_CD','smallint'), ('NOSAUKUMS','varchar(128)'), ('VKUR_CD','int'),
            ('VKUR_TIPS','smallint'), ('STD','varchar(256)'), ('KOORD_X','real'), ('KOORD_Y','real'),
            ('DD_N','real'), ('DD_E','real')
        ],
    }

    def ddl_for(table: str, cols: list[tuple[str,str]]) -> str:
        col_defs = ',\n        '.join([f"{name} {ctype}" for name, ctype in cols])
        return f"CREATE TABLE IF NOT EXISTS {table} (\n        {col_defs}\n    );"

    statements = []
    for base, cols in schemas.items():
        statements.append(ddl_for(base, cols))
        statements.append(ddl_for(f"{base}_his", cols))

    conn.executescript('\n\n'.join(statements))
    conn.commit()


def detect_delimiter(sample_line: str) -> str:
    semis = sample_line.count(';')
    commas = sample_line.count(',')
    if semis == 0 and commas == 0:
        return ';'
    return ',' if commas > semis else ';'


def get_table_columns(conn: sqlite3.Connection, table: str):
    cur = conn.execute(f"PRAGMA table_info({table})")
    return [row[1] for row in cur.fetchall()]


def load_csv(conn: sqlite3.Connection, table: str, csv_path: str, clear_before: bool = False):
    table_cols = get_table_columns(conn, table)
    if not table_cols:
        raise RuntimeError(f"Table '{table}' does not exist or has no columns. Ensure DDL is applied.")

    if clear_before:
        conn.execute(f"DELETE FROM {table}")
        conn.commit()

    with open(csv_path, 'r', encoding='utf-8', newline='') as f:
        pos = f.tell()
        first_line = f.readline()
        delim = detect_delimiter(first_line)
        f.seek(pos)
        reader = csv.reader(f, delimiter=delim)
        header = next(reader)
        # Normalize headers: remove BOM, whitespace, trailing/leading # and quotes, newlines
        def norm(h: str) -> str:
            return h.replace('\r', '').replace('\n', '').lstrip('\ufeff').strip().strip('#').strip('"').strip("'")
        header = [norm(h) for h in header]

        # Normalizer for cell values: trim CR/LF/space and symmetrical wrappers (#, ' or ")
        def norm_val(v: str) -> str:
            if v is None:
                return v
            v = v.replace('\r', '').replace('\n', '').strip()
            # Unwrap #...# if both ends are '#'
            if len(v) >= 2 and v[0] == '#' and v[-1] == '#':
                v = v[1:-1].strip()
            # Unwrap matching quotes
            if len(v) >= 2 and ((v[0] == '"' and v[-1] == '"') or (v[0] == "'" and v[-1] == "'")):
                v = v[1:-1].strip()
            return v

        insert_cols = [c for c in header if c in table_cols]
        if not insert_cols:
            raise RuntimeError(
                f"No matching columns between CSV ({os.path.basename(csv_path)}) and table '{table}'.\n"
                f"CSV header: {header}\nTable cols: {table_cols}"
            )

        placeholders = ','.join(['?'] * len(insert_cols))
        cols_sql = ','.join(insert_cols)
        sql = f"INSERT INTO {table} ({cols_sql}) VALUES ({placeholders})"

        conn.execute("PRAGMA synchronous = OFF")
        conn.execute("PRAGMA journal_mode = MEMORY")
        conn.execute("PRAGMA temp_store = MEMORY")
        conn.execute("PRAGMA cache_size = 100000")

        batch = []
        batch_size = 5000
        total = 0

        header_index_map = {h: i for i, h in enumerate(header)}
        col_index = [header_index_map[c] for c in insert_cols]

        conn.execute("BEGIN TRANSACTION")
        try:
            for row in reader:
                if len(row) < len(header):
                    row = row + [''] * (len(header) - len(row))
                values = [norm_val(row[i]) for i in col_index]
                batch.append(values)
                if len(batch) >= batch_size:
                    conn.executemany(sql, batch)
                    total += len(batch)
                    batch.clear()
            if batch:
                conn.executemany(sql, batch)
                total += len(batch)
        finally:
            conn.commit()

    print(f"Loaded {total} rows into {table} from {os.path.basename(csv_path)}")


def load_folder(conn: sqlite3.Connection, folder: str, mapping: dict, clear_tables: bool = True):
    files = sorted(os.listdir(folder)) if os.path.isdir(folder) else []
    planned = [(f, mapping[f]) for f in files if f in mapping]
    skipped = [f for f in files if f.lower().endswith('.csv') and f not in mapping]

    if not planned:
        print(f"No known CSV files found to load in {folder}.")
        return

    if clear_tables:
        for _, table in planned:
            conn.execute(f"DELETE FROM {table}")
        conn.commit()

    for fname, table in planned:
        path = os.path.join(folder, fname)
        print(f"Loading {path} into {table} ...")
        load_csv(conn, table, path)

    if skipped:
        print("Skipped files (no mapping):")
        for s in skipped:
            print(f" - {s}")


def main():
    conn = sqlite3.connect(DB_PATH)
    try:
        ensure_tables(conn)
        if os.path.isdir(CUR_DIR):
            load_folder(conn, CUR_DIR, CUR_FILE_TABLE_MAP, clear_tables=True)
        if os.path.isdir(HIS_DIR):
            load_folder(conn, HIS_DIR, HIS_FILE_TABLE_MAP, clear_tables=True)
        print('✅ ADR current and history CSVs loaded into SQLite tables.')
    finally:
        conn.close()


if __name__ == '__main__':
    main()
