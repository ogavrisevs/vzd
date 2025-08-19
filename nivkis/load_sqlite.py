import sqlite3
import xml.etree.ElementTree as ET
import os


DB_PATH = "nivkis.db"
ADDRESS_DIR = "./nivkis/all/Address"
OWNERSHIP_DIR = "./nivkis/all/Ownership"
# --- Merged logic for address and ownership ---
ADDRESS_TABLE = "address"
ADDRESS_COLUMNS = [
    ("ObjectCadastreNr", "TEXT"),
    ("ObjectType", "TEXT"),
    ("ARCode", "TEXT"),
    ("PostIndex", "TEXT"),
    ("Town", "TEXT"),
    ("County", "TEXT"),
    ("Parish", "TEXT"),
    ("Village", "TEXT"),
    ("Street", "TEXT"),
    ("House", "TEXT"),
    ("Apartment", "TEXT")
]
ADDRESS_CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {ADDRESS_TABLE} (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ObjectCadastreNr TEXT,
    ObjectType TEXT,
    ARCode TEXT,
    PostIndex TEXT,
    Town TEXT,
    County TEXT,
    Parish TEXT,
    Village TEXT,
    Street TEXT,
    House TEXT,
    Apartment TEXT
);
"""

OWNERSHIP_TABLE = "ownership"
OWNERSHIP_COLUMNS = [
    ("ObjectCadastreNr", "TEXT"),
    ("ObjectType", "TEXT"),
    ("OwnershipStatus", "TEXT"),
    ("PersonStatus", "TEXT")
]
OWNERSHIP_CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {OWNERSHIP_TABLE} (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ObjectCadastreNr TEXT,
    ObjectType TEXT,
    OwnershipStatus TEXT,
    PersonStatus TEXT
);
"""

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()
# Drop and recreate tables each run
cursor.execute(f"DROP TABLE IF EXISTS {ADDRESS_TABLE};")
cursor.execute(f"DROP TABLE IF EXISTS {OWNERSHIP_TABLE};")
conn.commit()
cursor.execute(ADDRESS_CREATE_SQL)
cursor.execute(OWNERSHIP_CREATE_SQL)
conn.commit()

def address_row_exists(row):
    where_clause = " AND ".join([f"{col[0]}=?" for col in ADDRESS_COLUMNS])
    select_sql = f"SELECT 1 FROM {ADDRESS_TABLE} WHERE {where_clause} LIMIT 1"
    cursor.execute(select_sql, row)
    return cursor.fetchone() is not None

def ownership_row_exists(row):
    where_clause = " AND ".join([f"{col[0]}=?" for col in OWNERSHIP_COLUMNS])
    select_sql = f"SELECT 1 FROM {OWNERSHIP_TABLE} WHERE {where_clause} LIMIT 1"
    cursor.execute(select_sql, row)
    return cursor.fetchone() is not None

def parse_and_insert(xml_path, file_num=None, total_files=None):
    tree = ET.parse(xml_path)
    root = tree.getroot()
    address_rows = []
    ownership_rows = []
    for item in root.findall(".//AddressItemData"):
        obj_rel = item.find("ObjectRelation")
        object_cadastre_nr = obj_rel.find("ObjectCadastreNr").text if obj_rel is not None and obj_rel.find("ObjectCadastreNr") is not None else None
        object_type = obj_rel.find("ObjectType").text if obj_rel is not None and obj_rel.find("ObjectType") is not None else None
        # AddressData
        address = item.find("AddressData")
        if address is not None:
            row = [object_cadastre_nr, object_type]
            for col in ADDRESS_COLUMNS[2:]:
                elem = address.find(col[0])
                row.append(elem.text if elem is not None else None)
            #if not address_row_exists(row):
            address_rows.append(row)
        # OwnershipStatusKindList
        status_list = item.find("OwnershipStatusKindList")
        if status_list is not None:
            for status_kind in status_list.findall("OwnershipStatusKind"):
                ownership_status = status_kind.find("OwnershipStatus").text if status_kind.find("OwnershipStatus") is not None else None
                person_status = status_kind.find("PersonStatus").text if status_kind.find("PersonStatus") is not None else None
                row = [object_cadastre_nr, object_type, ownership_status, person_status]
                if not ownership_row_exists(row):
                    ownership_rows.append(row)
    # Insert address rows
    if address_rows:
        placeholders = ", ".join(["?" for _ in ADDRESS_COLUMNS])
        insert_sql = f"INSERT INTO {ADDRESS_TABLE} (ObjectCadastreNr, ObjectType, ARCode, PostIndex, Town, County, Parish, Village, Street, House, Apartment) VALUES ({placeholders})"
        cursor.executemany(insert_sql, address_rows)
        conn.commit()
        msg_addr = f"Inserted {len(address_rows)} address rows from {os.path.basename(xml_path)}."
    else:
        msg_addr = f"No new address data found in {os.path.basename(xml_path)}."
    # Insert ownership rows
    if ownership_rows:
        placeholders = ", ".join(["?" for _ in OWNERSHIP_COLUMNS])
        insert_sql = f"INSERT INTO {OWNERSHIP_TABLE} (ObjectCadastreNr, ObjectType, OwnershipStatus, PersonStatus) VALUES ({placeholders})"
        cursor.executemany(insert_sql, ownership_rows)
        conn.commit()
        msg_own = f"Inserted {len(ownership_rows)} ownership rows from {os.path.basename(xml_path)}."
    else:
        msg_own = f"No new ownership data found in {os.path.basename(xml_path)}."
    # Print progress
    if file_num is not None and total_files is not None:
        print(f"[{file_num}/{total_files}] {msg_addr} {msg_own}")
    else:
        print(msg_addr, msg_own)


if __name__ == "__main__":
    #Scan Address subdir for address XML files
    address_xml_files = []
    for root_dir, dirs, files in os.walk(ADDRESS_DIR):
        for file in files:
            if file.lower().endswith('.xml') and file.lower() != 'output.xml':
                address_xml_files.append(os.path.join(root_dir, file))
    total_addr_files = len(address_xml_files)
    print(f"Found {total_addr_files} address XML files in '{ADDRESS_DIR}'.")
    for idx, xml_path in enumerate(address_xml_files, 1):
        # Only load address data from these files
        tree = ET.parse(xml_path)
        root = tree.getroot()
        address_rows = []
        for item in root.findall(".//AddressItemData"):
            obj_rel = item.find("ObjectRelation")
            object_cadastre_nr = obj_rel.find("ObjectCadastreNr").text if obj_rel is not None and obj_rel.find("ObjectCadastreNr") is not None else None
            object_type = obj_rel.find("ObjectType").text if obj_rel is not None and obj_rel.find("ObjectType") is not None else None
            address = item.find("AddressData")
            if address is not None:
                row = [object_cadastre_nr, object_type]
                for col in ADDRESS_COLUMNS[2:]:
                    elem = address.find(col[0])
                    row.append(elem.text if elem is not None else None)
                address_rows.append(row)
        if address_rows:
            placeholders = ", ".join(["?" for _ in ADDRESS_COLUMNS])
            insert_sql = f"INSERT INTO {ADDRESS_TABLE} (ObjectCadastreNr, ObjectType, ARCode, PostIndex, Town, County, Parish, Village, Street, House, Apartment) VALUES ({placeholders})"
            cursor.executemany(insert_sql, address_rows)
            conn.commit()
            msg_addr = f"Inserted {len(address_rows)} address rows from {os.path.basename(xml_path)}."
        else:
            msg_addr = f"No address data found in {os.path.basename(xml_path)}."
        print(f"[Address {idx}/{total_addr_files}] {msg_addr}")


    # Scan Ownership dir for ownership XML files
    ownership_xml_files = []
    for root_dir, dirs, files in os.walk(OWNERSHIP_DIR):
        for file in files:
            if file.lower().endswith('.xml') and file.lower() != 'output.xml':
                ownership_xml_files.append(os.path.join(root_dir, file))
    total_own_files = len(ownership_xml_files)
    print(f"Found {total_own_files} ownership XML files in '{OWNERSHIP_DIR}'.")
    for idx, xml_path in enumerate(ownership_xml_files, 1):
        # Only load ownership data from these files
        tree = ET.parse(xml_path)
        root = tree.getroot()
        ownership_rows = []
        for item in root.findall(".//OwnershipItemData"):
            obj_rel = item.find("ObjectRelation")
            object_cadastre_nr = obj_rel.find("ObjectCadastreNr").text if obj_rel is not None and obj_rel.find("ObjectCadastreNr") is not None else None
            object_type = obj_rel.find("ObjectType").text if obj_rel is not None and obj_rel.find("ObjectType") is not None else None
            status_list = item.find("OwnershipStatusKindList")
            if status_list is not None:
                for status_kind in status_list.findall("OwnershipStatusKind"):
                    ownership_status = status_kind.find("OwnershipStatus").text if status_kind.find("OwnershipStatus") is not None else None
                    person_status = status_kind.find("PersonStatus").text if status_kind.find("PersonStatus") is not None else None
                    row = [object_cadastre_nr, object_type, ownership_status, person_status]
                    ownership_rows.append(row)
        if ownership_rows:
            placeholders = ", ".join(["?" for _ in OWNERSHIP_COLUMNS])
            insert_sql = f"INSERT INTO {OWNERSHIP_TABLE} (ObjectCadastreNr, ObjectType, OwnershipStatus, PersonStatus) VALUES ({placeholders})"
            cursor.executemany(insert_sql, ownership_rows)
            conn.commit()
            msg_own = f"Inserted {len(ownership_rows)} ownership rows from {os.path.basename(xml_path)}."
        else:
            msg_own = f"No ownership data found in {os.path.basename(xml_path)}."
        print(f"[Ownership {idx}/{total_own_files}] {msg_own}")

    conn.close()
