# List of fields to store as INTEGER in SQLite
INTEGER_FIELDS = [
    'deal_id_original',
    'deals_count_in_selection',
    'buildings_count',
    'building_above_ground_floors',
    'room_groups_count',
    'apartments_count',
    'rooms_count_in_group',
    'bedrooms_count_in_apartment'
    # Add more field names here as needed
]


DATE_FIELDS = [
    'deal_date'
]

import os
import sqlite3
import csv

DB_PATH = 'nitis.db'
CSV_DIR = './nitis/all'
TABLES = {
    'zv': 'ZV_CSV_',
    'tg': 'TG_CSV_',
    'zvb': 'ZVB_CSV_'
}

# Translation mapping from Latvian to English field names
FIELD_TRANSLATIONS = {
    'Darījumu skaits atlasē': 'deals_count_in_selection',
    'Darījuma ID': 'deal_id_original',
    'Objekts': 'object_type',
    'Īpašuma kadastra numurs': 'property_cadastral_number',
    'Adreses pieraksts': 'address_record',
    'Novads': 'municipality',
    'Pilsēta': 'city',
    'Pagasts': 'parish',
    'Darījuma datums': 'deal_date',
    'Darījuma summa, EUR': 'deal_amount_eur',
    'Zemes vienību kadastra apzīmējumi(saraksts) (viena darījuma ietvaros)': 'land_unit_cadastral_ids',
    'Zemes vienību kadastra apzīmējumu saraksts (viena darījuma ietvaros)' : 'land_unit_cadastral_ids',
    'Zemes daļas(skaitītājs)': 'land_share_numerator',
    'Zemes daļas(saucējs)': 'land_share_denominator',
    'Pārdotā zemes kopplatība, m2 ': 'sold_land_total_area_m2',
    'Pārdotā zemes kopplatība, m2': 'sold_land_total_area_m2',
    'NĪLM grupas kods (lielākais pēc platības)': 'nilm_group_code_largest_by_area',
    'NĪLM kodi(saraksts)': 'nilm_codes_ids',
    'NĪLM (lielākais pēc platības)': 'nilm_largest_by_area',
    'Būvju skaits': 'buildings_count',
    'Būves kadastra apzīmējums': 'building_cadastral_id',
    'Būves daļas(skaitītājs)': 'building_share_numerator',
    'Būves daļas(saucējs)': 'building_share_denominator',
    'Būves daļas, skaitītājs': 'building_share_numerator',
    'Būves daļas, saucējs': 'building_share_denominator',
    'Būves lietošanas veida nosaukums': 'building_usage_type_name',
    'Būves lietošanas veida kods': 'building_usage_type_code',
    'Būves virszemes stāvu skaits': 'building_above_ground_floors',
    'Būves apbūves laukums, m2': 'building_footprint_area_m2',
    'Būves kopplatība, m2': 'building_total_area_m2',
    'Būves būvtilpums, m3': 'building_volume_m3',
    'Būves ārsienu materiāla nosaukums': 'building_exterior_wall_material',
    'Būves ekspluatācijas uzsākšanas gads': 'building_commissioning_year',
    'Būves fiziskais nolietojums, %': 'building_physical_depreciation_percent',
    'Būves nolietojums': 'building_depreciation',
    'Būves kadastra apzīmējumu saraksts (viena darījuma ietvaros)': 'building_cadastral_ids',
    'Telpu grupu skaits (viena darījuma ietvaros)': 'room_groups_count',
    'Dzīvokļu skaits (viena darījuma ietvaros)': 'apartments_count',
    'Telpu grupas kadastra apzīmējums': 'room_group_cadastral_id',
    'Telpu grupas daļas(skaitītājs)': 'room_group_share_numerator',
    'Telpu grupas daļas(saucējs)': 'room_group_share_denominator',
    'Telpu grupas lietošanas veida kods': 'room_group_usage_type_code',
    'Telpu grupas zemākais stāvs': 'room_group_lowest_floor',
    'Telpu grupas augstākais stāvs': 'room_group_highest_floor',
    'Telpu grupas platība, m2': 'room_group_area_m2',
    'Dzīvokļa kopplatība, m2': 'apartment_total_area_m2',
    'Telpu skaits telpu grupā': 'rooms_count_in_group',
    'Istabu skaits dzīvoklī': 'bedrooms_count_in_apartment',
    'Zemes vienību skaits': 'land_units_count',
    'Vai zeme ir apbūvēta (0-nav/1-ir)': 'is_land_built_up',
    'Pārdotā lauksaimniecības zemes platība, m2': 'sold_agricultural_land_area_m2',
    'Pārdotā aramzemes platība, m2': 'sold_arable_land_area_m2',
    'Pārdotā augļu dārzu platība, m2': 'sold_orchard_area_m2',
    'Pārdotā pļavu platība, m2': 'sold_meadow_area_m2',
    'Pārdotā ganību platība, m2': 'sold_pasture_area_m2',
    'Pārdotā meliorētās LIZ platība, m2': 'sold_improved_agricultural_area_m2',
    'Pārdotā mežu zemes platība, m2': 'sold_forest_land_area_m2',
    'Pārdotā krūmāju platība, m2': 'sold_shrubland_area_m2',
    'Pārdotā purvu platība, m2': 'sold_swampland_area_m2',
    'Pārdotā zemes zem ūdeņiem platība, m2': 'sold_underwater_land_area_m2',
    'Pārdotā zemes zem dīķiem platība, m2': 'sold_pond_land_area_m2',
    'Pārdotā zemes zem ēkām un pagalmiem platība, m2': 'sold_built_up_land_area_m2',
    'Pārdotā zemes zem ceļiem platība, m2': 'sold_road_land_area_m2',
    'Pārdotā pārējās zemes platība, m2': 'sold_other_land_area_m2',
    'Zemes daļas (skaitītājs)': 'land_share_numerator',
    'Zemes daļas (saucējs)': 'land_share_denominator'
}

def translate_field_name(latvian_name):
    trimmed_name = latvian_name.strip().lstrip('\ufeff')
    if trimmed_name in FIELD_TRANSLATIONS:
        return FIELD_TRANSLATIONS[trimmed_name]
    # Fallback cleaning similar to load_dynamodb.py
    cleaned = latvian_name.lower()
    latvian_chars = {
        'ā': 'a', 'č': 'c', 'ē': 'e', 'ģ': 'g', 'ī': 'i', 
        'ķ': 'k', 'ļ': 'l', 'ņ': 'n', 'š': 's', 'ū': 'u', 'ž': 'z',
        'Ā': 'a', 'Č': 'c', 'Ē': 'e', 'Ģ': 'g', 'Ī': 'i',
        'Ķ': 'k', 'Ļ': 'l', 'Ņ': 'n', 'Š': 's', 'Ū': 'u', 'Ž': 'z'
    }
    for latvian_char, english_char in latvian_chars.items():
        cleaned = cleaned.replace(latvian_char, english_char)
    cleaned = cleaned.replace(' ', '_')
    cleaned = cleaned.replace(',', '')
    cleaned = cleaned.replace('(', '')
    cleaned = cleaned.replace(')', '')
    cleaned = cleaned.replace('/', '_')
    cleaned = cleaned.replace('-', '_')
    cleaned = cleaned.replace('.', '')
    cleaned = cleaned.replace(';', '')
    while '__' in cleaned:
        cleaned = cleaned.replace('__', '_')
    cleaned = cleaned.strip('_')
    return cleaned


def create_table_with_columns(conn, table, columns):
    # Create table only if it doesn't exist (append mode)
    col_defs = ', '.join([
        f'{col} INTEGER' if col in INTEGER_FIELDS 
        else f'{col} DATE' if col in DATE_FIELDS
        else f'{col} TEXT'
        for col in columns
    ])
    conn.execute(f'CREATE TABLE IF NOT EXISTS {table} ({col_defs})')
    conn.commit()

def load_csv_to_table(conn, table, csv_path):
    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter=';')
        headers = next(reader)
        columns = [translate_field_name(h) for h in headers]
        row_count = 0
        for row in reader:
            # Pad short rows with empty strings
            if len(row) < len(columns):
                row = row + [''] * (len(columns) - len(row))
            elif len(row) > len(columns):
                row = row[:len(columns)]

            # Convert integer and date fields
            converted_row = []
            for value, col in zip(row, columns):
                if col in INTEGER_FIELDS:
                    try:
                        # Accept empty string or NULL as None
                        if value.strip() == '' or value.strip().upper() == 'NULL':
                            converted_row.append(None)
                        else:
                            converted_row.append(int(value))
                    except Exception:
                        converted_row.append(None)
                elif col in DATE_FIELDS:
                    try:
                        # Convert DD.MM.YYYY to YYYY-MM-DD for SQLite
                        if value.strip() == '' or value.strip().upper() == 'NULL':
                            converted_row.append(None)
                        else:
                            from datetime import datetime
                            date_obj = datetime.strptime(value.strip(), '%d.%m.%Y')
                            converted_row.append(date_obj.strftime('%Y-%m-%d'))
                    except Exception:
                        converted_row.append(None)
                else:
                    converted_row.append(value)

            placeholders = ','.join(['?'] * len(columns))
            try:
                conn.execute(f'INSERT INTO {table} VALUES ({placeholders})', converted_row)
                row_count += 1
            except Exception as e:
                raise RuntimeError(f"Failed to insert row into {table} from {os.path.basename(csv_path)}: {converted_row}\nError: {e}")
    conn.commit()
    print(f'Inserted {row_count} rows into {table} from {os.path.basename(csv_path)}')


def main():
    conn = sqlite3.connect(DB_PATH)
    
    # Clear all tables at the start
    for table in TABLES.keys():
        conn.execute(f'DROP TABLE IF EXISTS {table}')
    conn.commit()
    
    # Create all tables first by reading the first CSV file of each type
    for table, prefix in TABLES.items():
        for fname in os.listdir(CSV_DIR):
            if fname.startswith(prefix) and fname.endswith('.csv'):
                csv_path = os.path.join(CSV_DIR, fname)
                with open(csv_path, newline='', encoding='utf-8') as f:
                    reader = csv.reader(f, delimiter=';')
                    headers = next(reader)
                    columns = [translate_field_name(h) for h in headers]
                    create_table_with_columns(conn, table, columns)
                    print(f'Created table {table} with {len(columns)} columns')
                break  # Only need first file to create table structure
    
    # Now load all CSV files
    for table, prefix in TABLES.items():
        for fname in os.listdir(CSV_DIR):
            if fname.startswith(prefix) and fname.endswith('.csv'):
                csv_path = os.path.join(CSV_DIR, fname)
                print(f'Loading {csv_path} into {table}...')
                load_csv_to_table(conn, table, csv_path)
    conn.close()
    print('✅ All CSV files loaded into SQLite tables.')

if __name__ == '__main__':
    main()
