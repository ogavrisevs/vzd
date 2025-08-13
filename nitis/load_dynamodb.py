import csv
import boto3
import os
import sys
from decimal import Decimal
from botocore.exceptions import ClientError

TABLE_NAME= "deal"
REGION_NAME='eu-central-1'

dynamodb = boto3.resource('dynamodb', region_name=REGION_NAME)
table = dynamodb.Table(TABLE_NAME)

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

def ensure_gsi_on_property_cadastral_number():
    gsi_name='property_cadastral_number-index'
    client = boto3.client('dynamodb', region_name=REGION_NAME)
    try:
        desc = client.describe_table(TableName=TABLE_NAME)
        gsis = desc['Table'].get('GlobalSecondaryIndexes', [])
        for gsi in gsis:
            if gsi['IndexName'] == gsi_name:
                print(f"GSI '{gsi_name}' already exists.")
                return
        print(f"Creating GSI '{gsi_name}'...")
        client.update_table(
            TableName=TABLE_NAME,
            AttributeDefinitions=[
                {'AttributeName': 'property_cadastral_number', 'AttributeType': 'S'},
            ],
            GlobalSecondaryIndexUpdates=[
                {
                    'Create': {
                        'IndexName': gsi_name,
                        'KeySchema': [
                            {'AttributeName': 'property_cadastral_number', 'KeyType': 'HASH'}
                        ],
                        'Projection': {'ProjectionType': 'ALL'}
                    }
                }
            ]
        )
        print(f"GSI '{gsi_name}' creation initiated.")
    except ClientError as e:
        print(f"Error: {e}")


def translate_field_name(latvian_name):
    """Translate Latvian field name to English, or return cleaned English version if no translation found"""
    # Debug: print the original field name being processed
    # print(f"DEBUG: Processing field: '{latvian_name}' (length: {len(latvian_name)})")
    
    # Trim whitespace and check for direct translation
    trimmed_name = latvian_name.strip().lstrip('\ufeff')

    if trimmed_name in FIELD_TRANSLATIONS:
        # print(f"DEBUG: Found translation for trimmed '{trimmed_name}' -> '{FIELD_TRANSLATIONS[trimmed_name]}'")
        return FIELD_TRANSLATIONS[trimmed_name]
    
    # Also check the original name (with whitespace) in case it's specifically mapped
    if latvian_name in FIELD_TRANSLATIONS:
        # print(f"DEBUG: Found translation for original '{latvian_name}' -> '{FIELD_TRANSLATIONS[latvian_name]}'")
        return FIELD_TRANSLATIONS[latvian_name]
    
    # Debug: if we reach here, no translation was found
    print(f"⚠️  No translation found for: '{latvian_name}' -> using fallback cleaning")
    
    # If no direct translation, clean up the field name
    cleaned = latvian_name.lower()
    
    # Replace Latvian characters with English equivalents
    latvian_chars = {
        'ā': 'a', 'č': 'c', 'ē': 'e', 'ģ': 'g', 'ī': 'i', 
        'ķ': 'k', 'ļ': 'l', 'ņ': 'n', 'š': 's', 'ū': 'u', 'ž': 'z',
        'Ā': 'a', 'Č': 'c', 'Ē': 'e', 'Ģ': 'g', 'Ī': 'i',
        'Ķ': 'k', 'Ļ': 'l', 'Ņ': 'n', 'Š': 's', 'Ū': 'u', 'Ž': 'z'
    }
    
    for latvian_char, english_char in latvian_chars.items():
        cleaned = cleaned.replace(latvian_char, english_char)
    
    # Clean up punctuation and spaces
    cleaned = cleaned.replace(' ', '_')
    cleaned = cleaned.replace(',', '')
    cleaned = cleaned.replace('(', '')
    cleaned = cleaned.replace(')', '')
    cleaned = cleaned.replace('/', '_')
    cleaned = cleaned.replace('-', '_')
    cleaned = cleaned.replace('.', '')
    cleaned = cleaned.replace(';', '')
    
    # Remove multiple underscores and trailing underscores
    while '__' in cleaned:
        cleaned = cleaned.replace('__', '_')
    cleaned = cleaned.strip('_')
    
    print(f"    Fallback result: '{cleaned}'")
    return cleaned

def clear_table():
    """Delete all items from the DynamoDB table"""
    print("🗑️  Clearing existing data from table...")
    
    try:
        # Scan the table to get all items
        response = table.scan(
            ProjectionExpression='pk, sk',  # Get both primary and sort keys
            Select='SPECIFIC_ATTRIBUTES'
        )

        items_to_delete = response.get('Items', [])
        deleted_count = 0

        # Delete items in batches
        with table.batch_writer() as batch:
            for item in items_to_delete:
                key = {'pk': item['pk']}
                if 'sk' in item:
                    key['sk'] = item['sk']
                batch.delete_item(Key=key)
                deleted_count += 1
                if deleted_count % 100 == 0:
                    print(f"Deleted {deleted_count} items...")

        # Handle pagination if there are more items
        while 'LastEvaluatedKey' in response:
            response = table.scan(
                ProjectionExpression='pk, sk',
                Select='SPECIFIC_ATTRIBUTES',
                ExclusiveStartKey=response['LastEvaluatedKey']
            )

            items_to_delete = response.get('Items', [])

            with table.batch_writer() as batch:
                for item in items_to_delete:
                    key = {'pk': item['pk']}
                    if 'sk' in item:
                        key['sk'] = item['sk']
                    batch.delete_item(Key=key)
                    deleted_count += 1
                    if deleted_count % 100 == 0:
                        print(f"Deleted {deleted_count} items...")
        
        print(f"✅ Cleared {deleted_count} existing items from table")
        
    except Exception as e:
        print(f"⚠️  Error clearing table: {e}")
        print("Continuing with data insertion...")

def convert_row(row, file_type, sequence_num):
    item = {}
    
    # Add file type as part of the composite key to avoid conflicts between files
    primary_key = None
    
    # Define fields that contain dates
    date_fields = {
        'deal_date'
    }
    
    for key, value in row.items():
        if value == '' or value == 'NULL':
            continue  # skip empty fields
        
        # Translate field name to English
        english_key = translate_field_name(key)
        
        # Identify the primary key field (Deal ID exists in all files)
        if key == 'Darījuma ID':
            primary_key = value

        # Handle different data types based on field name
        if english_key.endswith('_ids') and value is not None:
            # Handle list fields - automatically detect fields ending with "_list" or containing "list"
            if ',' in value:
                # Split by comma and clean up whitespace
                list_items = [item.strip() for item in value.split(',') if item.strip()]
                item[english_key] = list_items
            else:
                # Single item, still store as list for consistency
                item[english_key] = [value.strip()] if value.strip() else []
        
        elif english_key in date_fields and value is not None:
            # Handle date fields - store as string but could be formatted
            # Expected format: DD.MM.YYYY (e.g., "29.11.2023")
            item[english_key] = value.strip()
            
            # Also create ISO format version for easier querying
            try:
                from datetime import datetime
                # Parse DD.MM.YYYY format
                date_obj = datetime.strptime(value.strip(), '%d.%m.%Y')
                item[f"{english_key}_iso"] = date_obj.strftime('%Y-%m-%d')
                item[f"{english_key}_year"] = date_obj.year
                item[f"{english_key}_month"] = date_obj.month
            except:
                # If date parsing fails, just store the original value
                pass
        
        else:
            # Handle regular fields
            if english_key in ('property_cadastral_number', 'building_cadastral_id', 'room_group_cadastral_id'):
                # Always store as string
                item[english_key] = str(value).strip()
            else:
                try:
                    # Numeric conversion (use Decimal for DynamoDB)
                    item[english_key] = Decimal(value)
                except:
                    # String fields
                    if value is not None and str(value).strip() != '':
                        item[english_key] = value.strip()
    
    # Create composite primary key: file_type + deal_id
    if primary_key:
        item['pk'] = f"{file_type}#{primary_key}"
        item['deal_id'] = primary_key
        item['file_type'] = file_type
        item['sequence_num'] = sequence_num


        if file_type == "ZV":
            if 'property_cadastral_number' in item:
                item['sk'] = item['property_cadastral_number']
            elif 'land_unit_cadastral_ids' in item:
                value = item['land_unit_cadastral_ids']
                if isinstance(value, list) and value:
                    item['sk'] = value[0]
                else:
                    item['sk'] = value
        elif file_type == "TG":
            if 'room_group_cadastral_id' in item:
                item['sk'] = item['room_group_cadastral_id']
        elif file_type == "ZVB":
            if 'building_cadastral_id' in item:
                item['sk']  = item['building_cadastral_id']

        # Add composite field for file_type and deal_date_year if date is available
        if 'deal_date_year' in item:
            item['file_type_year'] = f"{file_type}#{item['deal_date_year']}"

    # Ensure both pk and sk are set
    if 'pk' not in item or 'sk' not in item or item['pk'] is None or item['sk'] is None:
        print(f"⚠️  Skipping item due to missing pk or sk: {item}")
        return None
    return item

def process_csv_file(file_path):
    print(f"Processing file: {file_path}")
    
    # Extract file type from path for primary key - handle any year
    file_name = os.path.basename(file_path)
    
    # Extract file type more flexibly (TG, ZV, ZVB, etc.) from various formats like:
    # TG_CSV_2024.csv, ZV_CSV_2023.csv, TG_CSV_2022.csv, etc.
    if '_CSV_' in file_name:
        file_type = file_name.split('_CSV_')[0]  # Gets "TG", "ZV", "ZVB", etc.
    else:
        # Fallback: use filename without extension
        file_type = file_name.replace('.csv', '').replace('.CSV', '')
        
    # Track seen (pk, sk) pairs to handle duplicates
    seen_keys = set()
    
    with open(file_path, newline='', encoding='utf-8') as csvfile:
        # Read the header line to get column names
        header_line = csvfile.readline().strip()
        headers = header_line.split(';')

        print(f"Found {len(headers)} columns: {headers[:5]}...")  # Show first 5 column names

        # Create DictReader with the actual headers from the file
        reader = csv.DictReader(csvfile, fieldnames=headers, delimiter=';')

        with table.batch_writer() as batch:
            row_count = 0
            for row in reader:
                item = convert_row(row, file_type, row_count + 1)  # Pass sequence number
                if item and 'pk' in item:  # Only insert if item has data and primary key
                    # Use (pk, sk) tuple for uniqueness
                    sk_value = item.get('sk')
                    pk_value = item.get('pk')
                    key_tuple = (pk_value, sk_value)
                    if key_tuple in seen_keys:
                        print(f"⚠️  Duplicate key found: pk={pk_value}, sk={sk_value}")
                    seen_keys.add(key_tuple)
                    batch.put_item(Item=item)
                    row_count += 1
                    if row_count % 100 == 0:  # Progress indicator
                        print(f"Processed {row_count} rows...")
                elif item:
                    print(f"⚠️  Skipping row without primary key: {row}")
            print(f"✅ Completed {file_path}: {row_count} rows inserted")

# Automatically find all CSV files in the nitis directory
nitis_dir = './nitis'
csv_files = []

# Walk through the nitis directory and subdirectories to find all CSV files
for root, dirs, files in os.walk(nitis_dir):
    for file in files:
        if file.endswith('.csv') and file.startswith('TG_CSV') or file.startswith('ZV_CSV') or file.startswith('ZVB_CSV'):
            csv_file_path = os.path.join(root, file)
            csv_files.append(csv_file_path)

# Sort files for consistent processing order
csv_files.sort()

print(f"Found {len(csv_files)} CSV files to process:")
for csv_file in csv_files:
    print(f"  - {csv_file}")

# Clear existing data before inserting new data
clear_table()

ensure_gsi_on_property_cadastral_number()

# Process each CSV file
for csv_file in csv_files:
    if os.path.exists(csv_file):
        process_csv_file(csv_file)
    else:
        print(f"⚠️  File not found: {csv_file}")

print("✅ All uploads completed.")