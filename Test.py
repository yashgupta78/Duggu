import os
import json
import pandas as pd
import datetime
from xml.etree import ElementTree as ET

# --- Error Logging Function ---

def log_error(file_path, error):
    """Appends a formatted error message to the error_log.txt file."""
    log_file = 'error_log.txt'
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(log_file, 'a') as f:
        f.write(f"[{timestamp}] - ERROR processing file\n")
        f.write(f"  File Path: {file_path}\n")
        f.write(f"  Reason: {str(error)}\n\n")

# --- Helper Functions (Common for both processes) ---

def flatten_dict(data, parent_key='', sep='.'):
    """Flattens a nested dictionary."""
    items = {}
    for key, value in data.items():
        new_key = parent_key + sep + key if parent_key else key
        if isinstance(value, dict):
            items.update(flatten_dict(value, new_key, sep=sep))
        else:
            items[new_key] = value
    return items

def etree_to_dict(t):
    """Converts an ElementTree object to a dictionary."""
    d = {t.tag: {} if t.attrib else None}
    children = list(t)
    if children:
        dd = {}
        for dc in map(etree_to_dict, children):
            for k, v in dc.items():
                if k in dd:
                    if not isinstance(dd[k], list):
                        dd[k] = [dd[k]]
                    dd[k].append(v)
                else:
                    dd[k] = v
        d = {t.tag: dd}
    if t.attrib:
        d[t.tag].update(('@' + k, v) for k, v in t.attrib.items())
    if t.text:
        text = t.text.strip()
        if children or t.attrib:
            if text:
                d[t.tag]['#text'] = text
        else:
            d[t.tag] = text
    return d

# --- Core Processing Function ---

def process_folder(folder_path, file_extension, data_parser, output_prefix):
    """A generic function to process a folder of data files (JSON or XML)."""
    data_files = sorted([f for f in os.listdir(folder_path) if f.endswith(file_extension)])
    if not data_files:
        print(f"  No '{file_extension}' files found to process.")
        return

    excel_groups = []
    for file_name in data_files:
        file_path = os.path.join(folder_path, file_name)
        flattened_data = data_parser(file_path)
        
        if flattened_data is None:
            continue
            
        current_file_attributes = set(flattened_data.keys())
        found_group = False
        for group in excel_groups:
            if group['attributes'].intersection(current_file_attributes):
                group['data'].append(flattened_data)
                group['attributes'].update(current_file_attributes)
                found_group = True
                break
        
        if not found_group:
            new_group_id = len(excel_groups) + 1
            excel_groups.append({
                'id': new_group_id,
                'attributes': current_file_attributes,
                'data': [flattened_data]
            })

    if not excel_groups:
        print("  No valid data was processed to create Excel files.")
        return
        
    for group in excel_groups:
        df = pd.DataFrame(group['data'])
        
        # --- MODIFIED LINE FOR NEW NAMING CONVENTION ---
        output_filename = f"{output_prefix}{group['id']}.xlsx"
        
        df.to_excel(output_filename, index=False)
        print(f"  -> Successfully created {output_filename} with {len(group['data'])} rows.")

# --- Data Parser Implementations with Error Handling ---

def parse_json_file(file_path):
    """Parser for JSON files with robust error handling."""
    try:
        with open(file_path, 'r') as f:
            content = f.read()
            if not content:
                raise ValueError("File is empty")
            data = json.loads(content)
        return flatten_dict(data)
    except (json.JSONDecodeError, IOError, ValueError) as e:
        log_error(file_path, e)
        print(f"  Warning: Failed to process {os.path.basename(file_path)}. See error_log.txt.")
        return None

def parse_xml_file(file_path):
    """Parser for XML files with robust error handling."""
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()
        xml_as_dict = etree_to_dict(root)
        return flatten_dict(xml_as_dict)
    except (ET.ParseError, IOError) as e:
        log_error(file_path, e)
        print(f"  Warning: Failed to process {os.path.basename(file_path)}. See error_log.txt.")
        return None
        
# --- Main Controller Function ---

def process_all_subfolders(parent_folder):
    """Walks through subfolders, detects file type, and processes them."""
    print(f"--- Starting processing for parent folder: '{parent_folder}' ---")
    try:
        if os.path.exists('error_log.txt'):
            os.remove('error_log.txt')
        subfolders = [f.path for f in os.scandir(parent_folder) if f.is_dir()]
    except FileNotFoundError:
        print(f"Error: Parent folder '{parent_folder}' not found.")
        return

    if not subfolders:
        print("No subfolders found to process.")
        return

    for folder_path in subfolders:
        folder_name = os.path.basename(folder_path)
        print(f"\nProcessing subfolder: '{folder_name}'...")
        
        files_in_folder = os.listdir(folder_path)
        file_type_detected = None
        for file_name in files_in_folder:
            if file_name.endswith('.json'):
                file_type_detected = 'json'
                break
            if file_name.endswith('.xml'):
                file_type_detected = 'xml'
                break

        if file_type_detected == 'json':
            print(f"  Detected JSON files. Running JSON processor...")
            process_folder(folder_path, '.json', parse_json_file, folder_name)
        elif file_type_detected == 'xml':
            print(f"  Detected XML files. Running XML processor...")
            process_folder(folder_path, '.xml', parse_xml_file, folder_name)
        else:
            print(f"  No .json or .xml files found in this folder. Skipping.")
    
    print("\n--- All processing complete. ---")

# --- How to use ---

# 1. Create directory structure
parent_dir = 'company_data'
json_dir = os.path.join(parent_dir, 'sales_reports_json')
xml_dir = os.path.join(parent_dir, 'employee_records_xml')
os.makedirs(json_dir, exist_ok=True)
os.makedirs(xml_dir, exist_ok=True)

# 2. Populate with sample files that will create multiple groups
# JSON sales data - will create 2 groups
with open(os.path.join(json_dir, 'report1.json'), 'w') as f:
    json.dump({'region': 'North', 'sales': 50000}, f)
with open(os.path.join(json_dir, 'order1.json'), 'w') as f:
    json.dump({'order_id': 'A123', 'amount': 250}, f)
with open(os.path.join(json_dir, 'report2.json'), 'w') as f:
    json.dump({'region': 'South', 'sales': 75000}, f)

# XML employee data - will create 1 group
with open(os.path.join(xml_dir, 'emp1.xml'), 'w') as f:
    f.write('<employee><id>E1</id><name>Alice</name></employee>')
with open(os.path.join(xml_dir, 'emp2.xml'), 'w') as f:
    f.write('<employee><id>E2</id><name>Bob</name></employee>')

# 3. Run the main controller function
process_all_subfolders(parent_dir)
