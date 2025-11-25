import csv
import datetime
from hashlib import sha256
import psycopg
import pandas
import time
import re
from typing import Dict, List, Optional

class HospitalChargeETLCSV:

    ALLOWED_CPT_HCPCS_CODES = {
        '19120', '29826', '29881', '33206', '33207', '33208', '33274', '36415', '42820',
        '43235', '43239', '45378', '45380', '45385', '45391', '47562', '49505', '55700',
        '55866', '59400', '59510', '59610', '62322', '64483', '66821', '66984', '70450',
        '70553', '72110', '72148', '72193', '73700', '73702', '73721', '74176', '74177',
        '74178', '76700', '76805', '76830', '77065', '77066', '77067', '80048', '80053',
        '80055', '80061', '80069', '80076', '81000', '81001', '81002', '81003', '84153',
        '84154', '84443', '85025', '85027', '85610', '85730', '90832', '90834', '90837',
        '90846', '90847', '90853', '92961', '93000', '93306', '93350', '93452', '93650',
        '93656', '95810', '97110', '97161', '97162', '97163', '99203', '99204', '99205',
        '99243', '99244', '99385', '99386', '99421', '99422', '99423', '00670', '01214',
        '01215', '01402', '01961', '01967', '12001', '17134', '20526', '20550', '20552',
        '20600', '20605', '20606', '20610', '20611', '20612', '20931', '22514', '22551',
        '22845', '23350', '24220', '25246', '27093', '27096', '27130', '27134', '27369',
        '27447', '27648', '29826', '29827', '29881', '32555', '36415', '38571', '42820',
        '45385', '46415', '47000', '47562', '49083', '49505', '50200', '51700', '51701',
        '51798', '52000', '55700', '55866', '58340', '59400', '59610', '62323', '63047',
        '63048', '63060', '64447', '64483', '66291', '70110', '70140', '70160', '70200',
        '70220', '70260', '70330', '70336', '70355', '70450', '70460', '70470', '70480',
        '70481', '70482', '70486', '70487', '70490', '70491', '70492', '70540', '70543',
        '70551', '70553', '71045', '71046', '71100', '71101', '71120', '71130', '71250',
        '71260', '71270', '71550', '71552', '72020', '72040', '72070', '72072', '72082',
        '72100', '72110', '72125', '72126', '72128', '72129', '72131', '72132', '72141',
        '72146', '72148', '72156', '72157', '72158', '72170', '72192', '72193', '72194',
        '72195', '72197', '72202', '72220', '73000', '73010', '73030', '73040', '73050',
        '73080', '73085', '73090', '73110', '73115', '73130', '73140', '73200', '73201',
        '73218', '73220', '73221', '73223', '73502', '73525', '73552', '73562', '73564',
        '73580', '73590', '73610', '73630', '73650', '73660', '73700', '73701', '73718',
        '73720', '73721', '73723', '73925', '73971', '74018', '74150', '74153', '74160',
        '74170', '74176', '74177', '74178', '74181', '74183', '74220', '74270', '74280',
        '74740', '75012', '75557', '75561', '75565', '76000', '76376', '76380', '76506',
        '76536', '76604', '76641', '76642', '76700', '76705', '76770', '76775', '76776',
        '76801', '76805', '76811', '76813', '76815', '76816', '76817', '76819', '76830',
        '76831', '76856', '76857', '76870', '76872', '76882', '76942', '76946', '77002',
        '77063', '77065', '77066', '77067', '77072', '77073', '77074', '77075', '77077',
        '78452', '78815', '78816', '80048', '80053', '80055', '80061', '80069', '80076',
        '81000', '81001', '81002', '81003', '82040', '82043', '82247', '82248', '82306',
        '82310', '82374', '82435', '82565', '82570', '82607', '82728', '82947', '83036',
        '83540', '83550', '83735', '83970', '84075', '84100', '84132', '84153', '84154',
        '84155', '84156', '84439', '84443', '84450', '84460', '85027', '85610', '85652',
        '85730', '86140', '87086', '88300', '88300', '88307', '88313', '88346', '90832',
        '90834', '90837', '90846', '90847', '90853', '93000', '93005', '93010', '93016',
        '93017', '93018', '93225', '93226', '93227', '93308', '93312', '93320', '93325',
        '93350', '93452', '93880', '93882', '93886', '93888', '93892', '93893', '93923',
        '93926', '93930', '93931', '93970', '93975', '93976', '93978', '93979', '94070',
        '94640', '94668', '94760', '94762', '95720', '95810', '96101', '97110', '99152',
        '99153', '99211', '99243', '99244', '99385', '99386', '99421', '99422', '99423',
        'C8928'
    }
    
    ALLOWED_TYPES = {'MS-DRG', 'APR-DRG', 'CPT', 'HCPCS'}

    db_connection_str: str
    hospital_license_number: str
    column_mapping: dict

    def __init__(self, db_connection_str: str):
        self.db_connection_str = db_connection_str

    def normalize_string(self, s: str) -> str:
        if not s:
            return ""
        return re.sub(r'[^a-z0-9]', '', s.lower())

    def discover_columns(self, columns: list[str]) -> dict:
        mapping = {
            "code_columns": [],
            "type_columns": [],
            "setting": None,
            "description": None,
            "payer_name": None,
            "plan_name": None,
            "modifiers": None,
            "gross": None,
            "discounted_cash": None,
            "min": None,
            "max": None,
            "negotiated_dollar": None,
            "negotiated_percentage": None,
            "negotiated_algorithm": None,
            "estimated_amount": None,
            "methodology": None,
            "additional_notes": None,
        }

        for col in columns:
            normalized = self.normalize_string(col)

            if re.match(r'code\d+$', normalized):
                mapping["code_columns"].append(col)
            elif re.match(r'code\d+type$', normalized):
                mapping["type_columns"].append(col)
            elif "setting" in normalized:
                mapping["setting"] = col
            elif "description" in normalized or normalized == "desc":
                mapping["description"] = col
            elif "payer" in normalized and "name" in normalized:
                mapping["payer_name"] = col
            elif "plan" in normalized and "name" in normalized:
                mapping["plan_name"] = col
            elif "modifier" in normalized:
                mapping["modifiers"] = col
            elif "gross" in normalized:
                mapping["gross"] = col
            elif "discounted" in normalized:
                mapping["discounted_cash"] = col
            elif "min" in normalized:
                mapping["min"] = col
            elif "max" in normalized:
                mapping["max"] = col
            elif "negotiated" in normalized:
                if "dollar" in normalized:
                    mapping["negotiated_dollar"] = col
                elif "percent" in normalized:
                    mapping["negotiated_percentage"] = col
                elif "algorithm" in normalized:
                    mapping["negotiated_algorithm"] = col
            elif "estimated" in normalized:
                mapping["estimated_amount"] = col
            elif "methodology" in normalized:
                mapping["methodology"] = col
            elif "note" in normalized:
                mapping["additional_notes"] = col

        return mapping
    
    def read_hospital_data(self, file_path: str) -> dict:
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            header_row = next(reader)
            data_row = next(reader)            
            return dict(zip(header_row, data_row))
            
    def upsert_hospital_data(self, metadata: dict):
        hospital_license_number = ""
        hospital_name = None
        hospital_address = None
        hospital_location = None
        as_of_date = datetime.date.today()
        last_update = None
        version = None
        
        for key in metadata.keys():
            normalized = self.normalize_string(key)
            if "license" in normalized and "number" in normalized:
                hospital_license_number = metadata[key] + "|OH"
            elif "name" in normalized:
                hospital_name = metadata[key]
            elif "address" in normalized:
                hospital_address = metadata[key]
            elif "location" in normalized:
                hospital_location = metadata[key]
            elif "update" in normalized:
                last_update = metadata[key]
            elif "version" in normalized:
                version = metadata[key]

        self.hospital_license_number = hospital_license_number

        with psycopg.connect(self.db_connection_str) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO HOSPITALS (hospital_license_number, hospital_name, hospital_address, hospital_location, as_of_date, last_update, version)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (hospital_license_number)
                    DO UPDATE SET
                        hospital_name = EXCLUDED.hospital_name,
                        hospital_address = EXCLUDED.hospital_address,
                        hospital_location = EXCLUDED.hospital_location,
                        as_of_date = EXCLUDED.as_of_date,
                        last_update = EXCLUDED.last_update,
                        version = EXCLUDED.version
                """, (hospital_license_number, hospital_name, hospital_address, hospital_location, as_of_date, last_update, version))
                    
    def normalize_code_type(self, code_type: str) -> Optional[str]:
        if not code_type or (isinstance(code_type, float) and pandas.isna(code_type)):
            return None
        
        # Convert to string if not already
        code_type = str(code_type)
    
        normalized = code_type.upper().strip()
        # Handle variations like "APR-DRG" vs "APRDRG"
        normalized = normalized.replace('-', '').replace('_', '').replace(' ', '')
        
        type_mappings = {
            'MSDRG': 'MS-DRG',
            'MS_DRG': 'MS-DRG',
            'APRDRG': 'APR-DRG',
            'APR_DRG': 'APR-DRG',
            'CPT': 'CPT',
            'HCPCS': 'HCPCS',
        }
        
        return type_mappings.get(normalized, code_type.upper().strip())

    def normalize_setting(self, setting: str) -> Optional[str]:
        if not setting or (isinstance(setting, float) and pandas.isna(setting)):
            return None
        
        # Convert to string if not already
        setting = str(setting)
        
        normalized = setting.lower().strip()
        
        if 'inpatient' in normalized or normalized == 'in':
            return 'Inpatient'
        elif 'outpatient' in normalized or normalized == 'out':
            return 'Outpatient'
        elif 'both' in normalized:
            return 'Both'  # Special marker for duplication
        
        # Default: capitalize first letter
        return setting.capitalize()

    def filter_services(self, file_path: str) -> pandas.DataFrame:
        """Filter services with flexible column discovery and vectorized operations"""
        
        # Discover columns from file
        print("Discovering column structure...")
        with open(file_path, 'r', encoding='utf-8') as f:
            next(f)  # Skip hospital metadata
            next(f)
            charge_header = next(csv.reader(f))
        
        self.column_mapping = self.discover_columns(charge_header)
        
        print("Column mapping discovered:")
        for key, val in self.column_mapping.items():
            if isinstance(val, list) and len(val) > 3:
                print(f"  {key}: {val[:3]}... ({len(val)} total)")
            else:
                print(f"  {key}: {val}")
        
        # Read and filter in chunks
        print("\nReading and filtering CSV...")
        chunks = pandas.read_csv(file_path, skiprows=2, encoding="latin1", 
                            chunksize=100000, dtype=str, low_memory=False)
        filtered_chunks = []

        total_rows = 0
        kept_rows = 0
        
        for chunk_num, chunk in enumerate(chunks, 1):
            total_rows += len(chunk)
            
            # Add columns to store matched code and type for later use
            chunk['_matched_code'] = None
            chunk['_matched_type'] = None
            
            # Normalize code types across all code columns
            for type_col in self.column_mapping['type_columns']:
                if type_col in chunk.columns:
                    chunk[type_col] = chunk[type_col].apply(self.normalize_code_type)
            
            # Normalize setting
            if self.column_mapping['setting'] in chunk.columns:
                chunk[self.column_mapping['setting']] = chunk[self.column_mapping['setting']].apply(self.normalize_setting)
            
            # Build filter condition and capture matched code/type
            filter_condition = pandas.Series([False] * len(chunk), index=chunk.index)

            for code_col, type_col in zip(self.column_mapping['code_columns'], 
                                            self.column_mapping['type_columns']):
                if code_col not in chunk.columns or type_col not in chunk.columns:
                    continue
                
                # Normalize codes (uppercase, strip)
                chunk[code_col] = chunk[code_col].astype(str).str.upper().str.strip()
                
                # Check if type is allowed
                valid_type = chunk[type_col].isin(self.ALLOWED_TYPES)
                
                # For DRG types, all codes allowed
                is_drg = chunk[type_col].isin(['MS-DRG', 'APR-DRG'])
                
                # For CPT/HCPCS, only whitelist codes allowed
                is_cpt_hcpcs = chunk[type_col].isin(['CPT', 'HCPCS'])
                code_in_whitelist = chunk[code_col].isin(self.ALLOWED_CPT_HCPCS_CODES)
                
                # Combine: valid_type AND (DRG OR (CPT/HCPCS AND in_whitelist))
                matches = valid_type & (is_drg | (is_cpt_hcpcs & code_in_whitelist))
                
                # Only update rows that haven't been matched yet
                unmatched = chunk['_matched_code'].isna()
                newly_matched = matches & unmatched
                
                # Store the matched code and type - iterate through matched indices
                for idx in chunk[newly_matched].index:
                    chunk.at[idx, '_matched_code'] = chunk.at[idx, code_col]
                    chunk.at[idx, '_matched_type'] = chunk.at[idx, type_col]
                
                filter_condition = filter_condition | matches
            
            filtered_chunk = chunk[filter_condition]
            kept_rows += len(filtered_chunk)
            
            if len(filtered_chunk) > 0:
                # Handle "Both" settings by duplicating rows
                setting_col = self.column_mapping['setting']
                both_mask = filtered_chunk[setting_col] == 'Both'
                
                if both_mask.any():
                    # Split into "Both" and non-"Both" rows
                    both_rows = filtered_chunk[both_mask].copy()
                    normal_rows = filtered_chunk[~both_mask].copy()
                    
                    # Create two copies of "Both" rows - one for each setting
                    inpatient_rows = both_rows.copy()
                    inpatient_rows[setting_col] = 'Inpatient'
                    
                    outpatient_rows = both_rows.copy()
                    outpatient_rows[setting_col] = 'Outpatient'
                    
                    # Combine all rows
                    filtered_chunk = pandas.concat([normal_rows, inpatient_rows, outpatient_rows], ignore_index=True)
                    
                    print(f"    Duplicated {both_mask.sum()} 'Both' records into Inpatient/Outpatient")
                
                filtered_chunks.append(filtered_chunk)
            
            print(f"  Chunk {chunk_num}: {len(chunk)} rows -> {len(filtered_chunk)} kept")
        
        print(f"\nTotal: {total_rows} rows -> {kept_rows} kept ({100*kept_rows/total_rows:.1f}%)")
        
        if not filtered_chunks:
            return pandas.DataFrame()
        
        chargeData = pandas.concat(filtered_chunks, ignore_index=True)
        return chargeData.where(chargeData.notnull(), None)
    
    def arrange_charge_data(self, chargeData: pandas.DataFrame):
        """Process charge data with flexible column mapping"""
        
        start_time = time.time()
        
        total_services_inserted = 0
        total_standard_charges_inserted = 0
        total_payer_charges_inserted = 0
        
        with psycopg.connect(self.db_connection_str) as conn:
            with conn.cursor() as cur:
                num_records = 0
                batch_start = time.time()
                
                services_batch = []
                standard_charges_batch = []
                payer_charges_batch = []
                
                for _, row in chargeData.iterrows():
                    setting = row[self.column_mapping['setting']]
                    description = row[self.column_mapping['description']]
                    
                    # Use the pre-matched code and type from filtering
                    code = row['_matched_code']
                    code_type = row['_matched_type']
                    
                    if not code or not code_type:
                        continue  # Should rarely happen since we filtered already

                    service_id = sha256(f"{setting}|{code}|{code_type}".encode()).hexdigest()

                    services_batch.append((service_id, setting, code, description, code_type))
                    
                    # Get standard charges (using column mapping)
                    gross = row[self.column_mapping['gross']] if self.column_mapping['gross'] else None
                    discounted = row[self.column_mapping['discounted_cash']] if self.column_mapping['discounted_cash'] else None
                    min_charge = row[self.column_mapping['min']] if self.column_mapping['min'] else None
                    max_charge = row[self.column_mapping['max']] if self.column_mapping['max'] else None
                    
                    standard_charges_batch.append((
                        service_id, 
                        self.hospital_license_number, 
                        gross, discounted, min_charge, max_charge
                    ))
                    
                    # Get payer charges (using column mapping)
                    payer_name = row[self.column_mapping['payer_name']] if self.column_mapping['payer_name'] else None
                    plan_name = row[self.column_mapping['plan_name']] if self.column_mapping['plan_name'] else None
                    modifiers = row[self.column_mapping['modifiers']] if self.column_mapping['modifiers'] else None
                    negotiated_dollar = row[self.column_mapping['negotiated_dollar']] if self.column_mapping['negotiated_dollar'] else None
                    negotiated_algorithm = row[self.column_mapping['negotiated_algorithm']] if self.column_mapping['negotiated_algorithm'] else None
                    negotiated_percentage = row[self.column_mapping['negotiated_percentage']] if self.column_mapping['negotiated_percentage'] else None
                    estimated_amount = row[self.column_mapping['estimated_amount']] if self.column_mapping['estimated_amount'] else None
                    methodology = row[self.column_mapping['methodology']] if self.column_mapping['methodology'] else None
                    additional_notes = row[self.column_mapping['additional_notes']] if self.column_mapping['additional_notes'] else None
                    
                    payer_charges_batch.append((
                        service_id,
                        self.hospital_license_number, 
                        payer_name, plan_name, modifiers,
                        negotiated_dollar, negotiated_algorithm, negotiated_percentage,
                        estimated_amount, methodology, additional_notes
                    ))

                    num_records += 1
                    
                    if num_records % 5000 == 0:
                        batch_counts = self.execute_batch_upserts(cur, services_batch, standard_charges_batch, payer_charges_batch)
                        total_services_inserted += batch_counts[0]
                        total_standard_charges_inserted += batch_counts[1]
                        total_payer_charges_inserted += batch_counts[2]
                        
                        batch_end = time.time()
                        batch_time = batch_end - batch_start
                        total_time = batch_end - start_time
                        avg_time_per_record = total_time / num_records
                        
                        print(f"Processed {num_records} records in {total_time:.2f}s "
                              f"(batch: {batch_time:.2f}s, avg: {avg_time_per_record*1000:.2f}ms/record)")
                        print(f"  Actual inserts - Services: {total_services_inserted}, "
                              f"Standard: {total_standard_charges_inserted}, "
                              f"Payer: {total_payer_charges_inserted}")
                        
                        services_batch = []
                        standard_charges_batch = []
                        payer_charges_batch = []
                        batch_start = time.time()
                
                if services_batch:
                    batch_counts = self.execute_batch_upserts(cur, services_batch, standard_charges_batch, payer_charges_batch)
                    total_services_inserted += batch_counts[0]
                    total_standard_charges_inserted += batch_counts[1]
                    total_payer_charges_inserted += batch_counts[2]
                
                conn.commit()
                
                end_time = time.time()
                total_time = end_time - start_time
                print(f"\n=== Total Insertion Complete ===")
                print(f"Total records processed: {num_records}")
                print(f"Actual insertions:")
                print(f"  Services: {total_services_inserted} (conflicts: {num_records - total_services_inserted})")
                print(f"  Standard Charges: {total_standard_charges_inserted} (conflicts: {num_records - total_standard_charges_inserted})")
                print(f"  Payer Charges: {total_payer_charges_inserted} (conflicts: {num_records - total_payer_charges_inserted})")
                print(f"Total time: {total_time:.2f}s")
                print(f"Average time per record: {(total_time/num_records)*1000:.2f}ms")
                print(f"Records per second: {num_records/total_time:.2f}")

    def execute_batch_upserts(self, cur, services_batch, standard_charges_batch, payer_charges_batch):
        services_inserted = 0
        standard_charges_inserted = 0
        payer_charges_inserted = 0
        
        if services_batch:
            cur.executemany("""
                INSERT INTO services (service_id, setting, code, description, type)
                VALUES(%s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, services_batch)
            services_inserted = cur.rowcount
        
        if standard_charges_batch:
            cur.executemany("""
                INSERT INTO standard_charges (service_id, hospital_license_number, standard_charge_gross, standard_charge_discounted_cash, standard_charge_min, standard_charge_max)
                VALUES(%s, %s, %s, %s, %s, %s)
                ON CONFLICT (service_id, hospital_license_number)
                DO UPDATE SET 
                    standard_charge_gross = COALESCE(EXCLUDED.standard_charge_gross, standard_charges.standard_charge_gross),
                    standard_charge_discounted_cash = COALESCE(EXCLUDED.standard_charge_discounted_cash, standard_charges.standard_charge_discounted_cash),
                    standard_charge_min = COALESCE(EXCLUDED.standard_charge_min, standard_charges.standard_charge_min),
                    standard_charge_max = COALESCE(EXCLUDED.standard_charge_max, standard_charges.standard_charge_max)                            
            """, standard_charges_batch)
            standard_charges_inserted = cur.rowcount
        
        if payer_charges_batch:
            cur.executemany("""
                INSERT INTO payer_charges (service_id, hospital_license_number, payer_name, plan_name, modifiers, standard_charge_negotiated_dollar, standard_charge_negotiated_algorithm, standard_charge_negotiated_percent, estimated_amount, standard_charge_methodology, additional_generic_notes)
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (service_id, hospital_license_number, payer_name, plan_name)
                DO UPDATE SET
                    modifiers = EXCLUDED.modifiers,
                    standard_charge_negotiated_dollar = EXCLUDED.standard_charge_negotiated_dollar,
                    standard_charge_negotiated_algorithm = EXCLUDED.standard_charge_negotiated_algorithm,
                    standard_charge_negotiated_percent = EXCLUDED.standard_charge_negotiated_percent,
                    estimated_amount = EXCLUDED.estimated_amount,
                    standard_charge_methodology = EXCLUDED.standard_charge_methodology,
                    additional_generic_notes = EXCLUDED.additional_generic_notes
            """, payer_charges_batch)
            payer_charges_inserted = cur.rowcount
        
        return (services_inserted, standard_charges_inserted, payer_charges_inserted)


if __name__ == "__main__":
    print("Starting ETL process...")
    overall_start = time.time()

    db_connection_str = ""
    with open("../Credentials/cred.txt", "r") as f:
        db_connection_str = f.readline()
    
    etl = HospitalChargeETLCSV(db_connection_str)
    
    print("Reading hospital metadata...")
    hospital_dict = etl.read_hospital_data("../MachineReadableFiles/metrohealth-system_standardcharges.csv")
    
    print("Upserting hospital data...")
    etl.upsert_hospital_data(hospital_dict)
    
    print("Filtering services from CSV...")
    filter_start = time.time()
    df = etl.filter_services("../MachineReadableFiles/metrohealth-system_standardcharges.csv")    
    filter_time = time.time() - filter_start
    print(f"Filtered {len(df)} records in {filter_time:.2f}s")
    
    print("\nStarting charge data insertion...")
    etl.arrange_charge_data(df)
    
    overall_time = time.time() - overall_start
    print(f"\n=== ETL Process Complete ===")
    print(f"Total execution time: {overall_time:.2f}s ({overall_time/60:.2f} minutes)")
