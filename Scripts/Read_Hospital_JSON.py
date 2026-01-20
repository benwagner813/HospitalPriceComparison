from hashlib import sha256
import json
import logging
import time
import datetime
import psycopg

class HospitalChargeETLJSON:

    data: dict = {}
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
    
    ALLOWED_TYPES_VARIABLE = {'CPT', 'HCPCS'} # Not always allowed
    ALLOWED_TYPES = {'MS-DRG', 'APR-DRG'}

    def __init__(self, db_connection_str: str, file_path: str):
        logging.basicConfig(filename="../Logs/ETLLogs.log", level=logging.INFO)
        self.logger = logging.getLogger("ETL Logger")
        self.file_path = file_path
        self.db_connection_str = db_connection_str

        self.npis = None
        self.hospital_name = None


    def execute(self):

        self.logger.info("="*70)
        self.logger.info("HOSPITAL CHARGE ETL PROCESS")
        self.logger.info("="*70)
        self.logger.info(f"File: {self.file_path}")

        overall_start = time.time()

        with open(self.file_path, "r", encoding="utf-8-sig") as fp:
            self.data = json.load(fp)

        self.logger.info("STEP 1: Loading and inserting hospital metadata")
        hospital_data = self._extract_hospital_data()
        self._load_hospital_data(hospital_data)

        self.logger.info("STEP 2: Loading and inserting charge data")
        total_rows_inserted = self._extract_charge_data()

        overall_time = time.time() - overall_start
        self.logger.info("\n" + "="*70)
        self.logger.info("ETL PROCESS COMPLETE")
        self.logger.info("="*70)
        self.logger.info(f"Total execution time: {overall_time:.2f}s ({overall_time/60:.2f} minutes)")
        self.logger.info(f"Hospital: {self.hospital_name}")
        self.logger.info(f"Records inserted: {total_rows_inserted:,}\n\n\n")

        return {
            'status': 'success',
            'execution_time': overall_time,
            'hospital_license_number': self.hospital_name,
        }


    def _extract_hospital_data(self):
        hospital_name: str | None = self.data.get("hospital_name")
        self.hospital_name = hospital_name

        locations_list: list | None = self.data.get("location_name")
        if locations_list is None: # Handle legacy location key
            locations_list = self.data.get("hospital_location")

        locations = None
        if locations_list is not None:
            locations = "|".join(location for location in locations_list) # type: ignore

        addresses_list: list | None = self.data.get("hospital_address")
        addresses = None
        if addresses_list is not None:
            addresses = "|".join(address for address in addresses_list)

        license_information: dict | None = self.data.get("license_information")
        as_of_date: datetime.date = datetime.date.today()
        last_update: datetime.date | None = self.data.get("last_updated_on")
        version: str | None = self.data.get("version")  
        npis: list | None = self.data.get("type_2_npi")
        financial_aid_policy: list | str | None = self.data.get("financial_aid_policy")
        if isinstance(financial_aid_policy, list):
            financial_aid_policy = "|".join(str(x) for x in financial_aid_policy if x)

        hospital_license_number = None

        if license_information is not None:
            hospital_license_number = license_information.get("license_number") 
            hospital_license_number += "|" + license_information.get("state")

        if npis is not None:
            result = "|".join(npi for npi in npis)
            self.npis = result
                
        return (hospital_name, hospital_license_number, self.npis, locations, addresses, as_of_date, last_update, version, financial_aid_policy)

    def _load_hospital_data(self, hospital_data):
        with psycopg.connect(self.db_connection_str) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    DELETE FROM standard_charges
                    WHERE hospital_name = (%s)
                """, (self.hospital_name,))
                cur.execute("""
                    DELETE FROM payer_charges
                    WHERE hospital_name = (%s)
                """, (self.hospital_name,))
                cur.execute("""
                    INSERT INTO HOSPITALS (hospital_name, hospital_license_number, hospital_national_provider_identifiers, hospital_location, hospital_address, as_of_date, last_update, version, financial_aid_policy)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (hospital_name)
                    DO UPDATE SET
                        hospital_license_number = EXCLUDED.hospital_license_number,
                        hospital_national_provider_identifiers = EXCLUDED.hospital_national_provider_identifiers, 
                        hospital_address = EXCLUDED.hospital_address,
                        hospital_location = EXCLUDED.hospital_location,
                        as_of_date = EXCLUDED.as_of_date,
                        last_update = EXCLUDED.last_update,
                        version = EXCLUDED.version,
                        financial_aid_policy = EXCLUDED.financial_aid_policy
                """, hospital_data)

    def _extract_charge_data(self):
    
        start_time = time.time()

        total_services_inserted = 0
        total_standard_charges_inserted = 0
        total_payer_charges_inserted = 0

        with psycopg.connect(self.db_connection_str) as conn:
            with conn.cursor() as cur:
                standard_charges: list = self.data["standard_charge_information"]

                services_batch = []
                standard_charges_batch = []
                payer_charges_batch = []
                num_records = 0
                batch_start = time.time()

                for standard_charge in standard_charges:
                    relevant_code = self._relevant_code(standard_charge)
                    if relevant_code is None:
                        continue
                    
                    code_type = relevant_code[0]
                    code = relevant_code[1]
                    description = standard_charge["description"]

                    charge_data: list = standard_charge["standard_charges"]

                    for charge in charge_data:
                        setting = charge["setting"]
                        setting = setting.capitalize()

                        setting1 = "Inpatient"
                        setting2 = "Outpatient"

                        modifiers = charge.get("modifier_code")
                        
                        service_id1 = sha256(f"{setting1}|{code}|{code_type}|{modifiers}".encode()).hexdigest()
                        service_id2 = sha256(f"{setting2}|{code}|{code_type}|{modifiers}".encode()).hexdigest()
                        service_id = sha256(f"{setting}|{code}|{code_type}|{modifiers}".encode()).hexdigest()
                        
                        if setting == "Both":
                            services_batch.append((service_id1, setting1, code, description, code_type, modifiers))
                            services_batch.append((service_id2, setting2, code, description, code_type, modifiers))
                        else:
                            services_batch.append((service_id, setting, code, description, code_type, modifiers))

                        discounted_cash = charge.get("discounted_cash")
                        minimum = charge.get("minimum")
                        maximum = charge.get("maximum")
                        gross = charge.get("gross_charge")
                        additional_generic_notes = charge.get("additional_generic_notes")
                        
                        if setting == "Both":
                            standard_charges_batch.append((service_id1, self.hospital_name, gross, discounted_cash, minimum, maximum))
                            standard_charges_batch.append((service_id2, self.hospital_name, gross, discounted_cash, minimum, maximum))
                        else:    
                            standard_charges_batch.append((service_id, self.hospital_name, gross, discounted_cash, minimum, maximum))

                        payers_information = charge.get("payers_information")

                        if payers_information is not None:
                            for payer in payers_information:
                                payer_name = payer["payer_name"]
                                plan_name = payer["plan_name"]
                                standard_charge_negotiated_dollar = payer.get("standard_charge_dollar")
                                standard_charge_negotiated_percent = payer.get("standard_charge_percent")
                                standard_charge_negotiated_algorithm = payer.get("standard_charge_algorithm")
                                estimated_amount = payer.get("estimated_amount")
                                median = payer.get("median_amount")
                                tenth_percentile = payer.get("10th_percentile")
                                ninetyth_percentile = payer.get("90th_percentile")
                                count = payer.get("count")
                                standard_charge_negotiated_methodology = payer.get("methodology")
                                if setting == "Both":
                                    payer_charges_batch.append((service_id1, self.hospital_name, payer_name, plan_name, 
                                                            standard_charge_negotiated_dollar, standard_charge_negotiated_algorithm, standard_charge_negotiated_percent,
                                                            estimated_amount, standard_charge_negotiated_methodology, additional_generic_notes, median, tenth_percentile, 
                                                            ninetyth_percentile, count))
                                    payer_charges_batch.append((service_id2, self.hospital_name, payer_name, plan_name, 
                                                            standard_charge_negotiated_dollar, standard_charge_negotiated_algorithm, standard_charge_negotiated_percent,
                                                            estimated_amount, standard_charge_negotiated_methodology, additional_generic_notes, median, tenth_percentile, 
                                                            ninetyth_percentile, count))
                                else:
                                    payer_charges_batch.append((service_id, self.hospital_name, payer_name, plan_name, 
                                                            standard_charge_negotiated_dollar, standard_charge_negotiated_algorithm, standard_charge_negotiated_percent,
                                                            estimated_amount, standard_charge_negotiated_methodology, additional_generic_notes, median, tenth_percentile, 
                                                            ninetyth_percentile, count))
                                
                                num_records += 1
                            
                                if num_records % 5000 == 0:
                                    batch_counts = self._load_charge_batch_data(cur, services_batch, standard_charges_batch, payer_charges_batch)
                                    total_services_inserted += batch_counts[0]
                                    total_standard_charges_inserted += batch_counts[1]
                                    total_payer_charges_inserted += batch_counts[2]

                                    services_batch = []
                                    standard_charges_batch = []
                                    payer_charges_batch = []

                                    batch_end = time.time()
                                    batch_time = batch_end - batch_start
                                    total_time = batch_end - start_time
                                    avg_time_per_record = total_time / num_records
                                    self.logger.info(f"Processed {num_records:,} records in {total_time:.2f}s "
                                            f"(batch: {batch_time:.2f}s, avg: {avg_time_per_record*1000:.2f}ms/record)")
                                    batch_start = time.time()
                if services_batch:
                    batch_counts = self._load_charge_batch_data(cur, services_batch, standard_charges_batch, payer_charges_batch)
                    total_services_inserted += batch_counts[0]
                    total_standard_charges_inserted += batch_counts[1]
                    total_payer_charges_inserted += batch_counts[2]

                end_time = time.time()
                total_time = end_time - start_time
                self.logger.info(f"\n=== Insertion Complete ===")
                self.logger.info(f"Total records processed: {num_records:,}")
                self.logger.info(f"Actual insertions:")
                self.logger.info(f"  Services: {total_services_inserted:,}")
                self.logger.info(f"  Standard Charges: {total_standard_charges_inserted:,}")
                self.logger.info(f"  Payer Charges: {total_payer_charges_inserted:,}")
                self.logger.info(f"Total time: {total_time:.2f}s")
                self.logger.info(f"Records per second: {num_records/total_time:.2f}")
                return num_records

    def _relevant_code(self, standard_charge) -> tuple[str, str] | None:
        for code in standard_charge["code_information"]:
            if code["type"] in self.ALLOWED_TYPES or code["type"] in self.ALLOWED_TYPES_VARIABLE and code["code"] in self.ALLOWED_CPT_HCPCS_CODES:
                return (code["type"], code["code"])
        return None
    
    def _load_charge_batch_data(self, cur, services_batch, standard_charges_batch, payer_charges_batch):
        services_inserted = 0
        standard_charges_inserted = 0
        payer_charges_inserted = 0
        
        if services_batch:
            # First, check how many would conflict
            service_ids = [s[0] for s in services_batch]
            cur.execute("""
                SELECT service_id FROM services WHERE service_id = ANY(%s)
            """, (service_ids,))
            existing_services = {row[0] for row in cur.fetchall()}
            
            self.logger.info(f"Services batch: {len(services_batch)} total, {len(existing_services)} conflicts")
            
            cur.executemany("""
                INSERT INTO services (service_id, setting, code, description, type, modifiers)
                VALUES(%s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, services_batch)
            services_inserted = cur.rowcount
        
        if standard_charges_batch:
            # Check for conflicts
            keys = [(s[0], s[1]) for s in standard_charges_batch]  # service_id, hospital_name
            cur.execute("""
                SELECT service_id, hospital_name 
                FROM standard_charges 
                WHERE (service_id, hospital_name) IN (SELECT unnest(%s::text[]), unnest(%s::text[]))
            """, ([k[0] for k in keys], [k[1] for k in keys]))
            existing_standard = cur.rowcount
            
            self.logger.info(f"Standard charges batch: {len(standard_charges_batch)} total, {existing_standard} conflicts")
            
            cur.executemany("""
                INSERT INTO standard_charges (service_id, hospital_name, standard_charge_gross, standard_charge_discounted_cash, standard_charge_min, standard_charge_max)
                VALUES(%s, %s, %s, %s, %s, %s)
                ON CONFLICT (service_id, hospital_name)
                DO UPDATE SET 
                    standard_charge_gross = COALESCE(EXCLUDED.standard_charge_gross, standard_charges.standard_charge_gross),
                    standard_charge_discounted_cash = COALESCE(EXCLUDED.standard_charge_discounted_cash, standard_charges.standard_charge_discounted_cash),
                    standard_charge_min = COALESCE(EXCLUDED.standard_charge_min, standard_charges.standard_charge_min),
                    standard_charge_max = COALESCE(EXCLUDED.standard_charge_max, standard_charges.standard_charge_max)                            
            """, standard_charges_batch)
            standard_charges_inserted = cur.rowcount
        
        if payer_charges_batch:
            # Check for conflicts
            keys = [(p[0], p[1], p[2], p[3]) for p in payer_charges_batch]  # service_id, hospital_name, payer_name, plan_name
            
            # Sample a few to see what's conflicting
            if keys:
                sample_keys = keys[:5]  # Look at first 5
                for key in sample_keys:
                    cur.execute("""
                        SELECT COUNT(*) FROM payer_charges 
                        WHERE service_id = %s AND hospital_name = %s AND payer_name = %s AND plan_name = %s
                    """, key)
                    if cur.fetchone()[0] > 0:
                        self.logger.info(f"CONFLICT EXAMPLE: service_id={key[0]}, hospital={key[1]}, payer={key[2]}, plan={key[3]}")
            
            self.logger.info(f"Payer charges batch: {len(payer_charges_batch)} total")
            
            cur.executemany("""
                INSERT INTO payer_charges (service_id, hospital_name, payer_name, plan_name, standard_charge_negotiated_dollar, standard_charge_negotiated_algorithm, standard_charge_negotiated_percent, estimated_amount, standard_charge_methodology, additional_generic_notes, median_amount, tenth_percentile_amount, ninetieth_percentile_amount, count_amounts)
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (service_id, hospital_name, payer_name, plan_name)
                DO UPDATE SET
                    standard_charge_negotiated_dollar = EXCLUDED.standard_charge_negotiated_dollar,
                    standard_charge_negotiated_algorithm = EXCLUDED.standard_charge_negotiated_algorithm,
                    standard_charge_negotiated_percent = EXCLUDED.standard_charge_negotiated_percent,
                    estimated_amount = EXCLUDED.estimated_amount,
                    standard_charge_methodology = EXCLUDED.standard_charge_methodology,
                    additional_generic_notes = EXCLUDED.additional_generic_notes,
                    median_amount = EXCLUDED.median_amount,
                    tenth_percentile_amount = EXCLUDED.tenth_percentile_amount,
                    ninetieth_percentile_amount = EXCLUDED.ninetieth_percentile_amount,
                    count_amounts = EXCLUDED.count_amounts
            """, payer_charges_batch)
            payer_charges_inserted = cur.rowcount
        
        return (services_inserted, standard_charges_inserted, payer_charges_inserted)

if __name__ == "__main__":
    print("Starting ETL process...")
    overall_start = time.time()

    db_connection_str = ""
    with open("../Credentials/cred.txt", "r") as f:
        db_connection_str = f.readline()
    
    file_path = "../MachineReadableFiles/310564121_1053339507_dayton-osteopathic-hospital_standardcharges.json"

    etl = HospitalChargeETLJSON(db_connection_str, file_path)
    result = etl.execute()
    print(result)
    