from hashlib import sha256
import json
import logging
import time
import datetime

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
        self.file_path = file_path
        self.db_connection_str = db_connection_str

        self.npis = None


    def execute(self):
        with open(self.file_path, "r") as fp:
            self.data = json.load(fp)

        hospital_data = self._extract_hospital_data()
        self._load_hospital_data(hospital_data)

    def _extract_hospital_data(self):
        hospital_name: str | None = self.data.get("hospital_name")

        locations: list | None = self.data.get("location_name")
        addresses: list | None = self.data.get("hospital_address")
        license_information: dict | None = self.data.get("license_information")
        as_of_date: datetime.date = datetime.date.today()
        last_update: datetime.date | None = self.data.get("last_updated_on")
        version: str | None = self.data.get("version")
        npis: list | None = self.data.get("type_2_npi")
        financial_aid_policy: str | None = self.data.get("financial_aid_policy")

        if npis is not None:
            result = "|".join(npi for npi in npis)
            self.npis = result
                

        if locations is None: # Handle legacy location key
            locations: list | None = self.data.get("hospital_location")

        return (hospital_name, locations, addresses, license_information, as_of_date, last_update, version, npis, financial_aid_policy)

    def _load_hospital_data(self, hospital_data):
        print(hospital_data)

    def _extract_charge_data(self):

        ## CREATE THE DATABASE CONNECTION FIRST

        standard_charges: list = self.data["standard_charge_information"]

        services_batch = []
        standard_charges_batch = []
        payer_charges_batch = []

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

                service_id = sha256(f"{setting}|{code}|{code_type}".encode()).hexdigest()
                services_batch.append((service_id, setting, code, description, code_type)) # need to add hospital id later

                discounted_cash = charge.get("discounted_cash")
                minimum = charge.get("minimum")
                maximum = charge.get("maximum")
                gross = charge.get("gross_charge")
                
                standard_charges_batch.append((service_id, gross, discounted_cash, minimum, maximum)) # need to add hospital id later

                payers_information = charge.get("payers_information")

                if payers_information is not None:
                    for payer in payers_information:
                        payer_name = payer["payer_name"]
                        plan_name = payer["plan_name"]
                        standard_charge_negotiated_dollar = payer.get("standard_charge_dollar")
                        standard_charge_negotiated_percent = payer.get("standard_charge_percent")
                        standard_charge_negotiated_algorithm = payer.get("standard_charge_algorithm")
                        median = payer.get("median_amount")
                        tenth_percentile = payer.get("10th_percentile")
                        ninetyth_percentile = payer.get("90th_percentile")
                        count = payer.get("count")
                        standard_charge_negotiated_methodology = payer.get("methodology")

                        payer_charges_batch.append((service_id, payer_name, plan_name, standard_charge_negotiated_dollar, standard_charge_negotiated_algorithm, 
                                                    standard_charge_negotiated_percent, median, tenth_percentile, ninetyth_percentile, count, standard_charge_negotiated_methodology))


    def _relevant_code(self, standard_charge) -> tuple[str, str] | None:
        for code in standard_charge["code_information"]:
            if code["type"] in self.ALLOWED_TYPES or code["type"] in self.ALLOWED_TYPES_VARIABLE and code["code"] in self.ALLOWED_CPT_HCPCS_CODES:
                return (code["type"], code["code"])
        return None
    
    def _load_charge_batch_data(self, cur, services_batch, standard_charges_batch, payer_charges_batch):
        
        
        return

if __name__ == "__main__":
    print("Starting ETL process...")
    overall_start = time.time()

    db_connection_str = ""
    with open("../Credentials/cred.txt", "r") as f:
        db_connection_str = f.readline()
    
    file_path = "../MachineReadableFiles/standard.json"

    etl = HospitalChargeETLJSON(db_connection_str, file_path)
    result = etl.execute()
    print(result)
    