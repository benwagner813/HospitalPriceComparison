BEGIN;

CREATE TYPE setting_enum AS ENUM ('Inpatient', 'Outpatient');

CREATE TYPE service_type_enum AS ENUM ('CPT', 'HCPCS', 'MS-DRG', 'APR-DRG');

CREATE TABLE "services" (
  "service_id" varchar(64) PRIMARY KEY,
  "setting" setting_enum,
  "code" text,
  "description" text,
  "type" service_type_enum,
  UNIQUE ("setting", "code", "type")
);

CREATE TABLE "hospitals" (
  "hospital_license_number" text PRIMARY KEY,
  "hospital_name" text,
  "hospital_address" text,
  "hospital_location" text,
  "as_of_date" date,
  "last_update" date,
  "version" text
);

CREATE TABLE "standard_charges" (
  "service_id" varchar(64) REFERENCES services(service_id),
  "hospital_license_number" text REFERENCES hospitals(hospital_license_number),
  "standard_charge_gross" numeric(12, 2),
  "standard_charge_discounted_cash" numeric(12, 2),
  "standard_charge_min" numeric(12, 2),
  "standard_charge_max" numeric(12, 2),
  PRIMARY KEY ("service_id", "hospital_license_number")
);

CREATE TABLE "payer_charges" (
  "service_id" varchar(64) REFERENCES services(service_id),
  "hospital_license_number" text REFERENCES hospitals(hospital_license_number),
  "payer_name" text,
  "plan_name" text,
  "modifiers" text,
  "standard_charge_negotiated_dollar" numeric(12, 2),
  "standard_charge_negotiated_algorithm" text,
  "standard_charge_negotiated_percent" float,
  "estimated_amount" numeric(12, 2),
  "standard_charge_methodology" text,
  "additional_generic_notes" text,
  PRIMARY KEY ("service_id", "hospital_license_number", "payer_name", "plan_name")
);


GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE services TO appuser;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE standard_charges TO appuser;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE payer_charges TO appuser;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE hospitals TO appuser;

COMMIT;