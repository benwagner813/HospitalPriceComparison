BEGIN;

CREATE TYPE setting_enum AS ENUM ('Inpatient', 'Outpatient');

CREATE TYPE service_type_enum AS ENUM ('CPT', 'HCPCS', 'MS-DRG', 'APR-DRG');

CREATE TABLE "services" (
  "service_id" varchar(64) PRIMARY KEY,
  "setting" setting_enum,
  "code" text,
  "description" text,
  "type" service_type_enum,
  "modifiers" text,
  UNIQUE ("setting", "code", "type", "modifiers")
);

CREATE TABLE "hospitals" (
  "hospital_name" text PRIMARY KEY,
  "hospital_license_number" text,
  "hospital_national_provider_identifiers" text,
  "hospital_address" text,
  "hospital_location" text,
  "as_of_date" date,
  "last_update" date,
  "version" text,
  "financial_aid_policy" text
);

CREATE TABLE "standard_charges" (
  "service_id" varchar(64) REFERENCES services(service_id),
  "hospital_name" text REFERENCES hospitals(hospital_name),
  "standard_charge_gross" numeric(12, 2),
  "standard_charge_discounted_cash" numeric(12, 2),
  "standard_charge_min" numeric(12, 2),
  "standard_charge_max" numeric(12, 2),
  PRIMARY KEY ("service_id", "hospital_name")
);

CREATE TABLE "payer_charges" (
  "service_id" varchar(64) REFERENCES services(service_id),
  "hospital_name" text REFERENCES hospitals(hospital_name),
  "payer_name" text,
  "plan_name" text,
  "standard_charge_negotiated_dollar" numeric(12, 2),
  "standard_charge_negotiated_algorithm" text,
  "standard_charge_negotiated_percent" float,
  "estimated_amount" numeric(12, 2),
  "median_amount" numeric(12,2),
  "tenth_percentile_amount" numeric(12,2),
  "ninetieth_percentile_amount" numeric(12,2),
  "count_amounts" text,
  "standard_charge_methodology" text,
  "additional_generic_notes" text,
  PRIMARY KEY ("service_id", "hospital_name", "payer_name", "plan_name")
);


GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE services TO appuser;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE standard_charges TO appuser;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE payer_charges TO appuser;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE hospitals TO appuser;

COMMIT;