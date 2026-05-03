#!/usr/bin/env python
# coding: utf-8

# ## Healthcare & Extended Types Data Generator
#
# Generates realistic test data covering 28+ PostgreSQL data types across 9 tables.
# Each run inserts new data AND updates ~30% of existing records.
#
# Tables (Healthcare — relational, FK-linked):
#   1. departments     — hospital departments
#   2. doctors         — physician directory
#   3. patients        — demographics
#   4. appointments    — patient-doctor visits
#   5. diagnoses       — diagnosis records per appointment
#   6. prescriptions   — medications prescribed per diagnosis
#
# Tables (Extended Types — wide, type-diverse):
#   7. data_type_showcase      — numeric, temporal, boolean, character (25 cols)
#   8. complex_types_showcase  — JSON, binary, arrays, network, geometry (18 cols)
#   9. edge_cases              — NULLs, boundaries, unicode, special values (28 cols)
#  10. audit_events            — composite PK (3 cols), TIMESTAMPTZ filter
#                                  (production-hardening test fixture)
#
# Configuration:
#   - Set DROP_AND_RECREATE = True to wipe all tables and start fresh
#   - Set DROP_AND_RECREATE = False (default) to append + update

# In[1]:

%run _common


# In[2]:

# ── Per-notebook overrides & volume knobs ────────────────────────────────────

# This notebook always targets the healthcare schema, regardless of the VL default.
cfg["pg_schema"] = "healthcare"

# Set to True to DROP all tables and recreate from scratch
DROP_AND_RECREATE = False

# Number of records per run (Healthcare)
NUM_PATIENTS = 200
NUM_DOCTORS = 30
NUM_DEPARTMENTS = 10
NUM_APPOINTMENTS = 500
NUM_DIAGNOSES = 600
NUM_PRESCRIPTIONS = 800

# Number of records per run (Extended Types)
NUM_SHOWCASE_ROWS = 500
NUM_COMPLEX_ROWS = 200
NUM_EDGE_CASE_ROWS = 100
NUM_AUDIT_EVENT_ROWS = 200

# Convenience aliases — the rest of the notebook references these names directly.
PG_HOST     = cfg["pg_host"]
PG_PORT     = cfg["pg_port"]
PG_DATABASE = cfg["pg_database"]
PG_SCHEMA   = cfg["pg_schema"]
PG_USER     = cfg["pg_user"]
PG_PASSWORD = pg_password()

JDBC_URL   = pg_jdbc_url()
JDBC_PROPS = pg_jdbc_props()

print(f"Target: {PG_HOST}:{PG_PORT}/{PG_DATABASE} (schema: {PG_SCHEMA})")
print(f"DROP_AND_RECREATE: {DROP_AND_RECREATE}")
print(f"Healthcare: {NUM_PATIENTS} patients, {NUM_DOCTORS} doctors, "
      f"{NUM_APPOINTMENTS} appointments, {NUM_DIAGNOSES} diagnoses, "
      f"{NUM_PRESCRIPTIONS} prescriptions")
print(f"Extended:   {NUM_SHOWCASE_ROWS} showcase, {NUM_COMPLEX_ROWS} complex, "
      f"{NUM_EDGE_CASE_ROWS} edge_cases, {NUM_AUDIT_EVENT_ROWS} audit_events")


# In[3]:

# ── DDL: Create (or drop+recreate) schema and tables ──────────────────────────

# Drop statements — reverse dependency order
DROP_STATEMENTS = f"""
DROP TABLE IF EXISTS {PG_SCHEMA}.audit_events CASCADE;
DROP TABLE IF EXISTS {PG_SCHEMA}.edge_cases CASCADE;
DROP TABLE IF EXISTS {PG_SCHEMA}.complex_types_showcase CASCADE;
DROP TABLE IF EXISTS {PG_SCHEMA}.data_type_showcase CASCADE;
DROP TABLE IF EXISTS {PG_SCHEMA}.prescriptions CASCADE;
DROP TABLE IF EXISTS {PG_SCHEMA}.diagnoses CASCADE;
DROP TABLE IF EXISTS {PG_SCHEMA}.appointments CASCADE;
DROP TABLE IF EXISTS {PG_SCHEMA}.patients CASCADE;
DROP TABLE IF EXISTS {PG_SCHEMA}.doctors CASCADE;
DROP TABLE IF EXISTS {PG_SCHEMA}.departments CASCADE;
"""

CREATE_STATEMENTS = f"""
CREATE SCHEMA IF NOT EXISTS {PG_SCHEMA};

CREATE TABLE IF NOT EXISTS {PG_SCHEMA}.departments (
    department_id   TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    floor           INT,
    head_doctor     TEXT,
    created_at      TIMESTAMP DEFAULT NOW(),
    last_updated    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS {PG_SCHEMA}.doctors (
    doctor_id       TEXT PRIMARY KEY,
    first_name      TEXT NOT NULL,
    last_name       TEXT NOT NULL,
    specialty       TEXT NOT NULL,
    department_id   TEXT REFERENCES {PG_SCHEMA}.departments(department_id),
    license_no      TEXT,
    phone           TEXT,
    created_at      TIMESTAMP DEFAULT NOW(),
    last_updated    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS {PG_SCHEMA}.patients (
    patient_id      TEXT PRIMARY KEY,
    first_name      TEXT NOT NULL,
    last_name       TEXT NOT NULL,
    date_of_birth   DATE NOT NULL,
    gender          TEXT,
    blood_type      TEXT,
    phone           TEXT,
    email           TEXT,
    address         TEXT,
    insurance_id    TEXT,
    created_at      TIMESTAMP DEFAULT NOW(),
    last_updated    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS {PG_SCHEMA}.appointments (
    appointment_id  TEXT PRIMARY KEY,
    patient_id      TEXT REFERENCES {PG_SCHEMA}.patients(patient_id),
    doctor_id       TEXT REFERENCES {PG_SCHEMA}.doctors(doctor_id),
    department_id   TEXT REFERENCES {PG_SCHEMA}.departments(department_id),
    appointment_date TIMESTAMP NOT NULL,
    status          TEXT DEFAULT 'scheduled',
    visit_type      TEXT,
    notes           TEXT,
    created_at      TIMESTAMP DEFAULT NOW(),
    last_updated    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS {PG_SCHEMA}.diagnoses (
    diagnosis_id    TEXT PRIMARY KEY,
    appointment_id  TEXT REFERENCES {PG_SCHEMA}.appointments(appointment_id),
    icd_code        TEXT NOT NULL,
    description     TEXT NOT NULL,
    severity        TEXT,
    diagnosed_at    TIMESTAMP DEFAULT NOW(),
    last_updated    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS {PG_SCHEMA}.prescriptions (
    prescription_id TEXT PRIMARY KEY,
    diagnosis_id    TEXT REFERENCES {PG_SCHEMA}.diagnoses(diagnosis_id),
    patient_id      TEXT REFERENCES {PG_SCHEMA}.patients(patient_id),
    doctor_id       TEXT REFERENCES {PG_SCHEMA}.doctors(doctor_id),
    medication      TEXT NOT NULL,
    dosage          TEXT,
    frequency       TEXT,
    duration_days   INT,
    prescribed_at   TIMESTAMP DEFAULT NOW(),
    last_updated    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS {PG_SCHEMA}.data_type_showcase (
    id                  BIGSERIAL PRIMARY KEY,
    record_uuid         UUID NOT NULL DEFAULT gen_random_uuid(),
    is_active           BOOLEAN NOT NULL DEFAULT TRUE,
    is_verified         BOOLEAN DEFAULT FALSE,
    tiny_val            SMALLINT,
    normal_val          INTEGER,
    big_val             BIGINT,
    float4_val          REAL,
    float8_val          DOUBLE PRECISION,
    price               NUMERIC(10,2),
    scientific_val      NUMERIC(20,10),
    exact_decimal       NUMERIC(38,18),
    code_fixed          CHAR(10),
    code_varying        VARCHAR(50),
    description         TEXT,
    created_date        DATE,
    created_ts          TIMESTAMP,
    created_tstz        TIMESTAMPTZ,
    event_time          TIME,
    event_time_tz       TIME WITH TIME ZONE,
    duration_interval   INTERVAL,
    last_updated        TIMESTAMP DEFAULT NOW(),
    created_at          TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS {PG_SCHEMA}.complex_types_showcase (
    id                  BIGSERIAL PRIMARY KEY,
    record_uuid         UUID NOT NULL DEFAULT gen_random_uuid(),
    metadata_jsonb      JSONB,
    raw_payload_json    JSON,
    file_hash           BYTEA,
    thumbnail           BYTEA,
    tags                TEXT[],
    scores              INTEGER[],
    measurements        DOUBLE PRECISION[],
    ip_address          INET,
    network_cidr        CIDR,
    mac_address         MACADDR,
    flags               BIT(8),
    permissions         BIT VARYING(32),
    search_vector       TSVECTOR,
    location_point      POINT,
    last_updated        TIMESTAMP DEFAULT NOW(),
    created_at          TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS {PG_SCHEMA}.edge_cases (
    id                  BIGSERIAL PRIMARY KEY,
    nullable_bool       BOOLEAN,
    nullable_int        INTEGER,
    nullable_bigint     BIGINT,
    nullable_float      DOUBLE PRECISION,
    nullable_decimal    NUMERIC(10,2),
    nullable_text       TEXT,
    nullable_date       DATE,
    nullable_ts         TIMESTAMP,
    nullable_jsonb      JSONB,
    nullable_array      TEXT[],
    nullable_bytea      BYTEA,
    max_int             INTEGER,
    min_int             INTEGER,
    max_bigint          BIGINT,
    zero_decimal        NUMERIC(10,5),
    negative_decimal    NUMERIC(10,5),
    empty_string        TEXT,
    whitespace_only     TEXT,
    unicode_text        TEXT,
    very_long_text      TEXT,
    newline_text        TEXT,
    empty_json_obj      JSONB,
    empty_json_arr      JSONB,
    nested_deep_json    JSONB,
    json_with_nulls     JSONB,
    epoch_ts            TIMESTAMP,
    far_future_ts       TIMESTAMP,
    last_updated        TIMESTAMP DEFAULT NOW(),
    created_at          TIMESTAMP DEFAULT NOW()
);

-- audit_events: production-hardening test fixture
--   • Composite PK (3 cols)        — exercises Finding #19 metadata fanout
--   • Multiple skip cols (4)       — exercises Finding #19 metadata fanout
--   • TIMESTAMPTZ filter column    — exercises Finding #21 timezone safety
--   • schema_drift_policy='fail'   — exercises Finding #20 (config in setup)
CREATE TABLE IF NOT EXISTS {PG_SCHEMA}.audit_events (
    tenant_id           TEXT NOT NULL,
    entity_id           TEXT NOT NULL,
    version             INTEGER NOT NULL,
    event_type          TEXT NOT NULL,
    payload             JSONB,
    actor               TEXT,
    -- Skip-from-comparison columns (pipeline metadata, not source-of-truth)
    etl_load_ts         TIMESTAMPTZ DEFAULT NOW(),
    etl_source          TEXT DEFAULT 'pg-direct',
    etl_batch_id        TEXT,
    created_at          TIMESTAMP DEFAULT NOW(),
    -- TIMESTAMPTZ filter column — explicit UTC anchoring tests Finding #21
    last_updated        TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (tenant_id, entity_id, version)
);
"""

def run_ddl(statements):
    """Run DDL via py4j JDBC connection."""
    spark._jvm.Class.forName("org.postgresql.Driver")
    conn = spark._jvm.java.sql.DriverManager.getConnection(JDBC_URL, PG_USER, PG_PASSWORD)
    try:
        conn.setAutoCommit(False)
        stmt = conn.createStatement()
        for sql in statements.split(";"):
            sql = sql.strip()
            if sql:
                stmt.execute(sql)
        conn.commit()
    finally:
        conn.close()

if DROP_AND_RECREATE:
    print("⚠️  Dropping all tables...")
    run_ddl(DROP_STATEMENTS)
    print("✅ All tables dropped")

print("Creating schema and tables (if not exist)...")
run_ddl(CREATE_STATEMENTS)
print("✅ Schema and tables created/verified")


# In[4]:

# ── Generate Healthcare data ──────────────────────────────────────────────────

import random
import uuid
import json
import os
import time
from datetime import datetime, timedelta, date
from decimal import Decimal
from pyspark.sql import Row
from pyspark.sql.types import *

BATCH_ID = datetime.utcnow().strftime("%Y%m%d%H%M%S")
random.seed(None)

def make_id(prefix):
    return f"{prefix}-{BATCH_ID}-{uuid.uuid4().hex[:8]}"

# ── Reference data ──────────────────────────────────────────────────────

DEPT_NAMES = [
    "Cardiology", "Neurology", "Orthopedics", "Pediatrics", "Oncology",
    "Emergency", "Dermatology", "Gastroenterology", "Pulmonology", "Psychiatry",
    "Radiology", "Urology", "Ophthalmology", "ENT", "General Surgery"
]

SPECIALTIES = {
    "Cardiology": ["Cardiologist", "Interventional Cardiologist"],
    "Neurology": ["Neurologist", "Neurosurgeon"],
    "Orthopedics": ["Orthopedic Surgeon", "Sports Medicine"],
    "Pediatrics": ["Pediatrician", "Neonatologist"],
    "Oncology": ["Medical Oncologist", "Radiation Oncologist"],
    "Emergency": ["Emergency Physician", "Trauma Surgeon"],
    "Dermatology": ["Dermatologist", "Cosmetic Dermatologist"],
    "Gastroenterology": ["Gastroenterologist", "Hepatologist"],
    "Pulmonology": ["Pulmonologist", "Sleep Specialist"],
    "Psychiatry": ["Psychiatrist", "Clinical Psychologist"],
    "Radiology": ["Radiologist", "Interventional Radiologist"],
    "Urology": ["Urologist"],
    "Ophthalmology": ["Ophthalmologist", "Retina Specialist"],
    "ENT": ["Otolaryngologist"],
    "General Surgery": ["General Surgeon", "Laparoscopic Surgeon"],
}

FIRST_NAMES = [
    "Aarav", "Vivaan", "Aditya", "Vihaan", "Arjun", "Sai", "Reyansh",
    "Ayaan", "Krishna", "Ishaan", "Ananya", "Diya", "Saanvi", "Aanya",
    "Aadhya", "Isha", "Myra", "Sara", "Priya", "Kavya", "James", "Mary",
    "Robert", "Patricia", "John", "Jennifer", "Michael", "Linda", "David",
    "Elizabeth", "Ahmed", "Fatima", "Omar", "Aisha", "Yusuf", "Zara",
]

LAST_NAMES = [
    "Sharma", "Patel", "Kumar", "Singh", "Reddy", "Gupta", "Joshi",
    "Mehta", "Shah", "Verma", "Iyer", "Nair", "Das", "Rao", "Pillai",
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Khan", "Ali", "Hassan", "Ibrahim",
]

BLOOD_TYPES = ["A+", "A-", "B+", "B-", "AB+", "AB-", "O+", "O-"]
GENDERS = ["Male", "Female", "Other"]
VISIT_TYPES = ["In-Person", "Telehealth", "Follow-up", "Emergency", "Routine Checkup"]
STATUSES = ["completed", "completed", "completed", "scheduled", "cancelled", "no-show"]
SEVERITIES = ["Mild", "Moderate", "Severe", "Critical"]

ICD_CODES = [
    ("I10", "Essential Hypertension"), ("E11.9", "Type 2 Diabetes Mellitus"),
    ("J06.9", "Acute Upper Respiratory Infection"), ("M54.5", "Low Back Pain"),
    ("K21.0", "Gastroesophageal Reflux Disease"), ("F32.9", "Major Depressive Disorder"),
    ("J45.909", "Unspecified Asthma"), ("E78.5", "Hyperlipidemia"),
    ("N39.0", "Urinary Tract Infection"), ("G43.909", "Migraine Unspecified"),
    ("I25.10", "Coronary Artery Disease"), ("E03.9", "Hypothyroidism"),
    ("J18.9", "Pneumonia"), ("K58.9", "Irritable Bowel Syndrome"),
    ("L30.9", "Dermatitis Unspecified"), ("R10.9", "Unspecified Abdominal Pain"),
    ("R51", "Headache"), ("J02.9", "Acute Pharyngitis"), ("B34.9", "Viral Infection"),
]

MEDICATIONS = [
    ("Amlodipine", "5mg", "Once daily"), ("Metformin", "500mg", "Twice daily"),
    ("Amoxicillin", "250mg", "Three times daily"), ("Ibuprofen", "400mg", "As needed"),
    ("Omeprazole", "20mg", "Once daily"), ("Sertraline", "50mg", "Once daily"),
    ("Albuterol Inhaler", "2 puffs", "As needed"), ("Atorvastatin", "10mg", "Once daily"),
    ("Ciprofloxacin", "500mg", "Twice daily"), ("Sumatriptan", "50mg", "As needed"),
    ("Aspirin", "81mg", "Once daily"), ("Levothyroxine", "25mcg", "Once daily"),
    ("Azithromycin", "500mg", "Once daily"), ("Lisinopril", "10mg", "Once daily"),
    ("Prednisone", "10mg", "Tapering dose"), ("Cetirizine", "10mg", "Once daily"),
    ("Pantoprazole", "40mg", "Once daily"), ("Metoprolol", "25mg", "Twice daily"),
    ("Losartan", "50mg", "Once daily"), ("Gabapentin", "300mg", "Three times daily"),
]

# ── Generate departments ──────────────────────────────────────────────

selected_depts = random.sample(DEPT_NAMES, NUM_DEPARTMENTS)
departments = []
for name in selected_depts:
    departments.append(Row(
        department_id=make_id("DEPT"),
        name=name,
        floor=random.randint(1, 8),
        head_doctor=f"Dr. {random.choice(LAST_NAMES)}",
    ))

dept_ids = [d.department_id for d in departments]
dept_name_to_id = {d.name: d.department_id for d in departments}

# ── Generate doctors ──────────────────────────────────────────────────

doctors = []
for _ in range(NUM_DOCTORS):
    dept_name = random.choice(selected_depts)
    spec = random.choice(SPECIALTIES.get(dept_name, ["General Practitioner"]))
    doctors.append(Row(
        doctor_id=make_id("DOC"),
        first_name=random.choice(FIRST_NAMES),
        last_name=random.choice(LAST_NAMES),
        specialty=spec,
        department_id=dept_name_to_id[dept_name],
        license_no=f"LIC-{random.randint(100000, 999999)}",
        phone=f"+91-{random.randint(7000000000, 9999999999)}",
    ))

doc_ids = [d.doctor_id for d in doctors]

# ── Generate patients ─────────────────────────────────────────────────

patients = []
for _ in range(NUM_PATIENTS):
    dob = datetime(1940, 1, 1) + timedelta(days=random.randint(0, 30000))
    patients.append(Row(
        patient_id=make_id("PAT"),
        first_name=random.choice(FIRST_NAMES),
        last_name=random.choice(LAST_NAMES),
        date_of_birth=dob.date(),
        gender=random.choice(GENDERS),
        blood_type=random.choice(BLOOD_TYPES),
        phone=f"+91-{random.randint(7000000000, 9999999999)}",
        email=f"patient_{uuid.uuid4().hex[:6]}@example.com",
        address=f"{random.randint(1,999)} {random.choice(['MG Road','Park Street','Lake View','Hill Top','Main St'])}, Bangalore",
        insurance_id=f"INS-{random.randint(100000, 999999)}" if random.random() > 0.2 else None,
    ))

pat_ids = [p.patient_id for p in patients]

# ── Generate appointments ─────────────────────────────────────────────

appointments = []
now = datetime.utcnow()
for _ in range(NUM_APPOINTMENTS):
    appt_date = now - timedelta(days=random.randint(0, 365), hours=random.randint(0, 12))
    doc = random.choice(doctors)
    appointments.append(Row(
        appointment_id=make_id("APPT"),
        patient_id=random.choice(pat_ids),
        doctor_id=doc.doctor_id,
        department_id=doc.department_id,
        appointment_date=appt_date,
        status=random.choice(STATUSES),
        visit_type=random.choice(VISIT_TYPES),
        notes=random.choice([
            None, "Follow-up required in 2 weeks",
            "Patient reports improvement", "Lab work ordered",
            "Referred to specialist", "Vitals normal",
            "Symptoms persisting", "New symptoms reported",
        ]),
    ))

appt_ids = [a.appointment_id for a in appointments]

# ── Generate diagnoses ────────────────────────────────────────────────

diagnoses = []
for _ in range(NUM_DIAGNOSES):
    icd_code, desc = random.choice(ICD_CODES)
    diagnoses.append(Row(
        diagnosis_id=make_id("DIAG"),
        appointment_id=random.choice(appt_ids),
        icd_code=icd_code,
        description=desc,
        severity=random.choice(SEVERITIES),
        diagnosed_at=now - timedelta(days=random.randint(0, 365)),
    ))

diag_ids = [d.diagnosis_id for d in diagnoses]

# ── Generate prescriptions ────────────────────────────────────────────

prescriptions = []
for _ in range(NUM_PRESCRIPTIONS):
    med, dosage, freq = random.choice(MEDICATIONS)
    diag = random.choice(diagnoses)
    appt = next((a for a in appointments if a.appointment_id == diag.appointment_id), None)
    prescriptions.append(Row(
        prescription_id=make_id("RX"),
        diagnosis_id=diag.diagnosis_id,
        patient_id=appt.patient_id if appt else random.choice(pat_ids),
        doctor_id=appt.doctor_id if appt else random.choice(doc_ids),
        medication=med,
        dosage=dosage,
        frequency=freq,
        duration_days=random.choice([5, 7, 10, 14, 21, 30, 60, 90]),
        prescribed_at=(now - timedelta(days=random.randint(0, 365))),
    ))

print(f"✅ Generated healthcare batch {BATCH_ID}:")
print(f"   {len(departments)} departments, {len(doctors)} doctors, {len(patients)} patients")
print(f"   {len(appointments)} appointments, {len(diagnoses)} diagnoses, {len(prescriptions)} prescriptions")


# In[5]:

# ── Generate Extended Types data ──────────────────────────────────────────────

def generate_showcase_rows(n):
    """Generate rows for data_type_showcase table."""
    rows = []
    now_ts = datetime.utcnow()
    for i in range(n):
        row_ts = now_ts - timedelta(hours=random.randint(0, 168))
        rows.append(Row(
            record_uuid=str(uuid.uuid4()),
            is_active=random.choice([True, False]),
            is_verified=random.choice([True, False, None]),
            tiny_val=random.randint(-32768, 32767),
            normal_val=random.randint(-2_000_000_000, 2_000_000_000),
            big_val=random.randint(-9_000_000_000_000, 9_000_000_000_000),
            float4_val=float(round(random.uniform(-1000.0, 1000.0), 4)),
            float8_val=random.uniform(-1e10, 1e10),
            price=Decimal(f"{random.uniform(0.01, 99999.99):.2f}"),
            scientific_val=Decimal(f"{random.uniform(-1e8, 1e8):.10f}"),
            exact_decimal=Decimal(f"{random.uniform(-1e12, 1e12):.18f}"),
            code_fixed=f"{''.join(random.choices('ABCDEFGHIJKLMNOP', k=10))}",
            code_varying=f"VAR-{uuid.uuid4().hex[:20]}",
            description=f"Row {i} batch {BATCH_ID}: café, naïve, ñ, ü, 日本語",
            created_date=date.today() - timedelta(days=random.randint(0, 365)),
            created_ts=row_ts,
            created_tstz=row_ts,
            event_time=f"{random.randint(0,23):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}",
            event_time_tz=f"{random.randint(0,23):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}+05:30",
            duration_interval=f"{random.randint(0,365)} days {random.randint(0,23):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}",
        ))
    return rows


def generate_complex_rows(n):
    """Generate rows for complex_types_showcase table."""
    rows = []
    now_ts = datetime.utcnow()
    tag_options = ["urgent", "reviewed", "pending", "archived", "flagged", "priority", "deferred"]
    for i in range(n):
        rows.append(Row(
            record_uuid=str(uuid.uuid4()),
            metadata_jsonb=json.dumps({
                "version": random.randint(1, 5),
                "environment": random.choice(["dev", "staging", "prod"]),
                "nested": {"key": f"val_{i}", "numbers": [1, 2, 3]},
                "enabled": random.choice([True, False]),
            }, sort_keys=True),
            raw_payload_json=json.dumps({
                "event": f"action_{i}", "ts": now_ts.isoformat(),
                "data": {"x": random.randint(1, 100)},
            }),
            file_hash=bytearray(os.urandom(32)),
            thumbnail=bytearray(os.urandom(64)),
            tags=random.sample(tag_options, k=random.randint(1, 4)),
            scores=[random.randint(0, 100) for _ in range(random.randint(3, 8))],
            measurements=[round(random.uniform(0, 100), 4) for _ in range(5)],
            ip_address=f"{random.randint(1,254)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}/24",
            network_cidr=f"10.{random.randint(0,255)}.0.0/16",
            mac_address=":".join(f"{random.randint(0,255):02x}" for _ in range(6)),
            flags=f"{''.join(str(random.randint(0,1)) for _ in range(8))}",
            permissions=f"{''.join(str(random.randint(0,1)) for _ in range(random.randint(8,32)))}",
            search_vector=f"'word{i}':1 'test':2 'data':3",
            location_point=f"({round(random.uniform(-180, 180), 6)},{round(random.uniform(-90, 90), 6)})",
        ))
    return rows


def generate_edge_case_rows(n):
    """Generate rows with boundary values, NULLs, and special content."""
    rows = []
    now_ts = datetime.utcnow()

    unicode_samples = [
        "Hello 🌍🎉🚀 World", "中文测试数据", "العربية", "Ñoño café naïve",
        "🏥💊🩺 Healthcare", "Tab\there\nnewline", "NULL",
        "''", '<script>alert("xss")</script>', "   leading and trailing spaces   ",
    ]

    for i in range(n):
        make_null = lambda val: val if random.random() > 0.4 else None

        if i < 10:
            max_int = 2147483647 if i % 2 == 0 else -2147483648
            min_int = -2147483648 if i % 2 == 0 else 2147483647
            max_bigint = 9223372036854775807 if i % 3 == 0 else -9223372036854775808
            zero_dec = Decimal("0.00000")
            neg_dec = Decimal("-99999.99999")
            epoch_ts = datetime(1970, 1, 1, 0, 0, 0)
            far_future = datetime(2099, 12, 31, 23, 59, 59)
            empty_str = ""
            ws_only = "   \t  "
            unicode_txt = unicode_samples[i % len(unicode_samples)]
            long_text = "X" * 10000
            nl_text = "line1\nline2\r\nline3\ttab"
        else:
            max_int = random.choice([2147483647, -2147483648, random.randint(-1000, 1000)])
            min_int = random.choice([2147483647, -2147483648, random.randint(-1000, 1000)])
            max_bigint = random.choice([9223372036854775807, -9223372036854775808, random.randint(-1000000, 1000000)])
            zero_dec = Decimal(f"{random.uniform(-0.001, 0.001):.5f}")
            neg_dec = Decimal(f"{random.uniform(-99999, -0.01):.5f}")
            epoch_ts = datetime(1970, 1, 1) + timedelta(seconds=random.randint(0, 86400))
            far_future = datetime(2099, 1, 1) + timedelta(days=random.randint(0, 365))
            empty_str = random.choice(["", " ", "a"])
            ws_only = random.choice(["   ", "\t", "  \t  ", " x "])
            unicode_txt = random.choice(unicode_samples)
            long_text = "Y" * random.randint(100, 5000)
            nl_text = f"line_{i}\nnext\ttab"

        rows.append(Row(
            nullable_bool=make_null(random.choice([True, False])),
            nullable_int=make_null(random.randint(-1000, 1000)),
            nullable_bigint=make_null(random.randint(-1000000, 1000000)),
            nullable_float=make_null(random.uniform(-100, 100)),
            nullable_decimal=make_null(Decimal(f"{random.uniform(-999, 999):.2f}")),
            nullable_text=make_null(f"text_{i}"),
            nullable_date=make_null(date.today() - timedelta(days=random.randint(0, 365))),
            nullable_ts=make_null(now_ts - timedelta(hours=random.randint(0, 720))),
            nullable_jsonb=make_null(json.dumps({"key": f"val_{i}"})),
            nullable_array=make_null(["a", "b", "c"]),
            nullable_bytea=make_null(bytearray(os.urandom(16))),
            max_int=max_int,
            min_int=min_int,
            max_bigint=max_bigint,
            zero_decimal=zero_dec,
            negative_decimal=neg_dec,
            empty_string=empty_str,
            whitespace_only=ws_only,
            unicode_text=unicode_txt,
            very_long_text=long_text,
            newline_text=nl_text,
            empty_json_obj=json.dumps({}),
            empty_json_arr=json.dumps([]),
            nested_deep_json=json.dumps({"l1": {"l2": {"l3": {"l4": {"l5": f"deep_{i}"}}}}}),
            json_with_nulls=json.dumps({"key": None, "other": "val"}),
            epoch_ts=epoch_ts,
            far_future_ts=far_future,
        ))
    return rows


def generate_audit_event_rows(n):
    """Generate rows for audit_events (composite PK + TIMESTAMPTZ).

    Designed to exercise:
      • Composite PK (tenant_id, entity_id, version)
      • TIMESTAMPTZ filter column with rows placed throughout the recent window
        (including a few near boundaries to expose timezone bugs)
    """
    from datetime import timezone
    rows = []
    now_utc = datetime.now(timezone.utc)
    tenant_pool = [f"tenant_{i:02d}" for i in range(8)]
    event_types = ["created", "updated", "deleted", "viewed", "exported", "merged"]
    actors = ["system", "scheduler", "user_a", "user_b", "service_principal"]

    # Sprinkle a few rows AT the window boundaries (now-7d ± few seconds)
    # to validate that boundary timestamps are not double-counted/missed.
    boundary_offsets = [
        timedelta(days=-7, seconds=1),
        timedelta(days=-7, seconds=-1),
        timedelta(days=-7),
        timedelta(microseconds=-1),
        timedelta(seconds=-1),
    ]

    for i in range(n):
        tenant = random.choice(tenant_pool)
        entity = f"ent_{BATCH_ID}_{i:05d}"
        version = random.randint(1, 5)

        if i < len(boundary_offsets):
            ts = now_utc + boundary_offsets[i]
        else:
            ts = now_utc - timedelta(hours=random.randint(0, 168),
                                      seconds=random.randint(0, 59))

        rows.append(Row(
            tenant_id=tenant,
            entity_id=entity,
            version=version,
            event_type=random.choice(event_types),
            payload=json.dumps({
                "src": "generator",
                "i": i,
                "batch": BATCH_ID,
                "nested": {"k": f"v_{i % 7}"},
            }),
            actor=random.choice(actors),
            etl_batch_id=BATCH_ID,
            last_updated=ts,
        ))
    return rows


print(f"Generating extended types data...")
showcase_rows = generate_showcase_rows(NUM_SHOWCASE_ROWS)
complex_rows = generate_complex_rows(NUM_COMPLEX_ROWS)
edge_rows = generate_edge_case_rows(NUM_EDGE_CASE_ROWS)
audit_rows = generate_audit_event_rows(NUM_AUDIT_EVENT_ROWS)
print(f"✅ Generated: {len(showcase_rows)} showcase, {len(complex_rows)} complex, "
      f"{len(edge_rows)} edge_cases, {len(audit_rows)} audit_events")


# In[6]:

# ── Write all tables to PostgreSQL ────────────────────────────────────────────

# Healthcare table schemas (parent tables first for FK constraints)
HEALTHCARE_TABLES = [
    ("departments", departments, StructType([
        StructField("department_id", StringType()),
        StructField("name", StringType()),
        StructField("floor", IntegerType()),
        StructField("head_doctor", StringType()),
    ])),
    ("doctors", doctors, StructType([
        StructField("doctor_id", StringType()),
        StructField("first_name", StringType()),
        StructField("last_name", StringType()),
        StructField("specialty", StringType()),
        StructField("department_id", StringType()),
        StructField("license_no", StringType()),
        StructField("phone", StringType()),
    ])),
    ("patients", patients, StructType([
        StructField("patient_id", StringType()),
        StructField("first_name", StringType()),
        StructField("last_name", StringType()),
        StructField("date_of_birth", DateType()),
        StructField("gender", StringType()),
        StructField("blood_type", StringType()),
        StructField("phone", StringType()),
        StructField("email", StringType()),
        StructField("address", StringType()),
        StructField("insurance_id", StringType()),
    ])),
    ("appointments", appointments, StructType([
        StructField("appointment_id", StringType()),
        StructField("patient_id", StringType()),
        StructField("doctor_id", StringType()),
        StructField("department_id", StringType()),
        StructField("appointment_date", TimestampType()),
        StructField("status", StringType()),
        StructField("visit_type", StringType()),
        StructField("notes", StringType()),
    ])),
    ("diagnoses", diagnoses, StructType([
        StructField("diagnosis_id", StringType()),
        StructField("appointment_id", StringType()),
        StructField("icd_code", StringType()),
        StructField("description", StringType()),
        StructField("severity", StringType()),
        StructField("diagnosed_at", TimestampType()),
    ])),
    ("prescriptions", prescriptions, StructType([
        StructField("prescription_id", StringType()),
        StructField("diagnosis_id", StringType()),
        StructField("patient_id", StringType()),
        StructField("doctor_id", StringType()),
        StructField("medication", StringType()),
        StructField("dosage", StringType()),
        StructField("frequency", StringType()),
        StructField("duration_days", IntegerType()),
        StructField("prescribed_at", TimestampType()),
    ])),
]

# Extended type schemas
EXTENDED_TABLES = [
    ("data_type_showcase", showcase_rows, StructType([
        StructField("record_uuid", StringType()),
        StructField("is_active", BooleanType()),
        StructField("is_verified", BooleanType()),
        StructField("tiny_val", ShortType()),
        StructField("normal_val", IntegerType()),
        StructField("big_val", LongType()),
        StructField("float4_val", FloatType()),
        StructField("float8_val", DoubleType()),
        StructField("price", DecimalType(10, 2)),
        StructField("scientific_val", DecimalType(20, 10)),
        StructField("exact_decimal", DecimalType(38, 18)),
        StructField("code_fixed", StringType()),
        StructField("code_varying", StringType()),
        StructField("description", StringType()),
        StructField("created_date", DateType()),
        StructField("created_ts", TimestampType()),
        StructField("created_tstz", TimestampType()),
        StructField("event_time", StringType()),
        StructField("event_time_tz", StringType()),
        StructField("duration_interval", StringType()),
    ])),
    ("complex_types_showcase", complex_rows, StructType([
        StructField("record_uuid", StringType()),
        StructField("metadata_jsonb", StringType()),
        StructField("raw_payload_json", StringType()),
        StructField("file_hash", BinaryType()),
        StructField("thumbnail", BinaryType()),
        StructField("tags", ArrayType(StringType())),
        StructField("scores", ArrayType(IntegerType())),
        StructField("measurements", ArrayType(DoubleType())),
        StructField("ip_address", StringType()),
        StructField("network_cidr", StringType()),
        StructField("mac_address", StringType()),
        StructField("flags", StringType()),
        StructField("permissions", StringType()),
        StructField("search_vector", StringType()),
        StructField("location_point", StringType()),
    ])),
    ("edge_cases", edge_rows, StructType([
        StructField("nullable_bool", BooleanType()),
        StructField("nullable_int", IntegerType()),
        StructField("nullable_bigint", LongType()),
        StructField("nullable_float", DoubleType()),
        StructField("nullable_decimal", DecimalType(10, 2)),
        StructField("nullable_text", StringType()),
        StructField("nullable_date", DateType()),
        StructField("nullable_ts", TimestampType()),
        StructField("nullable_jsonb", StringType()),
        StructField("nullable_array", ArrayType(StringType())),
        StructField("nullable_bytea", BinaryType()),
        StructField("max_int", IntegerType()),
        StructField("min_int", IntegerType()),
        StructField("max_bigint", LongType()),
        StructField("zero_decimal", DecimalType(10, 5)),
        StructField("negative_decimal", DecimalType(10, 5)),
        StructField("empty_string", StringType()),
        StructField("whitespace_only", StringType()),
        StructField("unicode_text", StringType()),
        StructField("very_long_text", StringType()),
        StructField("newline_text", StringType()),
        StructField("empty_json_obj", StringType()),
        StructField("empty_json_arr", StringType()),
        StructField("nested_deep_json", StringType()),
        StructField("json_with_nulls", StringType()),
        StructField("epoch_ts", TimestampType()),
        StructField("far_future_ts", TimestampType()),
    ])),
    ("audit_events", audit_rows, StructType([
        StructField("tenant_id", StringType(), nullable=False),
        StructField("entity_id", StringType(), nullable=False),
        StructField("version", IntegerType(), nullable=False),
        StructField("event_type", StringType(), nullable=False),
        StructField("payload", StringType()),
        StructField("actor", StringType()),
        StructField("etl_batch_id", StringType()),
        StructField("last_updated", TimestampType()),
    ])),
]

ALL_TABLES = HEALTHCARE_TABLES + EXTENDED_TABLES

print(f"\nWriting batch {BATCH_ID} to PostgreSQL ({PG_SCHEMA} schema)...\n")

total_start = time.time()
for table_name, rows, schema in ALL_TABLES:
    t0 = time.time()
    fqn = f"{PG_SCHEMA}.{table_name}"
    df = spark.createDataFrame(rows, schema)

    # id is BIGSERIAL for extended tables — drop if present
    if "id" in df.columns:
        df = df.drop("id")

    df.write \
        .format("jdbc") \
        .option("url", JDBC_URL) \
        .option("dbtable", fqn) \
        .options(**JDBC_PROPS) \
        .mode("append") \
        .save()

    elapsed = time.time() - t0
    print(f"  ✅ {fqn:45s} → {len(rows):>5} rows  ({elapsed:.1f}s)")

total = time.time() - total_start
print(f"\n✅ All {len(ALL_TABLES)} tables written in {total:.1f}s (batch: {BATCH_ID})")


# In[7]:

# ── Update ~30% of existing records ───────────────────────────────────────────

print(f"\n═══ Updating ~30% of existing records ═══\n")

UPDATE_FRACTION = 0.3

UPDATE_CONFIGS = [
    ("departments", "department_id", [
        "floor = floor + 1",
        "head_doctor = 'Dr. ' || substring(md5(random()::text), 1, 8)",
    ]),
    ("doctors", "doctor_id", [
        "phone = '+91-' || (7000000000 + floor(random() * 3000000000))::text",
    ]),
    ("patients", "patient_id", [
        "phone = '+91-' || (7000000000 + floor(random() * 3000000000))::text",
        "email = 'updated_' || substring(md5(random()::text), 1, 6) || '@example.com'",
    ]),
    ("appointments", "appointment_id", [
        "status = (ARRAY['completed','cancelled','no-show','rescheduled'])[floor(random()*4)+1]",
        "notes = 'Updated: ' || (ARRAY['Follow-up required','Patient improving','Vitals normal','Referred to specialist'])[floor(random()*4)+1]",
    ]),
    ("diagnoses", "diagnosis_id", [
        "severity = (ARRAY['Mild','Moderate','Severe','Critical'])[floor(random()*4)+1]",
    ]),
    ("prescriptions", "prescription_id", [
        "duration_days = (ARRAY[5,7,10,14,21,30,60,90])[floor(random()*8)+1]",
        "frequency = (ARRAY['Once daily','Twice daily','Three times daily','As needed'])[floor(random()*4)+1]",
    ]),
    ("data_type_showcase", "id", [
        "is_active = NOT is_active",
        "normal_val = normal_val + 1",
        "description = 'Updated batch " + BATCH_ID + "'",
    ]),
    ("complex_types_showcase", "id", [
        "metadata_jsonb = jsonb_set(COALESCE(metadata_jsonb, '{}'::jsonb), '{updated}', 'true'::jsonb)",
    ]),
    ("edge_cases", "id", [
        "nullable_text = COALESCE(nullable_text, '') || '_upd'",
        "nullable_int = COALESCE(nullable_int, 0) + 1",
    ]),
    ("audit_events", "tenant_id, entity_id, version", [
        "actor = (ARRAY['system','scheduler','user_a','user_b','service_principal'])[floor(random()*5)+1]",
        "payload = jsonb_set(COALESCE(payload, '{}'::jsonb), '{updated}', 'true'::jsonb)",
    ]),
]

spark._jvm.Class.forName("org.postgresql.Driver")
conn = spark._jvm.java.sql.DriverManager.getConnection(JDBC_URL, PG_USER, PG_PASSWORD)

try:
    conn.setAutoCommit(False)
    stmt = conn.createStatement()

    for table_name, pk_col, set_clauses in UPDATE_CONFIGS:
        fqn = f"{PG_SCHEMA}.{table_name}"

        rs = stmt.executeQuery(f"SELECT COUNT(*) FROM {fqn}")
        rs.next()
        total_rows = rs.getInt(1)
        rs.close()

        if total_rows == 0:
            print(f"  ⏭️  {fqn}: no rows to update")
            continue

        num_to_update = max(1, int(total_rows * UPDATE_FRACTION))
        set_expr = ", ".join(set_clauses) + ", last_updated = NOW()"

        # Composite-PK support: wrap multi-column keys in row constructor.
        # Single-column keys remain unwrapped to keep generated SQL readable.
        is_composite = "," in pk_col
        pk_expr = f"({pk_col})" if is_composite else pk_col

        update_sql = f"""
            UPDATE {fqn}
            SET {set_expr}
            WHERE {pk_expr} IN (
                SELECT {pk_col} FROM {fqn}
                ORDER BY random()
                LIMIT {num_to_update}
            )
        """
        stmt.execute(update_sql.strip())
        print(f"  ✅ {fqn:45s} → updated {num_to_update}/{total_rows} rows")

    conn.commit()
    print(f"\n✅ Updates committed (batch: {BATCH_ID})")

except Exception as e:
    conn.rollback()
    print(f"  ❌ Update failed: {e}")
    raise

finally:
    conn.close()


# In[8]:

# ── Final summary ─────────────────────────────────────────────────────────────

print(f"\n{'═' * 65}")
print(f"  DATA GENERATION COMPLETE — batch {BATCH_ID}")
print(f"{'═' * 65}")
print(f"\n  Schema: {PG_SCHEMA}")
print(f"  DROP_AND_RECREATE: {DROP_AND_RECREATE}")
print(f"\n  Healthcare tables (6):")
print(f"    • departments:     {NUM_DEPARTMENTS} new rows")
print(f"    • doctors:         {NUM_DOCTORS} new rows")
print(f"    • patients:        {NUM_PATIENTS} new rows")
print(f"    • appointments:    {NUM_APPOINTMENTS} new rows")
print(f"    • diagnoses:       {NUM_DIAGNOSES} new rows")
print(f"    • prescriptions:   {NUM_PRESCRIPTIONS} new rows")
print(f"\n  Extended type tables (4):")
print(f"    • data_type_showcase:     {NUM_SHOWCASE_ROWS} new rows (25 columns)")
print(f"    • complex_types_showcase: {NUM_COMPLEX_ROWS} new rows (18 columns)")
print(f"    • edge_cases:             {NUM_EDGE_CASE_ROWS} new rows (28 columns)")
print(f"    • audit_events:           {NUM_AUDIT_EVENT_ROWS} new rows (composite PK + TIMESTAMPTZ)")
print(f"\n  ~30% of ALL existing rows updated with last_updated = NOW()")
print(f"\n  Next steps:")
print(f"    1. Run getDataFromPostgres to copy to Lakehouse")
print(f"    2. Run comparison_setup to register tables")
print(f"    3. Run comparison_engine to validate")
print(f"{'═' * 65}")

# Verify row counts
print(f"\n📊 Row counts in PostgreSQL ({PG_SCHEMA}):\n")
for table_name, _, _ in ALL_TABLES:
    df = spark.read.format("jdbc") \
        .option("url", JDBC_URL) \
        .option("query", f"SELECT COUNT(*) AS cnt FROM {PG_SCHEMA}.{table_name}") \
        .options(**JDBC_PROPS) \
        .load()
    count = df.collect()[0]["cnt"]
    print(f"  {PG_SCHEMA}.{table_name:30s} → {count:>8} total rows")
