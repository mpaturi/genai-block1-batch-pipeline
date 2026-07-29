"""Concept dictionaries mapping Synthea source codes to synthetic *_concept_id values.

See docs/spec.md ("Concept ID mapping") for the rationale: Block 1 does not aim
for full OMOP vocabulary fidelity, so these dicts define a curated whitelist of
Synthea source codes -> small synthetic integers. src/generator.py uses
`.get(code)` against these dicts; a record whose source code is absent (returns
None) is excluded rather than mapped.
"""

# --- PERSON ---------------------------------------------------------------

# patients.csv GENDER
GENDER_CONCEPT_ID = {
    "M": 1,  # Male
    "F": 2,  # Female
}

# patients.csv RACE
RACE_CONCEPT_ID = {
    "white": 1,
    "black": 2,
    "asian": 3,
    "hawaiian": 4,
    "native": 5,   # American Indian / Alaska Native
    "other": 6,
}

# patients.csv ETHNICITY
ETHNICITY_CONCEPT_ID = {
    "hispanic": 1,
    "nonhispanic": 2,
}

# --- VISIT_OCCURRENCE -------------------------------------------------------

VISIT_OUTPATIENT = 1
VISIT_INPATIENT = 2
VISIT_ER = 3

# encounters.csv ENCOUNTERCLASS, bucketed into outpatient/inpatient/ER.
# Every ENCOUNTERCLASS value maps to one of these three; none are excluded.
VISIT_CONCEPT_ID = {
    "ambulatory": VISIT_OUTPATIENT,
    "outpatient": VISIT_OUTPATIENT,
    "wellness": VISIT_OUTPATIENT,
    "urgentcare": VISIT_OUTPATIENT,
    "virtual": VISIT_OUTPATIENT,
    "home": VISIT_OUTPATIENT,
    "inpatient": VISIT_INPATIENT,
    "snf": VISIT_INPATIENT,
    "hospice": VISIT_INPATIENT,
    "emergency": VISIT_ER,
}

# --- CONDITION_OCCURRENCE ---------------------------------------------------

CONDITION_DIABETES = 1
CONDITION_HYPERTENSION = 2
CONDITION_HYPERLIPIDEMIA = 3
CONDITION_ANEMIA = 4
CONDITION_OSTEOPOROSIS = 5
CONDITION_CHF = 6
CONDITION_AFIB = 7
CONDITION_STREP_THROAT = 8
CONDITION_UTI = 9
CONDITION_PULMONARY_EMBOLISM = 10
CONDITION_OSTEOARTHRITIS = 11

# conditions.csv CODE (SNOMED)
CONDITION_CONCEPT_ID = {
    "44054006": CONDITION_DIABETES,  # Diabetes mellitus type 2 (disorder)
    "59621000": CONDITION_HYPERTENSION,  # Essential hypertension (disorder)
    "55822004": CONDITION_HYPERLIPIDEMIA,  # Hyperlipidemia (disorder)
    "271737000": CONDITION_ANEMIA,  # Anemia (disorder)
    "64859006": CONDITION_OSTEOPOROSIS,  # Osteoporosis (disorder)
    "88805009": CONDITION_CHF,  # Chronic congestive heart failure (disorder)
    "49436004": CONDITION_AFIB,  # Atrial fibrillation (disorder)
    "43878008": CONDITION_STREP_THROAT,  # Streptococcal sore throat (disorder)
    "307426000": CONDITION_UTI,  # Acute infective cystitis (disorder)
    "706870000": CONDITION_PULMONARY_EMBOLISM,  # Acute pulmonary embolism (disorder)
    "239873007": CONDITION_OSTEOARTHRITIS,  # Osteoarthritis of knee (disorder)
}

# --- DRUG_EXPOSURE -----------------------------------------------------------

# medications.csv CODE (RxNorm), linked via REASONCODE to CONDITION_CONCEPT_ID above
DRUG_CONCEPT_ID = {
    "860975": 1,  # Metformin (diabetes)
    "106892": 2,  # Humulin insulin (diabetes)
    "314076": 3,  # Lisinopril (hypertension)
    "308136": 4,  # Amlodipine (hypertension)
    "310798": 5,  # Hydrochlorothiazide (hypertension)
    "314231": 6,  # Simvastatin (hyperlipidemia)
    "205923": 7,   # Epoetin Alfa (anemia)
    "904419": 8,   # Alendronic acid (osteoporosis)
    "1719286": 9,  # Furosemide injection (CHF)
    "200033": 10,  # Carvedilol (CHF)
    "197604": 11,  # Digoxin (atrial fibrillation)
    "855332": 12,  # Warfarin (atrial fibrillation)
    "834102": 13,  # Penicillin V Potassium 500mg (strep throat)
    "834061": 13,  # Penicillin V Potassium 250mg (strep throat, same concept as above)
    "309309": 14,  # Ciprofloxacin (UTI)
    "1648755": 15, # Nitrofurantoin (UTI)
    "854252": 16,  # Enoxaparin (pulmonary embolism)
    "849574": 17,  # Naproxen sodium (osteoarthritis)
}

# --- MEASUREMENT --------------------------------------------------------------

MEASUREMENT_SBP = 1
MEASUREMENT_BMI = 2
MEASUREMENT_GLUCOSE = 3
MEASUREMENT_HBA1C = 4

# observations.csv CODE (LOINC). Glucose (Blood) and Glucose (Serum/Plasma) are
# collapsed into a single MEASUREMENT_GLUCOSE concept.
MEASUREMENT_CONCEPT_ID = {
    "8480-6": MEASUREMENT_SBP,  # Systolic Blood Pressure
    "39156-5": MEASUREMENT_BMI,  # Body mass index (BMI) [Ratio]
    "2339-0": MEASUREMENT_GLUCOSE,  # Glucose [Mass/volume] in Blood
    "2345-7": MEASUREMENT_GLUCOSE,  # Glucose [Mass/volume] in Serum or Plasma
    "4548-4": MEASUREMENT_HBA1C,  # Hemoglobin A1c/Hemoglobin.total in Blood
}

# observations.csv UNITS for the measurements above
UNIT_CONCEPT_ID = {
    "mm[Hg]": 1,
    "kg/m2": 2,
    "mg/dL": 3,
    "%": 4,
}
