"""
Read Synthea CSV exports from data/synthea_raw/csv/, map to the 6 simplified
OMOP-style tables, generate NOTE text, inject dirty rows, and write CSVs to
data/raw/.

Run directly:  python -m src.generator
"""

import random

import pandas as pd

from src.config import DIRTY_DATA_FRACTION, RANDOM_SEED, RAW_DIR, SYNTHEA_RAW_DIR, VISITS_PER_PERSON
from src.concepts import (
    CONDITION_CONCEPT_ID,
    DRUG_CONCEPT_ID,
    ETHNICITY_CONCEPT_ID,
    GENDER_CONCEPT_ID,
    MEASUREMENT_CONCEPT_ID,
    RACE_CONCEPT_ID,
    UNIT_CONCEPT_ID,
    VISIT_CONCEPT_ID,
)

# UUID → int maps and included-encounter set shared across all table mappers
_person_id_map: dict[str, int] = {}
_visit_id_map: dict[str, int] = {}
_included_encounter_uuids: set[str] = set()


def _csv(name: str) -> pd.DataFrame:
    """Read a Synthea CSV by table name (no extension)."""
    return pd.read_csv(SYNTHEA_RAW_DIR / "csv" / f"{name}.csv", dtype=str)


def _map_person() -> pd.DataFrame:
    global _person_id_map
    df = _csv("patients")

    _person_id_map = {uuid: i + 1 for i, uuid in enumerate(df["Id"])}
    df["person_id"] = df["Id"].map(_person_id_map).astype(int)
    df["gender_concept_id"] = df["GENDER"].map(GENDER_CONCEPT_ID)
    df["race_concept_id"] = df["RACE"].str.lower().map(RACE_CONCEPT_ID)
    df["ethnicity_concept_id"] = df["ETHNICITY"].str.lower().map(ETHNICITY_CONCEPT_ID)
    df["year_of_birth"] = pd.to_datetime(df["BIRTHDATE"]).dt.year.astype(int)
    df["location_id"] = pd.NA

    before = len(df)
    df = df.dropna(subset=["gender_concept_id", "race_concept_id", "ethnicity_concept_id"])
    dropped = before - len(df)
    if dropped:
        print(f"[PERSON] dropped {dropped} rows with unmapped concept codes")

    for col in ["gender_concept_id", "race_concept_id", "ethnicity_concept_id"]:
        df[col] = df[col].astype(int)

    return df[["person_id", "gender_concept_id", "year_of_birth",
               "race_concept_id", "ethnicity_concept_id", "location_id"]].copy()


def _map_visit_occurrence() -> pd.DataFrame:
    global _visit_id_map, _included_encounter_uuids
    df = _csv("encounters")

    df["person_id"] = df["PATIENT"].map(_person_id_map)
    df["visit_concept_id"] = df["ENCOUNTERCLASS"].str.lower().map(VISIT_CONCEPT_ID)
    df["visit_start_date"] = pd.to_datetime(df["START"]).dt.date
    df["visit_end_date"] = pd.to_datetime(df["STOP"]).dt.date
    df["care_site_id"] = pd.NA
    df["provider_id"] = pd.NA

    before = len(df)
    df = df.dropna(subset=["person_id", "visit_concept_id", "visit_start_date"])
    dropped = before - len(df)
    if dropped:
        print(f"[VISIT_OCCURRENCE] dropped {dropped} rows with unmapped/missing values")

    # Cap at VISITS_PER_PERSON most recent visits per person
    df = (
        df.sort_values("visit_start_date", ascending=False)
          .groupby("person_id", sort=False)
          .head(VISITS_PER_PERSON)
          .reset_index(drop=True)
    )
    print(f"[VISIT_OCCURRENCE] capped to {VISITS_PER_PERSON} visits/person: {len(df):,} rows retained")

    _included_encounter_uuids = set(df["Id"])
    _visit_id_map = {uuid: i + 1 for i, uuid in enumerate(df["Id"])}

    df["visit_occurrence_id"] = df["Id"].map(_visit_id_map).astype(int)
    for col in ["visit_occurrence_id", "person_id", "visit_concept_id"]:
        df[col] = df[col].astype(int)

    return df[["visit_occurrence_id", "person_id", "visit_concept_id",
               "visit_start_date", "visit_end_date",
               "care_site_id", "provider_id"]].copy()


def _map_condition_occurrence() -> pd.DataFrame:
    df = _csv("conditions")

    # Whitelist filter — drop any SNOMED code not in our 3-condition dict
    df["condition_concept_id"] = df["CODE"].map(CONDITION_CONCEPT_ID)
    df = df.dropna(subset=["condition_concept_id"])

    df["person_id"] = df["PATIENT"].map(_person_id_map)
    df["visit_occurrence_id"] = df["ENCOUNTER"].map(_visit_id_map).astype("Int64")
    df["condition_start_date"] = pd.to_datetime(df["START"], errors="coerce").dt.date
    df["condition_end_date"] = pd.to_datetime(df["STOP"], errors="coerce").dt.date

    before = len(df)
    df = df.dropna(subset=["person_id", "condition_start_date"])
    dropped = before - len(df)
    if dropped:
        print(f"[CONDITION_OCCURRENCE] dropped {dropped} rows with missing required values")

    df = df.reset_index(drop=True)
    df["condition_occurrence_id"] = df.index + 1
    df["condition_concept_id"] = df["condition_concept_id"].astype(int)
    df["person_id"] = df["person_id"].astype(int)

    return df[["condition_occurrence_id", "person_id", "condition_concept_id",
               "condition_start_date", "condition_end_date",
               "visit_occurrence_id"]].copy()


def _map_drug_exposure() -> pd.DataFrame:
    df = _csv("medications")

    # Keep only medications from visits included in the visit cap
    df = df[df["ENCOUNTER"].isin(_included_encounter_uuids)]

    # Whitelist filter — drop any RxNorm code not in our 6-drug dict
    df["drug_concept_id"] = df["CODE"].map(DRUG_CONCEPT_ID)
    df = df.dropna(subset=["drug_concept_id"])

    df["person_id"] = df["PATIENT"].map(_person_id_map)
    df["drug_exposure_start_date"] = pd.to_datetime(df["START"], errors="coerce").dt.date
    df["drug_exposure_end_date"] = pd.to_datetime(df["STOP"], errors="coerce").dt.date

    # days_supply: derive from date diff; clamp to minimum 1 to avoid 0-day dispenses
    start_dt = pd.to_datetime(df["START"], errors="coerce")
    stop_dt = pd.to_datetime(df["STOP"], errors="coerce")
    df["days_supply"] = (stop_dt - start_dt).dt.days.fillna(30).clip(lower=1).astype(int)

    df["quantity"] = pd.to_numeric(df["DISPENSES"], errors="coerce").fillna(1.0)

    before = len(df)
    df = df.dropna(subset=["person_id", "drug_exposure_start_date"])
    dropped = before - len(df)
    if dropped:
        print(f"[DRUG_EXPOSURE] dropped {dropped} rows with missing required values")

    df = df.reset_index(drop=True)
    df["drug_exposure_id"] = df.index + 1
    df["drug_concept_id"] = df["drug_concept_id"].astype(int)
    df["person_id"] = df["person_id"].astype(int)

    return df[["drug_exposure_id", "person_id", "drug_concept_id",
               "drug_exposure_start_date", "drug_exposure_end_date",
               "days_supply", "quantity"]].copy()


def _map_measurement() -> pd.DataFrame:
    df = _csv("observations")

    # Keep only observations from visits included in the visit cap
    df = df[df["ENCOUNTER"].isin(_included_encounter_uuids)]

    # Whitelist filter — keep only our 5 LOINC codes
    df["measurement_concept_id"] = df["CODE"].map(MEASUREMENT_CONCEPT_ID)
    df = df.dropna(subset=["measurement_concept_id"])

    df["person_id"] = df["PATIENT"].map(_person_id_map)
    df["visit_occurrence_id"] = df["ENCOUNTER"].map(_visit_id_map).astype("Int64")
    df["measurement_date"] = pd.to_datetime(df["DATE"], errors="coerce").dt.date
    df["value_as_number"] = pd.to_numeric(df["VALUE"], errors="coerce")
    # unit_concept_id is nullable — unmatched units stay null, row is kept
    df["unit_concept_id"] = df["UNITS"].map(UNIT_CONCEPT_ID).astype("Int64")

    before = len(df)
    df = df.dropna(subset=["person_id", "measurement_date", "value_as_number"])
    dropped = before - len(df)
    if dropped:
        print(f"[MEASUREMENT] dropped {dropped} rows with missing required values")

    df = df.reset_index(drop=True)
    df["measurement_id"] = df.index + 1
    df["measurement_concept_id"] = df["measurement_concept_id"].astype(int)
    df["person_id"] = df["person_id"].astype(int)

    return df[["measurement_id", "person_id", "measurement_concept_id",
               "measurement_date", "value_as_number", "unit_concept_id",
               "visit_occurrence_id"]].copy()


_NOTE_COMPLAINTS = {
    1: [  # outpatient
        "Routine follow-up visit.",
        "Preventive care evaluation.",
        "Annual wellness check.",
        "Follow-up for chronic condition management.",
    ],
    2: [  # inpatient
        "Admission for inpatient monitoring and treatment.",
        "Inpatient stay for management of acute exacerbation.",
        "Admitted for observation and further evaluation.",
    ],
    3: [  # ER
        "Emergency evaluation for acute symptoms.",
        "Urgent care visit for new-onset complaint.",
        "Emergency department visit.",
    ],
}

_NOTE_ASSESSMENT = {
    1: "Patient seen and evaluated. Vital signs stable. Plan discussed with patient. Follow-up scheduled as appropriate.",
    2: "Patient admitted and monitored. Treatment initiated per protocol. Discharge planning in progress.",
    3: "Patient assessed in the emergency department. Appropriate workup completed. Disposition determined.",
}


def _map_note(visit: pd.DataFrame) -> pd.DataFrame:
    rng = random.Random(RANDOM_SEED)

    def _make_text(visit_concept_id: int) -> str:
        complaint = rng.choice(_NOTE_COMPLAINTS.get(visit_concept_id, _NOTE_COMPLAINTS[1]))
        assessment = _NOTE_ASSESSMENT.get(visit_concept_id, _NOTE_ASSESSMENT[1])
        return f"CHIEF COMPLAINT: {complaint}\n\nASSESSMENT AND PLAN: {assessment}"

    df = visit[["visit_occurrence_id", "person_id", "visit_start_date", "visit_concept_id"]].copy()
    df = df.rename(columns={"visit_start_date": "note_date"})
    df["note_text"] = df["visit_concept_id"].apply(_make_text)
    df = df.reset_index(drop=True)
    df["note_id"] = df.index + 1

    return df[["note_id", "person_id", "note_date", "note_text", "visit_occurrence_id"]].copy()


def _inject_dirty_data(
    person: pd.DataFrame,
    visit: pd.DataFrame,
    condition: pd.DataFrame,
    drug: pd.DataFrame,
    measurement: pd.DataFrame,
    note: pd.DataFrame,
) -> tuple[pd.DataFrame, ...]:
    """Inject ~DIRTY_DATA_FRACTION of rows/values with known data quality issues."""
    rng = random.Random(RANDOM_SEED + 1)

    def _n(df: pd.DataFrame) -> int:
        return max(1, int(len(df) * DIRTY_DATA_FRACTION))

    def _pos(df: pd.DataFrame, k: int) -> list[int]:
        return rng.sample(range(len(df)), min(k, len(df)))

    def _dup(df: pd.DataFrame, k: int) -> pd.DataFrame:
        return pd.concat([df, df.iloc[_pos(df, k)].copy()], ignore_index=True)

    def _null(df: pd.DataFrame, col: str, k: int) -> pd.DataFrame:
        df = df.copy()
        if pd.api.types.is_integer_dtype(df[col]):
            df[col] = df[col].astype("Int64")
        df.loc[_pos(df, k), col] = pd.NA
        return df

    def _bad_end(df: pd.DataFrame, start_col: str, end_col: str, k: int) -> pd.DataFrame:
        df = df.copy()
        ix = _pos(df, k)
        df.loc[ix, end_col] = (
            pd.to_datetime(df.loc[ix, start_col]) - pd.Timedelta(days=1)
        ).dt.date.values
        return df

    def _neg(df: pd.DataFrame, col: str, k: int) -> pd.DataFrame:
        df = df.copy()
        df.loc[_pos(df, k), col] = -1
        return df

    def _orphan(df: pd.DataFrame, col: str, k: int) -> pd.DataFrame:
        df = df.copy()
        if pd.api.types.is_integer_dtype(df[col]):
            df[col] = df[col].astype("Int64")
        df.loc[_pos(df, k), col] = 99999
        return df

    # Pre-compute n from original table sizes before any rows are added
    n_p = _n(person); n_v = _n(visit);     n_c = _n(condition)
    n_d = _n(drug);   n_m = _n(measurement); n_n = _n(note)

    # PERSON: null year_of_birth + duplicates
    person = _null(person, "year_of_birth", n_p)
    person = _dup(person, n_p)

    # VISIT_OCCURRENCE: null visit_concept_id + bad end date + duplicates
    visit = _null(visit, "visit_concept_id", n_v)
    visit = _bad_end(visit, "visit_start_date", "visit_end_date", n_v)
    visit = _dup(visit, n_v)

    # CONDITION_OCCURRENCE: null condition_concept_id + bad end date + duplicates
    condition = _null(condition, "condition_concept_id", n_c)
    condition = _bad_end(condition, "condition_start_date", "condition_end_date", n_c)
    condition = _dup(condition, n_c)

    # DRUG_EXPOSURE: null drug_concept_id + bad end date + negative days_supply/quantity + duplicates
    drug = _null(drug, "drug_concept_id", n_d)
    drug = _bad_end(drug, "drug_exposure_start_date", "drug_exposure_end_date", n_d)
    drug = _neg(drug, "days_supply", n_d)
    drug = _neg(drug, "quantity", n_d)
    drug = _dup(drug, n_d)

    # MEASUREMENT: null measurement_date + implausible value_as_number + orphaned person_id + duplicates
    measurement = _null(measurement, "measurement_date", n_m)
    measurement = _neg(measurement, "value_as_number", n_m)
    measurement = _orphan(measurement, "person_id", n_m)
    measurement = _dup(measurement, n_m)

    # NOTE: null note_text + orphaned visit_occurrence_id + duplicates
    note = _null(note, "note_text", n_n)
    note = _orphan(note, "visit_occurrence_id", n_n)
    note = _dup(note, n_n)

    total = n_p + n_v + n_c + n_d + n_m + n_n
    print(f"[DIRTY] injected ~{total} dirty rows across 6 tables ({DIRTY_DATA_FRACTION:.1%} each)")

    return person, visit, condition, drug, measurement, note


def generate() -> None:
    """Build all 6 raw tables, inject dirty data, and write CSVs to data/raw/."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    # Map clean tables (order matters: _person_id_map/_visit_id_map populate first)
    person      = _map_person()
    visit       = _map_visit_occurrence()
    condition   = _map_condition_occurrence()
    drug        = _map_drug_exposure()
    measurement = _map_measurement()
    note        = _map_note(visit)

    # Inject dirty rows/values
    person, visit, condition, drug, measurement, note = _inject_dirty_data(
        person, visit, condition, drug, measurement, note
    )

    # Write to data/raw/
    person.to_csv(RAW_DIR / "person.csv", index=False)
    print(f"[PERSON] {len(person):,} rows -> data/raw/person.csv")

    visit.to_csv(RAW_DIR / "visit_occurrence.csv", index=False)
    print(f"[VISIT_OCCURRENCE] {len(visit):,} rows -> data/raw/visit_occurrence.csv")

    condition.to_csv(RAW_DIR / "condition_occurrence.csv", index=False)
    print(f"[CONDITION_OCCURRENCE] {len(condition):,} rows -> data/raw/condition_occurrence.csv")

    drug.to_csv(RAW_DIR / "drug_exposure.csv", index=False)
    print(f"[DRUG_EXPOSURE] {len(drug):,} rows -> data/raw/drug_exposure.csv")

    measurement.to_csv(RAW_DIR / "measurement.csv", index=False)
    print(f"[MEASUREMENT] {len(measurement):,} rows -> data/raw/measurement.csv")

    note.to_csv(RAW_DIR / "note.csv", index=False)
    print(f"[NOTE] {len(note):,} rows -> data/raw/note.csv")


if __name__ == "__main__":
    generate()
