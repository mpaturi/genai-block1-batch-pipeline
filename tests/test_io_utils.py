import pandas as pd

import src.io_utils as io_utils
from src import schemas


def test_read_csv_handles_embedded_newline_in_a_field(tmp_path, spark, monkeypatch):
    """Regression test for a bug where Spark's CSV reader (without multiLine)
    misparsed a quoted field containing an embedded newline as extra, corrupted
    rows. note_text legitimately contains "\\n\\n" (see generator.py's fallback
    template), so this scenario is not hypothetical -- it happened in
    data/raw/note.csv and silently inflated row counts."""
    monkeypatch.setattr(io_utils, "RAW_DIR", tmp_path)

    df = pd.DataFrame([
        {"note_id": 1, "person_id": 1, "note_date": "2025-01-01",
         "note_text": "CHIEF COMPLAINT: Routine visit.\n\nASSESSMENT AND PLAN: Stable.",
         "visit_occurrence_id": 1},
        {"note_id": 2, "person_id": 2, "note_date": "2025-01-02",
         "note_text": "Single-line note, no embedded newline.",
         "visit_occurrence_id": 2},
    ])
    df.to_csv(tmp_path / "note.csv", index=False)

    result = io_utils._read_csv(spark, "note.csv", schemas.NOTE).orderBy("note_id").collect()

    assert len(result) == 2
    assert result[0]["note_text"] == "CHIEF COMPLAINT: Routine visit.\n\nASSESSMENT AND PLAN: Stable."
    assert result[1]["note_text"] == "Single-line note, no embedded newline."
