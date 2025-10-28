from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse, StreamingResponse
import pandas as pd
import tempfile
import os
import sys

CURR_PATH=os.path.join(os.path.dirname(__file__))
sys.path.append(CURR_PATH)

from deepseek_owner_transformation import run_owner_normalization

app = FastAPI(
    title="Owner Normalization Service",
    description="Takes an inventory CSV, asks an LLM to clean owner fields, applies high-confidence fixes (>=70%), returns updated data.",
    version="1.0.0"
)

@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/process-file")
async def process_file(file: UploadFile = File(...), return_csv: bool = True):
    """
    Send a CSV with columns ['owner','owner_team','owner_email', ...].
    We'll:
      1. run the LLM
      2. merge high-confidence rows
      3. give you:
         - llm_json:     model output
         - updated_csv:  (optional) corrected CSV as a download stream
    """
    # sanity check: must be CSV-ish
    filename_lower = file.filename.lower()
    if not (filename_lower.endswith(".csv")):
        raise HTTPException(status_code=400, detail="Please upload a CSV file.")

    # write upload to a temp file on disk so pandas can read it
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
            tmp_path = tmp.name
            contents = await file.read()
            tmp.write(contents)

        # run your full pipeline
        result = run_owner_normalization(tmp_path)

        # result has:
        # - "updated_csv_path"
        # - "llm_json"
        # - etc.

        if return_csv:
            # We'll stream the corrected CSV back to caller.
            def iterfile():
                with open(result["updated_csv_path"], mode="rb") as f:
                    yield from f

            return StreamingResponse(
                iterfile(),
                media_type="text/csv",
                headers={
                    "Content-Disposition": 'attachment; filename="inventory_corrected.csv"',
                    "X-LLM-JSON": "see-body-json-field",
                },
            )

        # else: return JSON summary only
        return JSONResponse(
            content={
                "llm_json": result["llm_json"],
                "updated_df_preview": result["updated_df_preview"],
                "updated_csv_path": result["updated_csv_path"],
                "llm_json_path": result["llm_json_path"],
            }
        )

    finally:
        # cleanup temp upload
        if os.path.exists(tmp_path):
            os.remove(tmp_path)