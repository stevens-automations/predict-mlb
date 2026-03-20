# Legacy Surface

This directory holds demoted artifacts that are retained for traceability or occasional manual use, but are not part of the canonical historical rebuild and offline training surface.

Current contents:

- `retrieval/data_retriever.py`: old notebook-era bulk retriever
- `runtime/main.py`: legacy scheduler entrypoint for the daily prediction/tweet flow
- `notebooks/mlb-predict.ipynb`: notebook-era exploration/training artifact

When touching active repo guidance, prefer pointing users to `scripts/history_ingest.py`, `train/`, `scripts/training/`, and `configs/training/` instead.
