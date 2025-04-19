import os
from datatrove.pipeline.dedup.minhash import MinhashConfig
from datatrove.pipeline.extractors import Trafilatura
from datatrove.pipeline.filters import (
    C4QualityFilter,
    FineWebQualityFilter,
    GopherQualityFilter,
    GopherRepetitionFilter,
    LanguageFilter,
    URLFilter,
)
from datatrove.pipeline.formatters import PIIFormatter
from datatrove.pipeline.readers import WarcReader
from datatrove.pipeline.tokens import TokensCounter
from datatrove.pipeline.writers.jsonl import JsonlWriter
from datatrove.utils.pipeline import run_pipeline

# === Configuration ===
WARC_PATH = "warc.paths.gz"  # Path to your local WARC file
OUTPUT_DIR = "output_local"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# === Define the pipeline ===
pipeline = [
    WarcReader(
        path=WARC_PATH,
        default_metadata={"source": "local_warc"},
    ),
    URLFilter(),  # You can remove or tweak filters if needed
    Trafilatura(favour_precision=True),
    LanguageFilter(),  # defaults to keep only English
    GopherRepetitionFilter(),
    GopherQualityFilter(),
    C4QualityFilter(filter_no_terminal_punct=False),
    FineWebQualityFilter(),
    TokensCounter(),
    PIIFormatter(),
    JsonlWriter(f"{OUTPUT_DIR}/processed.jsonl.gz"),
]

# === Run pipeline ===
run_pipeline(pipeline)
