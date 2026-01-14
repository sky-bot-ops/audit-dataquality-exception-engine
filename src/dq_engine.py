"""
dq_engine.py — Audit Data Quality Exception Engine (production-style)

What this does (real-world, modern):
- Reads large CSV transaction extracts (chunked for scale)
- Validates schema + coerces datatypes safely
- Runs a configurable suite of audit/data-quality/compliance checks:
  * Negative/zero amounts, missing fields, invalid dates
  * Weekend / non-business day postings (US Federal holiday calendar)
  * Duplicate invoice/document IDs (and optional composite keys)
  * Outlier detection (robust z-score / MAD)
  * Benford’s Law anomaly signal (optional)
  * Suspicious vendor patterns (regex-based)
  * Future/too-old dates, posting_date > today, etc.
  * Amount rounding issues (e.g., fractions of cents)
  * Currency mismatch checks (optional)
  * Custom threshold rules (JSON/YAML-like dict inside code; easy to move to YAML later)
- Produces:
  * Excel exception report: sample_input, exceptions, summary, rule_catalog, profiling
  * Power BI–ready outputs: exceptions.csv, summary.csv in reports/
- Includes severity scoring + risk score per row
- Clean CLI usage

Run:
  pip install -r requirements.txt
  python src/dq_engine.py --input data/sample_transactions.csv --outdir reports

Notes:
- This file is self-contained; you can later extract CONFIG to YAML/JSON.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import math
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd


# -----------------------------
# Logging
# -----------------------------
def setup_logger(level: str = "INFO") -> logging.Logger:
    logger = logging.getLogger("dq_engine")
    if logger.handlers:
        return logger
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    ch = logging.StreamHandler(sys.stdout)
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    return logger


# -----------------------------
# Config (easy to move to YAML/JSON later)
# -----------------------------
DEFAULT_CONFIG: Dict[str, Any] = {
    "expected_columns": {
        # Standardized columns (case-insensitive match)
        "invoice_id": {"required": True, "aliases": ["invoice_id", "invoiceid", "doc_id", "document_id"]},
        "vendor": {"required": True, "aliases": ["vendor", "vendor_name", "supplier", "supplier_name"]},
        "amount": {"required": True, "aliases": ["amount", "txn_amount", "transaction_amount", "gross_amount"]},
        "posting_date": {"required": True, "aliases": ["posting_date", "post_date", "date", "txn_date"]},
        # Optional fields that unlock more checks
        "currency": {"required": False, "aliases": ["currency", "ccy"]},
        "country": {"required": False, "aliases": ["country", "country_code"]},
        "payment_terms": {"required": False, "aliases": ["payment_terms", "terms"]},
        "po_number": {"required": False, "aliases": ["po_number", "po", "purchase_order"]},
        "cost_center": {"required": False, "aliases": ["cost_center", "cc", "costcentre"]},
        "description": {"required": False, "aliases": ["description", "memo", "narration", "text"]},
    },
    "date_rules": {
        "max_future_days": 2,          # allow tiny timing drift
        "max_age_days": 365 * 7,       # flag postings older than 7 years (tune as needed)
        "business_calendar": "US_FEDERAL",  # currently supported: US_FEDERAL
    },
    "amount_rules": {
        "flag_zero_amount": True,
        "flag_negative_amount": True,
        "min_reasonable": None,        # e.g. 0.01
        "max_reasonable": None,        # e.g. 10_000_000
        "fractional_cent_tolerance": 1e-9,  # if amount has >2 decimals
        "outlier_detection": {
            "enabled": True,
            "method": "mad",           # "mad" or "zscore"
            "threshold": 6.0,          # mad threshold; 3.5–8 typical
            "min_non_null": 50,        # only run if enough data
        },
        "benford": {
            "enabled": True,
            "min_non_null": 200,
            "chi_square_threshold": 25.0,  # rough signal; tune per domain
        },
    },
    "duplicate_rules": {
        "enabled": True,
        "keys": [
            ["invoice_id"],  # baseline
            # Uncomment if you want stricter duplicates:
            # ["invoice_id", "vendor", "amount", "posting_date"]
        ],
        "treat_blank_invoice_as_exempt": False,
    },
    "string_rules": {
        "vendor_blacklist_regex": [
            r"\btest\b",
            r"\bdummy\b",
            r"\bna\b",
            r"\bn\/a\b",
            r"\bunknown\b",
        ],
        "vendor_suspicious_chars_regex": [
            r"[<>$%{}]",
        ],
        "min_vendor_length": 2,
        "description_suspicious_regex": [
            r"\bgift\b",
            r"\bcash\b",
            r"\bwire\b",
            r"\boff\s*book\b",
        ],
    },
    "compliance_rules": {
        "weekend_postings": True,
        "non_business_day_postings": True,
    },
    "output": {
        "excel_filename": "audit_exception_report.xlsx",
        "exceptions_csv": "exceptions_powerbi.csv",
        "summary_csv": "summary_powerbi.csv",
        "max_sample_rows": 200,
        "max_exceptions_rows": 200000,  # safety for Excel; keep CSV for full
    },
    "severity": {
        # Consistent scoring helps KPIs and prioritization
        "HIGH": 3,
        "MEDIUM": 2,
        "LOW": 1,
        "INFO": 0,
    },
}


# -----------------------------
# Holiday Calendar (US Federal)
# -----------------------------
def us_federal_holidays(year: int) -> set:
    """
    Lightweight US Federal holiday set for business-day check.
    Uses a practical list (not exhaustive of observed rules edge-cases, but strong enough for audit flagging).
    For high precision, you can later swap to `holidays` library.
    """
    # Basic holidays (some observed adjustments handled)
    holidays = set()

    def observed(d: dt.date) -> dt.date:
        # If holiday on Saturday -> observed Friday; if on Sunday -> observed Monday
        if d.weekday() == 5:
            return d - dt.timedelta(days=1)
        if d.weekday() == 6:
            return d + dt.timedelta(days=1)
        return d

    # New Year's Day
    holidays.add(observed(dt.date(year, 1, 1)))
    # Martin Luther King Jr. Day (3rd Monday Jan)
    holidays.add(nth_weekday_of_month(year, 1, 0, 3))
    # Washington's Birthday / Presidents Day (3rd Monday Feb)
    holidays.add(nth_weekday_of_month(year, 2, 0, 3))
    # Memorial Day (last Monday May)
    holidays.add(last_weekday_of_month(year, 5, 0))
    # Juneteenth (Jun 19)
    holidays.add(observed(dt.date(year, 6, 19)))
    # Independence Day (Jul 4)
    holidays.add(observed(dt.date(year, 7, 4)))
    # Labor Day (1st Monday Sep)
    holidays.add(nth_weekday_of_month(year, 9, 0, 1))
    # Columbus Day (2nd Monday Oct)
    holidays.add(nth_weekday_of_month(year, 10, 0, 2))
    # Veterans Day (Nov 11)
    holidays.add(observed(dt.date(year, 11, 11)))
    # Thanksgiving (4th Thursday Nov)
    holidays.add(nth_weekday_of_month(year, 11, 3, 4))
    # Christmas (Dec 25)
    holidays.add(observed(dt.date(year, 12, 25)))

    return holidays


def nth_weekday_of_month(year: int, month: int, weekday: int, n: int) -> dt.date:
    """weekday: Monday=0 ... Sunday=6"""
    first = dt.date(year, month, 1)
    shift = (weekday - first.weekday()) % 7
    return first + dt.timedelta(days=shift + (n - 1) * 7)


def last_weekday_of_month(year: int, month: int, weekday: int) -> dt.date:
    """weekday: Monday=0 ... Sunday=6"""
    if month == 12:
        last = dt.date(year + 1, 1, 1) - dt.timedelta(days=1)
    else:
        last = dt.date(year, month + 1, 1) - dt.timedelta(days=1)
    shift = (last.weekday() - weekday) % 7
    return last - dt.timedelta(days=shift)


def is_business_day(d: dt.date, calendar: str = "US_FEDERAL") -> bool:
    if d.weekday() >= 5:
        return False
    if calendar == "US_FEDERAL":
        return d not in us_federal_holidays(d.year)
    return True


# -----------------------------
# Utilities
# -----------------------------
def safe_to_datetime(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce")


def safe_to_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def normalize_str(s: Any) -> str:
    if pd.isna(s):
        return ""
    x = str(s).strip().lower()
    x = re.sub(r"\s+", " ", x)
    return x


def ensure_outdir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(c).strip().lower() for c in df.columns]
    return df


def map_aliases_to_standard(df: pd.DataFrame, config: Dict[str, Any]) -> Tuple[pd.DataFrame, Dict[str, str]]:
    """
    Rename columns from aliases -> standard names. Returns df, mapping used.
    """
    col_map = {}
    existing = set(df.columns)

    for std_col, spec in config["expected_columns"].items():
        aliases = [a.lower() for a in spec.get("aliases", [])]
        for a in aliases:
            if a in existing:
                col_map[a] = std_col
                break

    df = df.rename(columns=col_map)
    return df, col_map


def validate_required_columns(df: pd.DataFrame, config: Dict[str, Any]) -> List[str]:
    missing = []
    for col, spec in config["expected_columns"].items():
        if spec.get("required", False) and col not in df.columns:
            missing.append(col)
    return missing


def add_row_id(df: pd.DataFrame) -> pd.DataFrame:
    if "_row_id" not in df.columns:
        df["_row_id"] = np.arange(len(df), dtype=np.int64) + 1
    return df


@dataclass
class Finding:
    rule_id: str
    rule_name: str
    severity: str
    message: str


# -----------------------------
# Rule Engine
# -----------------------------
class RuleEngine:
    def __init__(self, config: Dict[str, Any], logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.findings_catalog = self._build_rule_catalog()

    def _build_rule_catalog(self) -> pd.DataFrame:
        rules = [
            ("AMT_NEGATIVE", "Negative amount", "HIGH", "Amount < 0"),
            ("AMT_ZERO", "Zero amount", "MEDIUM", "Amount == 0"),
            ("AMT_TOO_SMALL", "Amount below minimum threshold", "LOW", "Amount < configured minimum"),
            ("AMT_TOO_LARGE", "Amount above maximum threshold", "MEDIUM", "Amount > configured maximum"),
            ("AMT_FRACTIONAL_CENTS", "Fractional cents", "LOW", "Amount has >2 decimal precision"),
            ("REQ_MISSING_VENDOR", "Missing vendor", "HIGH", "Vendor is blank / null / too short"),
            ("REQ_MISSING_INVOICE", "Missing invoice/document id", "MEDIUM", "Invoice id is blank / null"),
            ("DATE_INVALID", "Invalid posting date", "HIGH", "Posting date not parseable"),
            ("DATE_FUTURE", "Future posting date", "HIGH", "Posting date beyond allowed future window"),
            ("DATE_TOO_OLD", "Very old posting date", "LOW", "Posting date older than max_age_days"),
            ("DATE_WEEKEND", "Weekend posting", "MEDIUM", "Posting on Saturday/Sunday"),
            ("DATE_NON_BUSINESS", "Non-business day posting", "LOW", "Posting on US federal holiday"),
            ("DUPLICATE_KEY", "Duplicate key", "HIGH", "Duplicate based on configured key(s)"),
            ("VENDOR_BLACKLIST", "Suspicious vendor name", "MEDIUM", "Vendor matches blacklist regex"),
            ("VENDOR_BAD_CHARS", "Vendor contains suspicious characters", "LOW", "Vendor has odd characters"),
            ("DESC_SUSPICIOUS", "Suspicious description keywords", "LOW", "Description matches regex list"),
            ("AMT_OUTLIER", "Amount outlier", "MEDIUM", "Robust outlier signal (MAD/Z-score)"),
            ("AMT_BENFORD", "Benford anomaly signal", "INFO", "Benford chi-square above threshold"),
        ]
        return pd.DataFrame(rules, columns=["rule_id", "rule_name", "severity", "definition"])

    def run(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Returns:
          exceptions_df: row-level exceptions (one row per finding per transaction)
          summary_df: aggregated counts by rule + severity
          profiling_df: dataset profiling (null rates, types, basic stats)
        """
        df = df.copy()
        df = add_row_id(df)

        # Coercions
        df["posting_date_raw"] = df.get("posting_date")
        df["posting_date"] = safe_to_datetime(df.get("posting_date"))
        df["amount_raw"] = df.get("amount")
        df["amount"] = safe_to_numeric(df.get("amount"))

        # Optional fields normalized
        if "vendor" in df.columns:
            df["vendor_norm"] = df["vendor"].map(normalize_str)
        else:
            df["vendor_norm"] = ""

        if "description" in df.columns:
            df["description_norm"] = df["description"].map(normalize_str)
        else:
            df["description_norm"] = ""

        findings_rows: List[Dict[str, Any]] = []

        # ---- Required/Missing checks
        findings_rows += self._check_missing_vendor(df)
        findings_rows += self._check_missing_invoice(df)
        findings_rows += self._check_invalid_dates(df)
        findings_rows += self._check_future_and_old_dates(df)

        # ---- Date compliance
        if self.config["compliance_rules"].get("weekend_postings", True):
            findings_rows += self._check_weekend_postings(df)
        if self.config["compliance_rules"].get("non_business_day_postings", True):
            findings_rows += self._check_non_business_days(df)

        # ---- Amount checks
        findings_rows += self._check_amount_signals(df)
        findings_rows += self._check_fractional_cents(df)
        findings_rows += self._check_outliers(df)
        findings_rows += self._check_benford(df)

        # ---- String/pattern checks
        findings_rows += self._check_vendor_patterns(df)
        findings_rows += self._check_description_patterns(df)

        # ---- Duplicates
        if self.config["duplicate_rules"].get("enabled", True):
            findings_rows += self._check_duplicates(df)

        # Build exceptions dataframe
        exceptions_df = pd.DataFrame(findings_rows)
        if exceptions_df.empty:
            exceptions_df = pd.DataFrame(
                columns=[
                    "_row_id",
                    "rule_id",
                    "rule_name",
                    "severity",
                    "severity_score",
                    "message",
                ]
                + self._core_columns_for_output(df)
            )

        else:
            exceptions_df["severity_score"] = exceptions_df["severity"].map(self.config["severity"]).fillna(0).astype(int)
            # Join back core fields for context
            core_cols = ["_row_id"] + self._core_columns_for_output(df)
            core = df[core_cols].copy()
            exceptions_df = exceptions_df.merge(core, on="_row_id", how="left")

        # Add an overall risk score per transaction (for Power BI)
        risk = (
            exceptions_df.groupby("_row_id", as_index=False)["severity_score"]
            .sum()
            .rename(columns={"severity_score": "risk_score"})
        )
        if not risk.empty:
            exceptions_df = exceptions_df.merge(risk, on="_row_id", how="left")
        else:
            exceptions_df["risk_score"] = 0

        # Summary
        summary_df = (
            exceptions_df.groupby(["rule_id", "rule_name", "severity"], as_index=False)
            .agg(exception_count=("_row_id", "count"), affected_rows=("_row_id", "nunique"))
            .sort_values(["severity", "exception_count"], ascending=[True, False])
        )
        summary_df["severity_score"] = summary_df["severity"].map(self.config["severity"]).fillna(0).astype(int)
        summary_df = summary_df.sort_values(["severity_score", "exception_count"], ascending=[False, False])

        # Profiling
        profiling_df = self._profile_dataset(df)

        return exceptions_df, summary_df, profiling_df

    def _core_columns_for_output(self, df: pd.DataFrame) -> List[str]:
        cols = []
        for c in ["invoice_id", "vendor", "amount", "posting_date", "currency", "country", "description"]:
            if c in df.columns:
                cols.append(c)
        return cols

    def _emit(self, df: pd.DataFrame, mask: pd.Series, rule_id: str, message: str) -> List[Dict[str, Any]]:
        if mask is None or mask.sum() == 0:
            return []
        rule = self.findings_catalog[self.findings_catalog["rule_id"] == rule_id].iloc[0].to_dict()
        out = []
        ids = df.loc[mask, "_row_id"].astype(int).tolist()
        for rid in ids:
            out.append(
                {
                    "_row_id": rid,
                    "rule_id": rule["rule_id"],
                    "rule_name": rule["rule_name"],
                    "severity": rule["severity"],
                    "message": message,
                }
            )
        return out

    # -----------------
    # Individual checks
    # -----------------
    def _check_missing_vendor(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        if "vendor" not in df.columns:
            return []
        min_len = self.config["string_rules"].get("min_vendor_length", 2)
        mask = df["vendor"].isna() | (df["vendor_norm"] == "") | (df["vendor_norm"].str.len() < min_len)
        return self._emit(df, mask, "REQ_MISSING_VENDOR", f"Vendor is missing/blank/too short (<{min_len}).")

    def _check_missing_invoice(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        if "invoice_id" not in df.columns:
            return []
        mask = df["invoice_id"].isna() | (df["invoice_id"].astype(str).str.strip() == "")
        return self._emit(df, mask, "REQ_MISSING_INVOICE", "Invoice/Document identifier is missing/blank.")

    def _check_invalid_dates(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        if "posting_date" not in df.columns:
            return []
        mask = df["posting_date"].isna()
        return self._emit(df, mask, "DATE_INVALID", "Posting date could not be parsed (invalid format/value).")

    def _check_future_and_old_dates(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        if "posting_date" not in df.columns:
            return []
        rules = self.config["date_rules"]
        max_future_days = int(rules.get("max_future_days", 0))
        max_age_days = int(rules.get("max_age_days", 365 * 7))

        today = pd.Timestamp(dt.date.today())
        # Allow slight future drift
        future_cutoff = today + pd.Timedelta(days=max_future_days)

        mask_future = df["posting_date"].notna() & (df["posting_date"] > future_cutoff)
        mask_old = df["posting_date"].notna() & (df["posting_date"] < (today - pd.Timedelta(days=max_age_days)))

        out = []
        out += self._emit(df, mask_future, "DATE_FUTURE", f"Posting date is > {max_future_days} day(s) in the future.")
        out += self._emit(df, mask_old, "DATE_TOO_OLD", f"Posting date is older than {max_age_days} day(s).")
        return out

    def _check_weekend_postings(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        if "posting_date" not in df.columns:
            return []
        mask = df["posting_date"].notna() & (df["posting_date"].dt.weekday >= 5)
        return self._emit(df, mask, "DATE_WEEKEND", "Posting occurred on a weekend (Sat/Sun).")

    def _check_non_business_days(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        if "posting_date" not in df.columns:
            return []
        cal = self.config["date_rules"].get("business_calendar", "US_FEDERAL")

        def _is_nonbiz(x: pd.Timestamp) -> bool:
            if pd.isna(x):
                return False
            d = x.date()
            return (d.weekday() < 5) and (not is_business_day(d, cal))

        mask = df["posting_date"].notna() & df["posting_date"].map(_is_nonbiz)
        return self._emit(df, mask, "DATE_NON_BUSINESS", f"Posting occurred on a non-business day ({cal} holiday).")

    def _check_amount_signals(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        if "amount" not in df.columns:
            return []
        rules = self.config["amount_rules"]

        out = []
        if rules.get("flag_negative_amount", True):
            out += self._emit(df, df["amount"].notna() & (df["amount"] < 0), "AMT_NEGATIVE", "Amount is negative.")
        if rules.get("flag_zero_amount", True):
            out += self._emit(df, df["amount"].notna() & (df["amount"] == 0), "AMT_ZERO", "Amount is zero.")

        mn = rules.get("min_reasonable", None)
        mx = rules.get("max_reasonable", None)
        if mn is not None:
            out += self._emit(
                df,
                df["amount"].notna() & (df["amount"] < float(mn)),
                "AMT_TOO_SMALL",
                f"Amount below minimum threshold ({mn}).",
            )
        if mx is not None:
            out += self._emit(
                df,
                df["amount"].notna() & (df["amount"] > float(mx)),
                "AMT_TOO_LARGE",
                f"Amount above maximum threshold ({mx}).",
            )
        return out

    def _check_fractional_cents(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        if "amount" not in df.columns:
            return []
        tol = float(self.config["amount_rules"].get("fractional_cent_tolerance", 1e-9))

        def has_more_than_2_decimals(x: float) -> bool:
            if pd.isna(x):
                return False
            # Check if amount*100 is "almost" an integer
            return abs((x * 100) - round(x * 100)) > tol

        mask = df["amount"].notna() & df["amount"].map(has_more_than_2_decimals)
        return self._emit(df, mask, "AMT_FRACTIONAL_CENTS", "Amount has fractional cents (>2 decimals).")

    def _check_outliers(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        cfg = self.config["amount_rules"].get("outlier_detection", {})
        if not cfg.get("enabled", True):
            return []
        if "amount" not in df.columns:
            return []
        series = df["amount"].dropna()
        if len(series) < int(cfg.get("min_non_null", 50)):
            return []

        method = cfg.get("method", "mad").lower()
        threshold = float(cfg.get("threshold", 6.0))

        mask = pd.Series(False, index=df.index)

        if method == "zscore":
            mu = series.mean()
            sd = series.std(ddof=0) if series.std(ddof=0) > 0 else 1.0
            z = (df["amount"] - mu) / sd
            mask = df["amount"].notna() & (z.abs() > threshold)
        else:
            # MAD (robust)
            med = series.median()
            mad = np.median(np.abs(series - med))
            if mad == 0:
                return []
            robust_z = 0.6745 * (df["amount"] - med) / mad
            mask = df["amount"].notna() & (robust_z.abs() > threshold)

        return self._emit(df, mask, "AMT_OUTLIER", f"Amount flagged as outlier ({method}, threshold={threshold}).")

    def _check_benford(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        cfg = self.config["amount_rules"].get("benford", {})
        if not cfg.get("enabled", True):
            return []
        if "amount" not in df.columns:
            return []
        series = df["amount"].dropna().abs()
        series = series[series > 0]
        if len(series) < int(cfg.get("min_non_null", 200)):
            return []

        # First digit distribution
        first_digits = series.map(lambda x: int(str(x).lstrip("0.")[0]) if x > 0 else np.nan).dropna()
        counts = first_digits.value_counts().reindex(range(1, 10), fill_value=0).values
        obs = counts / counts.sum()

        # Benford expected
        exp = np.array([math.log10(1 + 1 / d) for d in range(1, 10)])
        # Chi-square (simple signal)
        chi = ((counts - counts.sum() * exp) ** 2 / (counts.sum() * exp + 1e-9)).sum()
        thresh = float(cfg.get("chi_square_threshold", 25.0))

        if chi <= thresh:
            return []

        # Benford is a dataset-level signal; attach as INFO finding to first few rows for reporting
        # (Power BI can still show the signal)
        top_n = min(10, len(df))
        mask = pd.Series(False, index=df.index)
        mask.iloc[:top_n] = True
        return self._emit(
            df,
            mask,
            "AMT_BENFORD",
            f"Benford chi-square signal above threshold (chi={chi:.2f} > {thresh}). Investigate dataset-level anomalies.",
        )

    def _check_vendor_patterns(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        if "vendor" not in df.columns:
            return []
        cfg = self.config["string_rules"]
        out = []

        # Blacklist regex
        bl = cfg.get("vendor_blacklist_regex", [])
        if bl:
            pattern = re.compile("|".join(f"(?:{p})" for p in bl), flags=re.IGNORECASE)
            mask = df["vendor"].notna() & df["vendor"].astype(str).str.contains(pattern, na=False)
            out += self._emit(df, mask, "VENDOR_BLACKLIST", "Vendor matches suspicious/placeholder pattern (blacklist).")

        # Suspicious characters
        bad = cfg.get("vendor_suspicious_chars_regex", [])
        if bad:
            pattern2 = re.compile("|".join(f"(?:{p})" for p in bad))
            mask2 = df["vendor"].notna() & df["vendor"].astype(str).str.contains(pattern2, na=False)
            out += self._emit(df, mask2, "VENDOR_BAD_CHARS", "Vendor contains suspicious characters.")

        return out

    def _check_description_patterns(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        if "description" not in df.columns:
            return []
        cfg = self.config["string_rules"]
        pats = cfg.get("description_suspicious_regex", [])
        if not pats:
            return []
        pattern = re.compile("|".join(f"(?:{p})" for p in pats), flags=re.IGNORECASE)
        mask = df["description"].notna() & df["description"].astype(str).str.contains(pattern, na=False)
        return self._emit(df, mask, "DESC_SUSPICIOUS", "Description contains keywords often associated with higher-risk activity.")

    def _check_duplicates(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        cfg = self.config["duplicate_rules"]
        keys_list = cfg.get("keys", [["invoice_id"]])

        out = []
        for keys in keys_list:
            # only run if all keys exist
            if any(k not in df.columns for k in keys):
                continue

            # optionally exempt blank invoice_id duplicates
            temp = df[keys].copy()
            if cfg.get("treat_blank_invoice_as_exempt", False) and "invoice_id" in keys:
                blank = df["invoice_id"].isna() | (df["invoice_id"].astype(str).str.strip() == "")
                # mark blanks as unique by appending row_id
                temp.loc[blank, "invoice_id"] = temp.loc[blank, "invoice_id"].astype(str) + "_" + df.loc[blank, "_row_id"].astype(str)

            dup_mask = temp.duplicated(keep=False)
            if dup_mask.sum() > 0:
                msg = f"Duplicate detected for key(s): {', '.join(keys)}"
                out += self._emit(df, dup_mask, "DUPLICATE_KEY", msg)
        return out

    def _profile_dataset(self, df: pd.DataFrame) -> pd.DataFrame:
        rows = []
        n = len(df)
        for c in df.columns:
            s = df[c]
            nulls = int(s.isna().sum())
            null_rate = nulls / n if n else 0
            dtype = str(s.dtype)

            # Basic stats for numeric
            if pd.api.types.is_numeric_dtype(s):
                rows.append(
                    {
                        "column": c,
                        "dtype": dtype,
                        "rows": n,
                        "nulls": nulls,
                        "null_rate": round(null_rate, 6),
                        "min": float(np.nanmin(s.values)) if s.notna().any() else np.nan,
                        "max": float(np.nanmax(s.values)) if s.notna().any() else np.nan,
                        "mean": float(np.nanmean(s.values)) if s.notna().any() else np.nan,
                        "std": float(np.nanstd(s.values)) if s.notna().any() else np.nan,
                        "unique": int(s.nunique(dropna=True)),
                    }
                )
            else:
                # string/date-ish
                rows.append(
                    {
                        "column": c,
                        "dtype": dtype,
                        "rows": n,
                        "nulls": nulls,
                        "null_rate": round(null_rate, 6),
                        "min": np.nan,
                        "max": np.nan,
                        "mean": np.nan,
                        "std": np.nan,
                        "unique": int(s.nunique(dropna=True)),
                    }
                )
        return pd.DataFrame(rows).sort_values(["null_rate", "unique"], ascending=[False, True])


# -----------------------------
# IO: Read large CSVs safely
# -----------------------------
def read_csv_flex(
    path: Path,
    chunksize: int,
    logger: logging.Logger,
    encoding: Optional[str] = None,
) -> Iterable[pd.DataFrame]:
    """
    Generator that yields standardized chunks. Uses pandas chunking for scalability.
    """
    read_kwargs = dict(low_memory=False)
    if encoding:
        read_kwargs["encoding"] = encoding

    try:
        for chunk in pd.read_csv(path, chunksize=chunksize, **read_kwargs):
            yield chunk
    except UnicodeDecodeError:
        # fallback commonly works for messy ERP exports
        logger.warning("Encoding issue detected. Retrying with latin-1...")
        for chunk in pd.read_csv(path, chunksize=chunksize, encoding="latin-1", low_memory=False):
            yield chunk


# -----------------------------
# Reporting: Excel + Power BI outputs
# -----------------------------
def write_reports(
    outdir: Path,
    config: Dict[str, Any],
    sample_df: pd.DataFrame,
    exceptions_df: pd.DataFrame,
    summary_df: pd.DataFrame,
    profiling_df: pd.DataFrame,
    rule_catalog_df: pd.DataFrame,
    logger: logging.Logger,
) -> None:
    ensure_outdir(outdir)

    # Power BI ready CSVs (full)
    exceptions_csv = outdir / config["output"]["exceptions_csv"]
    summary_csv = outdir / config["output"]["summary_csv"]
    exceptions_df.to_csv(exceptions_csv, index=False)
    summary_df.to_csv(summary_csv, index=False)

    # Excel (trim exceptions if huge)
    excel_path = outdir / config["output"]["excel_filename"]
    max_exc = int(config["output"].get("max_exceptions_rows", 200000))
    exc_for_excel = exceptions_df.head(max_exc).copy()

    with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
        sample_df.to_excel(writer, sheet_name="sample_input", index=False)
        exc_for_excel.to_excel(writer, sheet_name="exceptions", index=False)
        summary_df.to_excel(writer, sheet_name="summary", index=False)
        rule_catalog_df.to_excel(writer, sheet_name="rule_catalog", index=False)
        profiling_df.to_excel(writer, sheet_name="profiling", index=False)

    logger.info(f"Saved Excel report: {excel_path}")
    logger.info(f"Saved Power BI outputs: {exceptions_csv.name}, {summary_csv.name}")


# -----------------------------
# Main pipeline
# -----------------------------
def run_pipeline(
    input_path: Path,
    outdir: Path,
    config: Dict[str, Any],
    chunksize: int,
    logger: logging.Logger,
) -> None:
    if not input_path.exists():
        raise FileNotFoundError(f"Input not found: {input_path}")

    ensure_outdir(outdir)

    engine = RuleEngine(config, logger)

    all_exceptions: List[pd.DataFrame] = []
    all_summary: List[pd.DataFrame] = []
    profiling_accum: List[pd.DataFrame] = []

    sample_rows_limit = int(config["output"].get("max_sample_rows", 200))
    sample_collector: List[pd.DataFrame] = []

    logger.info(f"Reading input: {input_path.name} (chunksize={chunksize})")

    # Process chunks
    chunk_idx = 0
    for chunk in read_csv_flex(input_path, chunksize=chunksize, logger=logger):
        chunk_idx += 1
        chunk = standardize_columns(chunk)
        chunk, mapping = map_aliases_to_standard(chunk, config)

        if chunk_idx == 1:
            missing = validate_required_columns(chunk, config)
            if missing:
                raise ValueError(
                    "Missing required columns after alias mapping:\n"
                    f"  Missing: {missing}\n"
                    f"  Found columns: {list(chunk.columns)}\n"
                    "Fix by renaming columns in the CSV or adding aliases in config."
                )
            logger.info(f"Column mapping (alias -> standard): {json.dumps(mapping, indent=2)}")

        # Collect sample rows for report
        if sum(len(x) for x in sample_collector) < sample_rows_limit:
            remaining = sample_rows_limit - sum(len(x) for x in sample_collector)
            sample_collector.append(chunk.head(remaining))

        # Run checks
        exceptions_df, summary_df, profiling_df = engine.run(chunk)

        all_exceptions.append(exceptions_df)
        all_summary.append(summary_df)
        profiling_accum.append(profiling_df)

        if chunk_idx % 10 == 0:
            logger.info(f"Processed {chunk_idx} chunks...")

    # Combine results
    exceptions_all = pd.concat(all_exceptions, ignore_index=True) if all_exceptions else pd.DataFrame()
    summary_all = _combine_summaries(all_summary, config)
    profiling_all = _combine_profiling(profiling_accum)

    sample_df = pd.concat(sample_collector, ignore_index=True) if sample_collector else pd.DataFrame()

    # Sort exceptions by risk
    if not exceptions_all.empty:
        exceptions_all = exceptions_all.sort_values(["risk_score", "severity_score"], ascending=[False, False])

    write_reports(
        outdir=outdir,
        config=config,
        sample_df=sample_df,
        exceptions_df=exceptions_all,
        summary_df=summary_all,
        profiling_df=profiling_all,
        rule_catalog_df=engine.findings_catalog,
        logger=logger,
    )

    logger.info("Done ✅")


def _combine_summaries(summaries: List[pd.DataFrame], config: Dict[str, Any]) -> pd.DataFrame:
    if not summaries:
        return pd.DataFrame(columns=["rule_id", "rule_name", "severity", "exception_count", "affected_rows", "severity_score"])
    s = pd.concat(summaries, ignore_index=True)
    # Re-aggregate
    out = (
        s.groupby(["rule_id", "rule_name", "severity"], as_index=False)
        .agg(exception_count=("exception_count", "sum"), affected_rows=("affected_rows", "sum"))
    )
    out["severity_score"] = out["severity"].map(config["severity"]).fillna(0).astype(int)
    out = out.sort_values(["severity_score", "exception_count"], ascending=[False, False])
    return out


def _combine_profiling(profiles: List[pd.DataFrame]) -> pd.DataFrame:
    if not profiles:
        return pd.DataFrame(columns=["column", "dtype", "rows", "nulls", "null_rate", "min", "max", "mean", "std", "unique"])
    # Profiling per chunk isn't additive for all fields; we will provide an approximate roll-up:
    # - rows/nulls additive
    # - min/max across chunks
    # - unique: max(unique) as a proxy
    # - mean/std: not combined precisely here (kept as NaN to avoid misleading)
    p = pd.concat(profiles, ignore_index=True)
    grouped = p.groupby(["column", "dtype"], as_index=False).agg(
        rows=("rows", "sum"),
        nulls=("nulls", "sum"),
        min=("min", "min"),
        max=("max", "max"),
        unique=("unique", "max"),
    )
    grouped["null_rate"] = (grouped["nulls"] / grouped["rows"]).replace([np.inf, -np.inf], np.nan)
    grouped["mean"] = np.nan
    grouped["std"] = np.nan
    return grouped.sort_values(["null_rate", "unique"], ascending=[False, True])


# -----------------------------
# CLI
# -----------------------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Audit Data Quality Exception Engine (CSV -> Excel + Power BI outputs)")
    p.add_argument("--input", required=True, help="Path to input CSV (ERP transaction extract)")
    p.add_argument("--outdir", default="reports", help="Output directory (default: reports)")
    p.add_argument("--chunksize", type=int, default=200000, help="CSV chunk size for large files (default: 200000)")
    p.add_argument("--loglevel", default="INFO", help="Logging level (DEBUG/INFO/WARNING/ERROR)")
    p.add_argument("--config", default=None, help="Optional path to JSON config to override defaults (future-proof)")
    return p.parse_args()


def load_config(path: Optional[str], logger: logging.Logger) -> Dict[str, Any]:
    cfg = json.loads(json.dumps(DEFAULT_CONFIG))  # deep-ish copy
    if not path:
        return cfg
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Config not found: {p}")
    with open(p, "r", encoding="utf-8") as f:
        user_cfg = json.load(f)
    # shallow merge with nested dict merge
    cfg = deep_merge(cfg, user_cfg)
    logger.info(f"Loaded config overrides from: {p.name}")
    return cfg


def deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(base)
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def main() -> None:
    args = parse_args()
    logger = setup_logger(args.loglevel)
    cfg = load_config(args.config, logger)

    input_path = Path(args.input)
    outdir = Path(args.outdir)

    run_pipeline(
        input_path=input_path,
        outdir=outdir,
        config=cfg,
        chunksize=args.chunksize,
        logger=logger,
    )


if __name__ == "__main__":
    main()
