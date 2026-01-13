# Audit Data Quality Exception Engine

A Python-based audit analytics engine that applies configurable data quality and compliance validation rules to large-scale transaction datasets and generates audit-ready exception reports and Power BI–ready outputs for efficient review.

---

## Purpose

Audit and compliance teams routinely receive high-volume transaction extracts from ERP systems such as **SAP**, **Oracle**, and **Workday**.  
Manual validation of these datasets is time-consuming, inconsistent, and prone to human error.

This project automates common audit data quality and risk checks, enabling faster identification of anomalies, improved consistency in audit procedures, and reduced manual effort during audit and compliance reviews.

---

## Checks Included

- Negative or zero transaction amounts  
- Missing or blank vendor names  
- Weekend or non-business day postings  
- Duplicate invoice or document identifiers  
- Configurable threshold-based validations (extensible)

---

## Input Format (CSV)

Expected input files contain the following standardized columns:

- 'invoice_id'
- 'vendor'
- 'amount'
- 'posting_date' (YYYY-MM-DD)

---

## Output

The engine generates an **Excel-based exception report** containing:

- **Sample Input** – preview of source transaction data  
- **Exceptions** – flagged records with the corresponding rule name  
- **Summary** – aggregated count of exceptions per rule for audit review  

Outputs are designed to be **Power BI–ready** for KPI tracking and trend analysis.

---

## Use Cases

- Continuous auditing and compliance analytics  
- Audit readiness and data quality assurance  
- Risk-focused transaction testing  
- Automation of repetitive audit procedures  

---

## Technology Stack

- **Python** (Pandas, NumPy, OpenPyXL)
- **Excel-based reporting**
- **Power BI–ready outputs**
- **Git** version control

---

## Future Enhancements

- Rule configuration via YAML/JSON  
- Severity scoring for risk prioritization  
- Power BI dashboard integration  
- Scheduling and automation support  

---
