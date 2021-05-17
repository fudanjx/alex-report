# Alexandra Hospital Report Automation
This repo keeps all the python scipts which use to generate the report for MOH and internal monthly statistics publication.
* Aggregate_Raw.py will help to aggregate all the six major data extraction from SAP, include inflight, admisison, discharge, procedure, UCC and SOC attendence.
* data_prep.py keep the reuseable functions which use to map the index keys as well as to clean the raw data and date format.
* Inpatient_reproting.py will aggregate, combine and summarize the data from inflight, admisison, discharge to generate ~20+ inpatient statistics reports
* Combine_email_BIS.py is to extract all the bed in service (BIS) information from daily BMU email notification.  User need to save the email message as .msg for this program to extract.
* path.py is the module to keep all the neccessary data folders, include WIP, Report, Raw_Download, as well as lookup. User can easily modify the directory in one single place.

More scripts will be added soon, which include:
* Procedure, SOC, and UCC
* Generation of the discrepency report
* Checking irregularity and highlight
* Report formatting
* On-going maintenance log
