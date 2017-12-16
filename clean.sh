#!/usr/bin/env bash
hadoop fs -rm -r crime_clean.out
hadoop fs -rm crime_clean.csv
spark-submit ColumnCleaning/CMPLNT_NUM.py crime.csv
hadoop fs -getmerge crime_clean.out crime_clean.csv
hadoop fs -rm -r crime_clean.out
hadoop fs -rm crime_clean.csv
hadoop fs -put crime_clean.csv
spark-submit ColumnCleaning/CMPLNT_FR_DT.py crime_clean.csv
hadoop fs -getmerge crime_clean.out crime_clean.csv
hadoop fs -rm -r crime_clean.out
hadoop fs -rm crime_clean.csv
hadoop fs -put crime_clean.csv
spark-submit ColumnCleaning/CMPLNT_FR_TM.py crime_clean.csv
hadoop fs -getmerge crime_clean.out crime_clean.csv
hadoop fs -rm -r crime_clean.out
hadoop fs -rm crime_clean.csv
hadoop fs -put crime_clean.csv
spark-submit ColumnCleaning/CMPLNT_TO_DT.py crime_clean.csv
hadoop fs -getmerge crime_clean.out crime_clean.csv
hadoop fs -rm -r crime_clean.out
hadoop fs -rm crime_clean.csv
hadoop fs -put crime_clean.csv
spark-submit ColumnCleaning/CMPLNT_TO_TM.py crime_clean.csv
hadoop fs -getmerge crime_clean.out crime_clean.csv
hadoop fs -rm -r crime_clean.out
hadoop fs -rm crime_clean.csv
hadoop fs -put crime_clean.csv
spark-submit ColumnCleaning/RPT_DT.py crime_clean.csv
hadoop fs -getmerge crime_clean.out crime_clean.csv
hadoop fs -rm -r crime_clean.out
hadoop fs -rm crime_clean.csv
hadoop fs -put crime_clean.csv
spark-submit ColumnCleaning/KY_CD.py crime_clean.csv
hadoop fs -getmerge crime_clean.out crime_clean.csv
hadoop fs -rm -r crime_clean.out
hadoop fs -rm crime_clean.csv
hadoop fs -put crime_clean.csv
spark-submit ColumnCleaning/OFNS_DESC.py crime_clean.csv
hadoop fs -getmerge crime_clean.out crime_clean.csv
hadoop fs -rm -r crime_clean.out
hadoop fs -rm crime_clean.csv
hadoop fs -put crime_clean.csv
spark-submit ColumnCleaning/PD_CD.py crime_clean.csv
hadoop fs -getmerge crime_clean.out crime_clean.csv
hadoop fs -rm -r crime_clean.out
hadoop fs -rm crime_clean.csv
hadoop fs -put crime_clean.csv
spark-submit ColumnCleaning/PD_DESC.py crime_clean.csv
hadoop fs -getmerge crime_clean.out crime_clean.csv
hadoop fs -rm -r crime_clean.out
hadoop fs -rm crime_clean.csv
hadoop fs -put crime_clean.csv
spark-submit ColumnCleaning/CRM_ATPT_CPTD_CD.py crime_clean.csv
hadoop fs -getmerge crime_clean.out crime_clean.csv
hadoop fs -rm -r crime_clean.out
hadoop fs -rm crime_clean.csv
hadoop fs -put crime_clean.csv
spark-submit ColumnCleaning/LAW_CAT_CD.py crime_clean.csv
hadoop fs -getmerge crime_clean.out crime_clean.csv
hadoop fs -rm -r crime_clean.out
hadoop fs -rm crime_clean.csv
hadoop fs -put crime_clean.csv
spark-submit ColumnCleaning/JURIS_DESC.py crime_clean.csv
hadoop fs -getmerge crime_clean.out crime_clean.csv
hadoop fs -rm -r crime_clean.out
hadoop fs -rm crime_clean.csv
hadoop fs -put crime_clean.csv
spark-submit ColumnCleaning/BORO_NM.py crime_clean.csv
hadoop fs -getmerge crime_clean.out crime_clean.csv
hadoop fs -rm -r crime_clean.out
hadoop fs -rm crime_clean.csv
hadoop fs -put crime_clean.csv
spark-submit ColumnCleaning/ADDR_PCT_CD.py crime_clean.csv
hadoop fs -getmerge crime_clean.out crime_clean.csv
hadoop fs -rm -r crime_clean.out
hadoop fs -rm crime_clean.csv
hadoop fs -put crime_clean.csv
spark-submit ColumnCleaning/LOC_OF_OCCUR_DESC.py crime_clean.csv
hadoop fs -getmerge crime_clean.out crime_clean.csv
hadoop fs -rm -r crime_clean.out
hadoop fs -rm crime_clean.csv
hadoop fs -put crime_clean.csv
spark-submit ColumnCleaning/PREM_TYP_DESC.py crime_clean.csv
hadoop fs -getmerge crime_clean.out crime_clean.csv
hadoop fs -rm -r crime_clean.out
hadoop fs -rm crime_clean.csv
hadoop fs -put crime_clean.csv
spark-submit ColumnCleaning/PARKS_NM.py crime_clean.csv
hadoop fs -getmerge crime_clean.out crime_clean.csv
hadoop fs -rm -r crime_clean.out
hadoop fs -rm crime_clean.csv
hadoop fs -put crime_clean.csv
spark-submit ColumnCleaning/HADEVELOPT.py crime_clean.csv
hadoop fs -getmerge crime_clean.out crime_clean.csv
hadoop fs -rm -r crime_clean.out
hadoop fs -rm crime_clean.csv
hadoop fs -put crime_clean.csv
