![](logo_transp_med.png)
# The pipelite Project
The purpose of this solution is to build and execute Data Pipelines. Nothing new under the sun of course, however it aims to make this task very simple and by just using the configuration to build those pipelines.  
**So in short ... pipelite is a Data Pipeline solution by design !**  
The way this solution is built is also totally extensible and enables all developers to extend its capabilities by addin new Data connectors and/or data pipelite.transformers.  

## Currently this solution provides data access and load from these data sources :  
✅  [External file (csv)](https://github.com/datacorner/pipelite/wiki/csvFileDS)  
✅  [External Excel Spreadsheet (xls, xlsx, xlsm, xlsb, odf, ods and odt) (read only)](https://github.com/datacorner/pipelite/wiki/excelFileDS)  
✅  [External XES File (read only)](https://github.com/datacorner/pipelite/wiki/xesFileDS)  
✅  [ODBC Data Sources (checked with SQL Server, SQLite) by using an configurable SQL query (Read Only)](https://github.com/datacorner/pipelite/wiki/odbcDS)  
✅  SAP Read Table via SAP RFC (Read Only)  
✅  ABBYY Timeline PI (write only in Repository)  

## And provides those transformers  
✅ Change the Data Sources names (input/output) !  
✅ Concat 2 Data sources  
✅ Lookup with 2 data sources  
✅ Joins (left, right, outer or inner) joins    
✅ Substring from a column  
✅ Changing column names  
✅ Managing column transformation (by using jinja)  
✅ Dataset profiling

[Jump to the wiki from here](https://github.com/datacorner/pipelite/wiki)

# Installation

just use pip by typing
```
    pip install pipelite
```
