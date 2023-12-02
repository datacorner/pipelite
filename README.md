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
✅  [SAP Read Table via SAP RFC (Read Only)](https://github.com/datacorner/pipelite/wiki/sapDS)  
✅  [ABBYY Timeline PI (write only in Repository)](https://github.com/datacorner/pipelite/wiki/piDS)  

## And provides those transformers  
✅ [Pass Through (Ex. just to change the Data Sources names IN-OUT)](https://github.com/datacorner/pipelite/wiki/passthroughTR)  
✅ [Dataset Profiling](https://github.com/datacorner/pipelite/wiki/profileTR)  
✅ [Concat 2 Data sources](https://github.com/datacorner/pipelite/wiki/concatTR)  
✅ [Join data sources](https://github.com/datacorner/pipelite/wiki/joinTR)  
✅ [SubString](https://github.com/datacorner/pipelite/wiki/extractstrTR)  
✅ [Rename Column Name](https://github.com/datacorner/pipelite/wiki/renamecolTR)  
✅ [Column Transformation](https://github.com/datacorner/pipelite/wiki/jinjaTR)  

❓ [Jump to the wiki from here](https://github.com/datacorner/pipelite/wiki)❓

# Installation

just use pip by typing
```
    pip install pipelite
```
# ... and use it !

[See here](https://github.com/datacorner/pipelite/wiki/run)