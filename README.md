![](logo_transp_med.png)
# The pipelite Project
Empower your data workflows effortlessly with **pipelite**, a lightweight Python program designed for seamless **data pipeline creation and execution**. Using a simple JSON configuration, users can build complex pipelines without writing code. What sets pipelite apart is its total extensibility—anyone can easily create and integrate new connectors or transformations, enhancing the program's capabilities. 

It's also possible to add new way to manage the flow of the transformations if needed. With a MIT license fostering collaboration, this flexible tool is perfect for users of all levels. Craft, execute, and extend your data pipelines with **pipelite**, your go-to solution for adaptable and scalable data processing.

**Some characteristics:**
* Simple JSON configuration
* Lightweight and code-free (MIT license for flexibility)
* Python Code (leverage the basics libraries instead addind many heavy and complex libs) 
* Effortless pipeline creation and high integrability thanks to the json configuration
* Streamlined execution process
* Total extensibility (connectivity, transformation, pipeline management)
* Boost data processing efficiency
* Quick learning curve
* Empower your data workflows in a simple way

So in one word ... pipelite is your extensible solution for dynamic data pipelines.

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
# ... and just use it !

[See here](https://github.com/datacorner/pipelite/wiki/run)

# External modules

* **[jinja2](https://pypi.org/project/Jinja2/)** BSD-3-Clause license
* **[jsonschema](https://pypi.org/project/jsonschema/)** MIT License (MIT)
* **[jsonpath-ng](https://pypi.org/project/jsonpath-ng/)** Apache Software License (Apache 2.0)
* **[xmltodict](https://pypi.org/project/xmltodict/)** MIT License (MIT)
* **[requests](https://pypi.org/project/requests/)** Apache Software License (Apache 2.0)
* **[pyrfc](https://pypi.org/project/pyrfc/)** Apache Software License (Apache 2.0)
* **[pyodbc](https://pypi.org/project/pyodbc/)** MIT License (MIT)
* **[openpyxl](https://pypi.org/project/openpyxl/)** MIT License (MIT)
* **[pandas](https://pypi.org/project/pandas/)** BSD 3-Clause License