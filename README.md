# SQL Data Integration Tool

## Tags
#PyQt5, #SQL, #DataIntegration, #ExcelImport, #DynamicReports, #Matplotlib, #Seaborn, #DatabaseManagement, #Logging, #Windows

## Description
The SQL Data Integration Tool is a PyQt5-based application designed for integrating, managing, and visualizing data in SQL Server databases. This tool lets you import data from Excel files, update SQL tables, execute custom queries, and generate dynamic reports with graphs—all while providing a user-friendly interface for SQL Server administration.

## Features
### SQL Server Connection:
- Login dialog for entering server address and authentication details.
- Supports both Windows and SQL Server Authentication.

### Database & Table Management:
- Retrieve accessible databases and tables.
- Display table designs including columns and primary keys.

### Data Import:
- Import data from Excel files (.xlsx, .xls, .xlsm).
- Map Excel columns to SQL table columns with preview and progress tracking.
- Insert data into SQL tables.

### Data Update:
- Update existing records in SQL tables using data from Excel.
- Configure column mappings and select a primary key for record identification.

### Query Execution:
- Write and execute custom SQL queries.
- View query results with syntax highlighting and autocompletion.
- Save and load frequently used queries.

### Dynamic Reports:
- Generate graphs (Bar, Line, Pie) based on query results using Matplotlib and Seaborn.
- Export reports as PDF or PNG files.

### Logging:
- All errors and events are logged to `data_integration_errors.log` for troubleshooting.

## Setup Instructions
### Install Dependencies:
Ensure you have Python 3.x installed and run:

```sh
pip install pandas pyodbc qdarkstyle matplotlib seaborn PyQt5
```
