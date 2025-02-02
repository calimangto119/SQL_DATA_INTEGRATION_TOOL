import sys
import pandas as pd
import pyodbc
import qdarkstyle
import logging
import json
import os
import matplotlib.pyplot as plt
import seaborn as sns
from PyQt5 import QtWidgets
from PyQt5.QtCore import Qt, QRegularExpression
from PyQt5.QtGui import QSyntaxHighlighter, QTextCharFormat, QColor, QFont, QPixmap
from PyQt5.QtWidgets import (
    QFileDialog, QComboBox, QLabel, QPushButton, QVBoxLayout,
    QHBoxLayout, QTableWidget, QTableWidgetItem, QGridLayout, QWidget,
    QInputDialog, QMessageBox, QProgressBar, QPlainTextEdit, QTextEdit, QTabWidget,
    QScrollArea, QAbstractScrollArea, QSizePolicy, QSplitter, QCompleter,
    QDialog, QLineEdit, QRadioButton, QButtonGroup, QFormLayout
)

# Configure logging
logging.basicConfig(
    filename="data_integration_errors.log",
    filemode="a",
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.ERROR
)


class LoginDialog(QDialog):
    """A dialog to capture SQL Server connection details."""

    def __init__(self, parent=None):
        super(LoginDialog, self).__init__(parent)
        self.setWindowTitle("SQL Server Login")
        self.setModal(True)
        self.init_ui()

    def init_ui(self):
        """Initialize the login dialog UI."""
        layout = QFormLayout()

        # Server Address
        self.server_input = QLineEdit()
        self.server_input.setPlaceholderText("e.g., localhost or server_name\\instance")
        layout.addRow("Server Address:", self.server_input)

        # Authentication Method
        self.auth_group = QButtonGroup(self)
        self.windows_auth_radio = QRadioButton("Windows Authentication")
        self.sql_auth_radio = QRadioButton("SQL Server Authentication")
        self.windows_auth_radio.setChecked(True)
        self.auth_group.addButton(self.windows_auth_radio)
        self.auth_group.addButton(self.sql_auth_radio)

        auth_layout = QHBoxLayout()
        auth_layout.addWidget(self.windows_auth_radio)
        auth_layout.addWidget(self.sql_auth_radio)
        layout.addRow("Authentication:", auth_layout)

        # Username and Password (default hidden)
        self.username_input = QLineEdit()
        self.username_input.setPlaceholderText("Enter username")
        self.password_input = QLineEdit()
        self.password_input.setPlaceholderText("Enter password")
        self.password_input.setEchoMode(QLineEdit.Password)

        self.username_input.setEnabled(False)
        self.password_input.setEnabled(False)

        layout.addRow("Username:", self.username_input)
        layout.addRow("Password:", self.password_input)

        # Connect radio buttons to toggle authentication fields
        self.windows_auth_radio.toggled.connect(self.toggle_auth_fields)

        # Buttons
        button_layout = QHBoxLayout()
        self.login_button = QPushButton("Login")
        self.cancel_button = QPushButton("Cancel")
        self.login_button.clicked.connect(self.accept)
        self.cancel_button.clicked.connect(self.reject)
        button_layout.addWidget(self.login_button)
        button_layout.addWidget(self.cancel_button)

        layout.addRow(button_layout)

        self.setLayout(layout)

    def toggle_auth_fields(self):
        """Enable or disable username/password fields based on authentication method."""
        if self.sql_auth_radio.isChecked():
            self.username_input.setEnabled(True)
            self.password_input.setEnabled(True)
        else:
            self.username_input.setEnabled(False)
            self.password_input.setEnabled(False)

    def get_credentials(self):
        """Retrieve the entered credentials."""
        server = self.server_input.text().strip()
        auth_method = "Windows" if self.windows_auth_radio.isChecked() else "SQL"
        username = self.username_input.text().strip()
        password = self.password_input.text()
        return server, auth_method, username, password


class SQLManager:
    def __init__(self, server: str, auth_method: str, username: str = '', password: str = ''):
        self.server = server
        self.auth_method = auth_method
        self.username = username
        self.password = password
        self.connect()

    def connect(self):
        """Establish a connection to the SQL Server."""
        try:
            if self.auth_method == "Windows":
                conn_str = (
                    f"Driver={{ODBC Driver 17 for SQL Server}};"
                    f"Server={self.server};"
                    f"Trusted_Connection=yes;"
                )
            else:
                conn_str = (
                    f"Driver={{ODBC Driver 17 for SQL Server}};"
                    f"Server={self.server};"
                    f"UID={self.username};"
                    f"PWD={self.password};"
                )
            self.conn = pyodbc.connect(conn_str, timeout=5)
            self.cursor = self.conn.cursor()
            logging.info(f"Connected to SQL Server: {self.server} using {self.auth_method} Authentication.")
        except pyodbc.InterfaceError as e:
            self.log_and_exit("Connection Error", "Failed to connect to SQL Server. Please verify the server address and network connectivity.")
        except pyodbc.Error as e:
            self.log_and_exit("Connection Error", f"Failed to connect to SQL Server: {e}")

    def log_and_exit(self, title: str, message: str):
        """Log error message and exit application."""
        QMessageBox.critical(None, title, message)
        logging.error(message)
        sys.exit(1)

    def set_database(self, database: str):
        """Set the active database for the connection."""
        try:
            self.cursor.execute(f"USE [{database}]")
            logging.info(f"Switched to database: {database}")
        except pyodbc.Error as e:
            logging.error(f"Failed to switch to database {database}: {e}")
            raise RuntimeError(f"Failed to switch to database {database}: {e}")

    def get_databases(self) -> list:
        """Retrieve a list of databases on the server that the user has read access to."""
        try:
            self.cursor.execute("""
                SELECT d.name 
                FROM sys.databases d
                WHERE HAS_DBACCESS(d.name) = 1
                  AND d.database_id NOT IN (1,2,3)  -- Optionally exclude system databases
                ORDER BY d.name
            """)
            accessible_databases = [row.name for row in self.cursor.fetchall()]
            logging.info(f"Retrieved {len(accessible_databases)} accessible databases.")
            return accessible_databases
        except pyodbc.Error as e:
            logging.error(f"Failed to retrieve databases: {e}")
            return []


    def get_tables(self, database: str) -> list:
        """Retrieve a list of tables in the selected database."""
        try:
            self.set_database(database)
            self.cursor.execute("SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
            return [f"{row.TABLE_SCHEMA}.{row.TABLE_NAME}" for row in self.cursor.fetchall()]
        except pyodbc.Error as e:
            logging.error(f"Failed to retrieve tables from {database}: {e}")
            return []

    def get_table_design(self, database: str, table: str) -> dict:
        """Retrieve the design (columns) of a specified table."""
        try:
            self.set_database(database)
            schema, table_name = table.split('.')
            self.cursor.execute(f"""
                SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = '{table_name}' AND TABLE_SCHEMA = '{schema}'
            """)
            return {row.COLUMN_NAME: {'type': row.DATA_TYPE, 'nullable': row.IS_NULLABLE == 'YES'} for row in self.cursor.fetchall()}
        except pyodbc.Error as e:
            logging.error(f"Failed to retrieve table design for {table} in {database}: {e}")
            return {}

    def get_table_columns(self, database: str, table: str) -> list:
        """Retrieve column details of a specified table."""
        try:
            self.set_database(database)
            schema, table_name = table.split('.')
            self.cursor.execute(f"""
                SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = '{table_name}' AND TABLE_SCHEMA = '{schema}'
            """)
            columns = []
            for row in self.cursor.fetchall():
                column = {
                    'COLUMN_NAME': row.COLUMN_NAME,
                    'DATA_TYPE': row.DATA_TYPE,
                    'IS_NULLABLE': row.IS_NULLABLE,
                    'COLUMN_DEFAULT': row.COLUMN_DEFAULT
                }
                columns.append(column)
            return columns
        except pyodbc.Error as e:
            logging.error(f"Failed to retrieve columns for {table} in {database}: {e}")
            return []

    def get_primary_keys(self, database: str, table: str) -> list:
        """Retrieve primary key columns of a specified table."""
        try:
            self.set_database(database)
            schema, table_name = table.split('.')
            self.cursor.execute(f"""
                SELECT KU.COLUMN_NAME
                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC
                JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU
                    ON TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME
                WHERE TC.TABLE_NAME = '{table_name}' 
                    AND TC.TABLE_SCHEMA = '{schema}'
                    AND TC.CONSTRAINT_TYPE = 'PRIMARY KEY'
            """)
            return [row.COLUMN_NAME for row in self.cursor.fetchall()]
        except pyodbc.Error as e:
            logging.error(f"Failed to retrieve primary keys for {table} in {database}: {e}")
            return []

    def insert_data(self, database: str, table: str, column_mapping: dict, data: list, progress_callback=None):
        """Insert data into the specified table."""
        try:
            self.set_database(database)
            schema, table_name = table.split('.')
            columns = ', '.join([f"[{col}]" for col in column_mapping.values()])
            placeholders = ', '.join(['?'] * len(column_mapping))
            query = f"INSERT INTO [{schema}].[{table_name}] ({columns}) VALUES ({placeholders})"
            logging.debug(f"SQL Query: {query}")

            for i, record in enumerate(data):
                mapped_row = [record.get(sql_col, None) for sql_col in column_mapping.values()]
                logging.debug(f"Inserting Row {i+1}: {mapped_row}")

                try:
                    self.cursor.execute(query, mapped_row)
                    if progress_callback:
                        progress_callback(i + 1)
                except pyodbc.Error as e:
                    logging.error(f"Failed to insert row: {record} | Error: {e}")
                    raise e

            self.conn.commit()
            logging.info(f"Successfully inserted {len(data)} records into {table} in {database}.")
        except Exception as e:
            logging.error(f"Error inserting data into {table} on {database}: {e}")
            raise RuntimeError(f"Error inserting data into {table} on {database}: {e}")

    def execute_query(self, database: str, query: str):
        """Execute a SQL query in the specified database and return results and column names."""
        try:
            self.set_database(database)  # Set the database context
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            column_names = [desc[0] for desc in self.cursor.description]
            logging.info(f"Executed query on {database}: {query}")
            return rows, column_names
        except pyodbc.Error as e:
            logging.error(f"Query execution failed: {e}")
            raise RuntimeError(f"Query execution failed: {e}")

    def update_data(self, database: str, table: str, column_mapping: dict, data: list, identifier_column: str, progress_callback=None):
        """Update data in the specified table based on an identifier column."""
        try:
            self.set_database(database)
            schema, table_name = table.split('.')
            set_clause = ', '.join([f"[{col}] = ?" for col in column_mapping.values() if col != identifier_column])
            query = f"UPDATE [{schema}].[{table_name}] SET {set_clause} WHERE [{identifier_column}] = ?"
            logging.debug(f"SQL Query: {query}")

            for i, record in enumerate(data):
                update_values = [record.get(col, None) for col in column_mapping.values() if col != identifier_column]
                identifier_value = record.get(identifier_column, None)

                if identifier_value is None:
                    logging.error(f"Identifier value missing in record: {record}")
                    continue

                update_values.append(identifier_value)
                try:
                    self.cursor.execute(query, update_values)
                    if progress_callback:
                        progress_callback(i + 1)
                except pyodbc.Error as e:
                    logging.error(f"Failed to update row: {record} | Error: {e}")
                    raise e

            self.conn.commit()
            logging.info(f"Successfully updated {len(data)} records in {table} in {database}.")
        except Exception as e:
            logging.error(f"Error updating data in {table} on {database}: {e}")
            raise RuntimeError(f"Error updating data in {table} on {database}: {e}")


class QueryEditor(QPlainTextEdit):
    """Custom QPlainTextEdit with autocompletion support."""

    def __init__(self, completer=None, parent=None):
        super(QueryEditor, self).__init__(parent)
        self.completer = completer

        if self.completer:
            self.completer.setWidget(self)
            self.completer.setCompletionMode(QCompleter.PopupCompletion)
            self.completer.setCaseSensitivity(Qt.CaseInsensitive)
            self.completer.activated.connect(self.insert_completion)

    def insert_completion(self, completion):
        """Insert the selected completion into the editor."""
        tc = self.textCursor()
        extra = len(completion) - len(self.completer.completionPrefix())
        tc.movePosition(tc.Left)
        tc.movePosition(tc.EndOfWord)
        tc.insertText(completion[-extra:])
        self.setTextCursor(tc)

    def textUnderCursor(self):
        """Get the text currently under the cursor."""
        tc = self.textCursor()
        tc.select(tc.WordUnderCursor)
        return tc.selectedText()

    def keyPressEvent(self, event):
        """Handle key press events to manage the completer."""
        if self.completer and self.completer.popup().isVisible():
            # The following keys are forwarded by the completer to the widget
            if event.key() in (Qt.Key_Enter, Qt.Key_Return, Qt.Key_Escape,
                               Qt.Key_Tab, Qt.Key_Backtab):
                event.ignore()
                return  # let the completer handle these keys

        super(QueryEditor, self).keyPressEvent(event)

        ctrl_or_shift = event.modifiers() & (Qt.ControlModifier | Qt.ShiftModifier)
        if ctrl_or_shift and event.text() == '':
            return

        eow = "~!@#$%^&*()+{}|:\"<>?,./;'[]\\-="  # end of word
        has_modifier = (event.modifiers() != Qt.NoModifier) and not ctrl_or_shift

        completion_prefix = self.textUnderCursor()

        if not has_modifier and (event.text() in eow or len(completion_prefix) < 1):
            self.completer.popup().hide()
            return

        if completion_prefix != self.completer.completionPrefix():
            self.completer.setCompletionPrefix(completion_prefix)
            self.completer.popup().setCurrentIndex(
                self.completer.completionModel().index(0, 0))
        
        cr = self.cursorRect()
        cr.setWidth(self.completer.popup().sizeHintForColumn(0)
                    + self.completer.popup().verticalScrollBar().sizeHint().width())
        self.completer.complete(cr)  # popup it up!


class SqlHighlighter(QSyntaxHighlighter):
    def __init__(self, parent=None):
        super(SqlHighlighter, self).__init__(parent)
        self.highlighting_rules = []

        # Define SQL keywords
        keyword_format = QTextCharFormat()
        keyword_format.setForeground(QColor("blue"))
        keyword_format.setFontWeight(QFont.Bold)
        keywords = [
            "SELECT", "FROM", "WHERE", "INSERT", "UPDATE", "DELETE", "JOIN",
            "INNER", "LEFT", "RIGHT", "FULL", "OUTER", "ON", "AS", "IN",
            "AND", "OR", "NOT", "NULL", "VALUES", "CREATE", "TABLE",
            "ALTER", "DROP", "GROUP BY", "ORDER BY", "HAVING", "DISTINCT",
            "LIMIT", "OFFSET", "COUNT", "AVG", "YEAR", "MONTH"
        ]
        for word in keywords:
            pattern = QRegularExpression(r"\b" + word + r"\b", QRegularExpression.PatternOption.CaseInsensitiveOption)
            self.highlighting_rules.append((pattern, keyword_format))

        # Define string literals
        string_format = QTextCharFormat()
        string_format.setForeground(QColor("magenta"))
        string_pattern = QRegularExpression(r"'[^']*'")
        self.highlighting_rules.append((string_pattern, string_format))

        # Define comments
        comment_format = QTextCharFormat()
        comment_format.setForeground(QColor("green"))
        comment_pattern = QRegularExpression(r"--[^\n]*")
        self.highlighting_rules.append((comment_pattern, comment_format))

    def highlightBlock(self, text):
        for pattern, fmt in self.highlighting_rules:
            match_iterator = pattern.globalMatch(text)
            while match_iterator.hasNext():
                match = match_iterator.next()
                start = match.capturedStart()
                length = match.capturedLength()
                self.setFormat(start, length, fmt)


class DataIntegrationTool(QWidget):
    def __init__(self, sql_manager: SQLManager):
        super().__init__()
        self.sql_manager = sql_manager
        self.excel_data = None
        self.column_mapping = {}
        self.table_design = {}
        self.last_query_results = None
        self.last_query_columns = None
        self.current_dynamic_fig = None  # To store the current graph figure
        self.saved_queries = {}
        self.load_queries_from_file()
        self.init_ui()

    def init_ui(self):
        """Initialize the user interface."""
        self.tabs = QTabWidget()

        # Data Import Tab
        self.data_import_tab = QWidget()
        self.init_data_import_ui()
        self.tabs.addTab(self.data_import_tab, "Data Import")

        # Update Data Tab
        self.update_data_tab = QWidget()
        self.init_update_data_ui()
        self.tabs.addTab(self.update_data_tab, "Update Data")

        # Execute Query Tab
        self.query_tab = QWidget()
        self.init_query_execution_ui()
        self.tabs.addTab(self.query_tab, "Execute Query")

        # Dynamic Reports Tab
        self.dynamic_reports_tab = QWidget()
        self.init_dynamic_reports_ui()
        self.tabs.addTab(self.dynamic_reports_tab, "Dynamic Reports")

        layout = QVBoxLayout()
        layout.addWidget(self.tabs)
        self.setLayout(layout)
        self.setWindowTitle("Data Integration Tool")
        self.resize(1200, 800)

    # ------------------ Data Import Tab ------------------

    def init_data_import_ui(self):
        """Setup the Data Import UI elements."""
        # Database and Table Selection
        self.import_database_dropdown = QComboBox()
        self.import_table_dropdown = QComboBox()

        self.import_database_label = QLabel("Select Database:")
        self.import_table_label = QLabel("Select Table:")

        self.import_database_dropdown.addItems(self.sql_manager.get_databases())
        self.import_database_dropdown.currentTextChanged.connect(self.load_import_tables)

        # File and Sheet Selection
        self.import_file_button = QPushButton("Select Excel File")
        self.import_file_button.clicked.connect(self.select_import_file)

        self.import_sheet_dropdown = QComboBox()
        self.import_sheet_dropdown.currentTextChanged.connect(self.load_import_excel_data)
        self.import_sheet_label = QLabel("Select Sheet:")

        # Column Mappings Layout
        self.import_column_mappings_layout = QGridLayout()
        self.import_column_mappings_layout.setSpacing(10)

        import_column_mapping_container = QWidget()
        import_column_mapping_container.setLayout(self.import_column_mappings_layout)

        self.import_column_mapping_scroll = QScrollArea()
        self.import_column_mapping_scroll.setWidget(import_column_mapping_container)
        self.import_column_mapping_scroll.setWidgetResizable(True)
        self.import_column_mapping_scroll.setMinimumHeight(200)

        # Data Preview
        self.import_data_preview = QTableWidget()
        self.import_data_preview.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        self.import_data_preview.setVerticalScrollMode(QtWidgets.QAbstractItemView.ScrollPerPixel)
        self.import_data_preview.setHorizontalScrollMode(QtWidgets.QAbstractItemView.ScrollPerPixel)
        self.import_data_preview.setSizeAdjustPolicy(QAbstractScrollArea.AdjustToContents)
        self.import_data_preview.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)

        # Progress Bar and Buttons
        self.import_progress_bar = QProgressBar()
        self.import_progress_bar.setValue(0)

        self.import_insert_button = QPushButton("Insert Data")
        self.import_insert_button.clicked.connect(self.insert_data_to_sql)

        self.import_reset_button = QPushButton("Reset Fields")
        self.import_reset_button.clicked.connect(self.reset_import_fields)

        # Layouts
        layout = QVBoxLayout()

        # Database and Table Layout
        db_table_layout = QHBoxLayout()
        db_table_layout.addWidget(self.import_database_label)
        db_table_layout.addWidget(self.import_database_dropdown)
        db_table_layout.addWidget(self.import_table_label)
        db_table_layout.addWidget(self.import_table_dropdown)

        # File and Sheet Layout
        file_layout = QHBoxLayout()
        file_layout.addWidget(self.import_file_button)
        file_layout.addWidget(self.import_sheet_label)
        file_layout.addWidget(self.import_sheet_dropdown)

        # Splitter for Column Mapping and Data Preview
        splitter = QSplitter(Qt.Vertical)
        splitter.addWidget(self.import_column_mapping_scroll)
        splitter.addWidget(self.import_data_preview)
        splitter.setStretchFactor(0, 1)
        splitter.setStretchFactor(1, 3)

        # Adding Widgets to Main Layout
        layout.addLayout(db_table_layout)
        layout.addLayout(file_layout)
        layout.addWidget(splitter)
        layout.addWidget(self.import_progress_bar)
        layout.addWidget(self.import_insert_button)
        layout.addWidget(self.import_reset_button)

        self.data_import_tab.setLayout(layout)

    def load_import_tables(self):
        """Load tables from the selected database into the table dropdown for import."""
        selected_database = self.import_database_dropdown.currentText()
        self.import_table_dropdown.clear()
        self.import_table_dropdown.addItems(self.sql_manager.get_tables(selected_database))

    def select_import_file(self):
        """Open a file dialog to select an Excel file for import."""
        file_path, _ = QFileDialog.getOpenFileName(self, "Select Excel File", "", "Excel Files (*.xlsx *.xls *.xlsm)")
        if file_path:
            try:
                self.excel_import_data = pd.ExcelFile(file_path)
                self.import_sheet_dropdown.clear()
                self.import_sheet_dropdown.addItems(self.excel_import_data.sheet_names)
            except Exception as e:
                logging.error(f"Failed to load Excel file for import: {e}")
                QMessageBox.critical(self, "Error", f"Failed to load Excel file: {e}")

    def load_import_excel_data(self):
        """Load data from the selected sheet for import and populate column mappings."""
        selected_sheet = self.import_sheet_dropdown.currentText()
        if selected_sheet:
            try:
                data = self.excel_import_data.parse(selected_sheet)
                self.populate_import_column_mapping(data)
                self.display_import_data_preview(data)
            except Exception as e:
                logging.error(f"Failed to read data from the import sheet: {e}")
                QMessageBox.critical(self, "Error", f"Failed to read data from the sheet: {e}")

    def populate_import_column_mapping(self, data: pd.DataFrame):
        """Populate the column mapping layout for import based on the selected table's design."""
        # Clear existing mappings
        for i in reversed(range(self.import_column_mappings_layout.count())):
            widget = self.import_column_mappings_layout.itemAt(i).widget()
            if widget is not None:
                widget.deleteLater()

        selected_database = self.import_database_dropdown.currentText()
        selected_table = self.import_table_dropdown.currentText()
        self.table_design = self.sql_manager.get_table_design(selected_database, selected_table)
        sql_columns = list(self.table_design.keys())
        sql_columns.insert(0, "Do not import")

        self.import_column_mapping = {}

        for i, excel_col in enumerate(data.columns):
            excel_label = QLabel(excel_col)
            sql_dropdown = QComboBox()
            sql_dropdown.addItems(sql_columns)

            sql_dropdown.setCurrentIndex(0)
            sql_dropdown.currentIndexChanged.connect(
                lambda idx, excel_col=excel_col, dropdown=sql_dropdown:
                self.update_import_column_mapping(excel_col, dropdown.currentText())
            )

            self.import_column_mappings_layout.addWidget(excel_label, i, 0)
            self.import_column_mappings_layout.addWidget(sql_dropdown, i, 1)

    def update_import_column_mapping(self, excel_col: str, selected_sql_col: str):
        """Update the column mapping for import based on user selection."""
        if selected_sql_col == "Do not import":
            self.import_column_mapping.pop(excel_col, None)
        else:
            self.import_column_mapping[excel_col] = selected_sql_col

    def display_import_data_preview(self, data: pd.DataFrame):
        """Display a preview of the data to be imported."""
        self.import_data_preview.clearContents()
        self.import_data_preview.setRowCount(min(len(data), 100))  # Limit to first 100 rows for performance
        self.import_data_preview.setColumnCount(len(data.columns))
        self.import_data_preview.setHorizontalHeaderLabels(data.columns)

        for row_idx, row in enumerate(data.head(100).itertuples(index=False)):
            for col_idx, value in enumerate(row):
                item = QTableWidgetItem(str(value))
                self.import_data_preview.setItem(row_idx, col_idx, item)

    def insert_data_to_sql(self):
        """Insert the mapped data from Excel into the SQL table."""
        column_mapping = {excel_col: sql_col for excel_col, sql_col in self.import_column_mapping.items() if sql_col != "Do not import"}
        logging.debug(f"Import Column Mapping: {column_mapping}")

        if not column_mapping:
            QMessageBox.critical(self, "Insert Error", "No columns selected for insertion.")
            return

        selected_sheet = self.import_sheet_dropdown.currentText()
        try:
            data = self.excel_import_data.parse(selected_sheet)
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to parse Excel sheet '{selected_sheet}': {e}")
            return

        data_records = []
        for _, row in data.iterrows():
            record = {}
            for excel_col, sql_col in column_mapping.items():
                value = row[excel_col]
                record[sql_col] = None if pd.isnull(value) else value
            data_records.append(record)

        if not data_records or all(all(value is None for value in record.values()) for record in data_records):
            QMessageBox.critical(self, "Insert Error", "Data records contain only NULL values or are empty.")
            return

        self.import_progress_bar.setMaximum(len(data_records))
        self.import_progress_bar.setValue(0)

        selected_database = self.import_database_dropdown.currentText()
        selected_table = self.import_table_dropdown.currentText()
        try:
            self.sql_manager.insert_data(
                selected_database,
                selected_table,
                column_mapping,
                data_records,
                progress_callback=self.update_import_progress
            )
            QMessageBox.information(self, "Success", "Data successfully inserted into the SQL table.")
        except RuntimeError as e:
            QMessageBox.critical(self, "Insert Error", f"Failed to insert data into SQL table:\n{str(e)}")

    def reset_import_fields(self):
        """Reset all input fields in the Data Import tab to their initial state."""
        self.import_database_dropdown.setCurrentIndex(0)
        self.import_table_dropdown.clear()
        self.import_sheet_dropdown.clear()
        self.import_data_preview.clearContents()
        self.import_column_mapping.clear()
        self.import_progress_bar.setValue(0)
        QMessageBox.information(self, "Reset", "All import fields have been reset.")

    def update_import_progress(self, value: int):
        """Update the progress bar during data insertion."""
        self.import_progress_bar.setValue(value)

    # ------------------ Update Data Tab ------------------

    def init_update_data_ui(self):
        """Setup the Update Data UI elements."""
        # Database and Table Selection
        self.update_database_dropdown = QComboBox()
        self.update_table_dropdown = QComboBox()

        self.update_database_label = QLabel("Select Database:")
        self.update_table_label = QLabel("Select Table:")

        self.update_database_dropdown.addItems(self.sql_manager.get_databases())
        self.update_database_dropdown.currentTextChanged.connect(self.load_update_tables)

        # File and Sheet Selection
        self.update_file_button = QPushButton("Select Excel File")
        self.update_file_button.clicked.connect(self.select_update_file)

        self.update_sheet_dropdown = QComboBox()
        self.update_sheet_dropdown.currentTextChanged.connect(self.load_update_excel_data)
        self.update_sheet_label = QLabel("Select Sheet:")

        # Column Mappings Layout
        self.update_column_mappings_layout = QGridLayout()
        self.update_column_mappings_layout.setSpacing(10)

        update_column_mapping_container = QWidget()
        update_column_mapping_container.setLayout(self.update_column_mappings_layout)

        self.update_column_mapping_scroll = QScrollArea()
        self.update_column_mapping_scroll.setWidget(update_column_mapping_container)
        self.update_column_mapping_scroll.setWidgetResizable(True)
        self.update_column_mapping_scroll.setMinimumHeight(200)

        # Data Preview
        self.update_data_preview = QTableWidget()
        self.update_data_preview.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        self.update_data_preview.setVerticalScrollMode(QtWidgets.QAbstractItemView.ScrollPerPixel)
        self.update_data_preview.setHorizontalScrollMode(QtWidgets.QAbstractItemView.ScrollPerPixel)
        self.update_data_preview.setSizeAdjustPolicy(QAbstractScrollArea.AdjustToContents)
        self.update_data_preview.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)

        # Progress Bar and Buttons
        self.update_progress_bar = QProgressBar()
        self.update_progress_bar.setValue(0)

        self.update_button = QPushButton("Update Data")
        self.update_button.clicked.connect(self.update_data_in_sql)

        self.update_reset_button = QPushButton("Reset Fields")
        self.update_reset_button.clicked.connect(self.reset_update_fields)

        # Layouts
        layout = QVBoxLayout()

        # Database and Table Layout
        db_table_layout = QHBoxLayout()
        db_table_layout.addWidget(self.update_database_label)
        db_table_layout.addWidget(self.update_database_dropdown)
        db_table_layout.addWidget(self.update_table_label)
        db_table_layout.addWidget(self.update_table_dropdown)

        # File and Sheet Layout
        file_layout = QHBoxLayout()
        file_layout.addWidget(self.update_file_button)
        file_layout.addWidget(self.update_sheet_label)
        file_layout.addWidget(self.update_sheet_dropdown)

        # Splitter for Column Mapping and Data Preview
        splitter = QSplitter(Qt.Vertical)
        splitter.addWidget(self.update_column_mapping_scroll)
        splitter.addWidget(self.update_data_preview)
        splitter.setStretchFactor(0, 1)
        splitter.setStretchFactor(1, 3)

        # Adding Widgets to Main Layout
        layout.addLayout(db_table_layout)
        layout.addLayout(file_layout)
        layout.addWidget(splitter)
        layout.addWidget(self.update_progress_bar)
        layout.addWidget(self.update_button)
        layout.addWidget(self.update_reset_button)

        self.update_data_tab.setLayout(layout)

    def load_update_tables(self, selected_database: str):
        """Load tables from the selected database into the table dropdown for update."""
        try:
            tables = self.sql_manager.get_tables(selected_database)
            self.update_table_dropdown.clear()
            self.update_table_dropdown.addItems(tables)
        except Exception as e:
            logging.error(f"Failed to load tables for database {selected_database}: {e}")
            QMessageBox.critical(self, "Error", f"Failed to load tables for database {selected_database}: {e}")

    def select_update_file(self):
        """Open a file dialog to select an Excel file for update."""
        file_path, _ = QFileDialog.getOpenFileName(self, "Select Excel File", "", "Excel Files (*.xlsx *.xls *.xlsm)")
        if file_path:
            try:
                self.excel_update_data = pd.ExcelFile(file_path)
                self.update_sheet_dropdown.clear()
                self.update_sheet_dropdown.addItems(self.excel_update_data.sheet_names)
            except Exception as e:
                logging.error(f"Failed to load Excel file for update: {e}")
                QMessageBox.critical(self, "Error", f"Failed to load Excel file: {e}")

    def load_update_excel_data(self):
        """Load data from the selected sheet for update and populate column mappings."""
        selected_sheet = self.update_sheet_dropdown.currentText()
        if selected_sheet:
            try:
                data = self.excel_update_data.parse(selected_sheet)
                self.populate_update_column_mapping(data)
                self.display_update_data_preview(data)
            except Exception as e:
                logging.error(f"Failed to read data from the update sheet: {e}")
                QMessageBox.critical(self, "Error", f"Failed to read data from the sheet: {e}")

    def populate_update_column_mapping(self, data: pd.DataFrame):
        """Populate the column mapping layout for update based on the selected table's design."""
        # Clear existing mappings
        for i in reversed(range(self.update_column_mappings_layout.count())):
            widget = self.update_column_mappings_layout.itemAt(i).widget()
            if widget is not None:
                widget.deleteLater()

        selected_database = self.update_database_dropdown.currentText()
        selected_table = self.update_table_dropdown.currentText()
        self.table_design = self.sql_manager.get_table_design(selected_database, selected_table)
        sql_columns = list(self.table_design.keys())
        sql_columns.insert(0, "Do not map")

        self.update_column_mapping = {}

        for i, excel_col in enumerate(data.columns):
            excel_label = QLabel(excel_col)
            sql_dropdown = QComboBox()
            sql_dropdown.addItems(sql_columns)

            sql_dropdown.setCurrentIndex(0)
            sql_dropdown.currentIndexChanged.connect(
                lambda idx, excel_col=excel_col, dropdown=sql_dropdown:
                self.update_update_column_mapping(excel_col, dropdown.currentText())
            )

            self.update_column_mappings_layout.addWidget(excel_label, i, 0)
            self.update_column_mappings_layout.addWidget(sql_dropdown, i, 1)

    def update_update_column_mapping(self, excel_col: str, selected_sql_col: str):
        """Update the column mapping for update based on user selection."""
        if selected_sql_col == "Do not map":
            self.update_column_mapping.pop(excel_col, None)
        else:
            self.update_column_mapping[excel_col] = selected_sql_col

    def display_update_data_preview(self, data: pd.DataFrame):
        """Display a preview of the data to be updated."""
        self.update_data_preview.clearContents()
        self.update_data_preview.setRowCount(min(len(data), 100))  # Limit to first 100 rows for performance
        self.update_data_preview.setColumnCount(len(data.columns))
        self.update_data_preview.setHorizontalHeaderLabels(data.columns)

        for row_idx, row in enumerate(data.head(100).itertuples(index=False)):
            for col_idx, value in enumerate(row):
                item = QTableWidgetItem(str(value))
                self.update_data_preview.setItem(row_idx, col_idx, item)

    def update_data_in_sql(self):
        """Update the mapped data in the SQL table based on a primary key."""
        column_mapping = {excel_col: sql_col for excel_col, sql_col in self.update_column_mapping.items() if sql_col != "Do not map"}
        logging.debug(f"Update Column Mapping: {column_mapping}")

        if not column_mapping:
            QMessageBox.critical(self, "Update Error", "No columns selected for update.")
            return

        identifier, ok = QInputDialog.getText(self, "Primary Key Selection", "Enter the primary key column for update:")
        if not ok or not identifier or identifier not in column_mapping.values():
            QMessageBox.critical(self, "Identifier Error", "A valid primary key column is required for updating.")
            return

        selected_sheet = self.update_sheet_dropdown.currentText()
        try:
            data = self.excel_update_data.parse(selected_sheet)
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to parse Excel sheet '{selected_sheet}': {e}")
            return

        data_records = []
        for _, row in data.iterrows():
            record = {}
            for excel_col, sql_col in column_mapping.items():
                value = row[excel_col]
                record[sql_col] = None if pd.isnull(value) else value
            data_records.append(record)

        if not data_records or all(all(value is None for value in record.values()) for record in data_records):
            QMessageBox.critical(self, "Update Error", "Data records contain only NULL values or are empty.")
            return

        self.update_progress_bar.setMaximum(len(data_records))
        self.update_progress_bar.setValue(0)

        selected_database = self.update_database_dropdown.currentText()
        selected_table = self.update_table_dropdown.currentText()
        try:
            self.sql_manager.update_data(
                selected_database,
                selected_table,
                column_mapping,
                data_records,
                identifier,
                progress_callback=self.update_update_progress
            )
            QMessageBox.information(self, "Success", "Data successfully updated in the SQL table.")
        except RuntimeError as e:
            QMessageBox.critical(self, "Update Error", f"Failed to update data in SQL table:\n{str(e)}")

    def reset_update_fields(self):
        """Reset all input fields in the Update Data tab to their initial state."""
        self.update_database_dropdown.setCurrentIndex(0)
        self.update_table_dropdown.clear()
        self.update_sheet_dropdown.clear()
        self.update_data_preview.clearContents()
        self.update_column_mapping.clear()
        self.update_progress_bar.setValue(0)
        QMessageBox.information(self, "Reset", "All update fields have been reset.")

    def update_update_progress(self, value: int):
        """Update the progress bar during data update."""
        self.update_progress_bar.setValue(value)

    # ------------------ Execute Query Tab ------------------

    def init_query_execution_ui(self):
        """Setup the Query Execution UI elements with separate dropdowns for database and table selection."""
        # Main layout for the tab
        main_layout = QHBoxLayout()

        # Left side: Query Editor and Execution
        left_layout = QVBoxLayout()

        # Saved Queries Dropdown
        saved_queries_layout = QHBoxLayout()
        self.saved_queries_dropdown = QComboBox()
        self.saved_queries_dropdown.setPlaceholderText("Select Saved Query")
        self.saved_queries_dropdown.currentIndexChanged.connect(self.load_selected_saved_query)  # Connect to load function

        saved_queries_layout.addWidget(QLabel("Saved Queries:"))
        saved_queries_layout.addWidget(self.saved_queries_dropdown)
        left_layout.addLayout(saved_queries_layout)

        # Define SQL keywords and table/column names for autocompletion
        sql_keywords = [
            "SELECT", "FROM", "WHERE", "INSERT", "UPDATE", "DELETE", "JOIN",
            "INNER", "LEFT", "RIGHT", "FULL", "OUTER", "ON", "AS", "IN",
            "AND", "OR", "NOT", "NULL", "VALUES", "CREATE", "TABLE",
            "ALTER", "DROP", "GROUP BY", "ORDER BY", "HAVING", "DISTINCT",
            "LIMIT", "OFFSET", "COUNT", "AVG", "YEAR", "MONTH"
        ]

        # Fetch all table and column names
        all_columns = []
        try:
            databases = self.sql_manager.get_databases()
            for db in databases:
                tables = self.sql_manager.get_tables(db)
                for table in tables:
                    columns = self.sql_manager.get_table_columns(db, table)
                    for col in columns:
                        all_columns.append(col['COLUMN_NAME'])
        except Exception as e:
            logging.error(f"Failed to retrieve columns for autocompletion: {e}")

        completer = QCompleter(sql_keywords + all_columns)
        completer.setCaseSensitivity(Qt.CaseInsensitive)

        # Create the QueryEditor with completer
        self.query_input = QueryEditor(completer=completer)
        self.query_input.setPlaceholderText("Enter your SQL query here...")

        # Apply SQL syntax highlighting
        self.sql_highlighter = SqlHighlighter(self.query_input.document())

        self.execute_button = QPushButton("Execute Query")
        self.execute_button.clicked.connect(self.execute_sql_query)

        # Save Query Button
        self.save_query_button = QPushButton("Save Query")
        self.save_query_button.clicked.connect(self.save_query_as)

        self.query_result_table = QTableWidget()
        self.query_result_table.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)

        left_layout.addWidget(self.query_input)
        left_layout.addWidget(self.execute_button)
        left_layout.addWidget(self.save_query_button)
        left_layout.addWidget(self.query_result_table)

        # Right side: Database and Table Selection, and Table Details
        right_layout = QVBoxLayout()

        # Database Selection Dropdown
        self.query_database_label = QLabel("Select Database:")
        self.query_database_dropdown = QComboBox()
        self.query_database_dropdown.addItems(self.sql_manager.get_databases())
        self.query_database_dropdown.currentTextChanged.connect(self.load_query_tables)

        # Table Selection Dropdown
        self.query_table_label = QLabel("Select Table:")
        self.query_table_dropdown = QComboBox()
        self.load_query_tables(self.query_database_dropdown.currentText())
        self.query_table_dropdown.currentTextChanged.connect(self.display_table_details)

        # Table Details Display with Scroll Area
        self.table_details_label = QLabel("Table Details:")
        self.table_details_text = QTextEdit()
        self.table_details_text.setReadOnly(True)

        # Wrap QTextEdit in QScrollArea
        self.table_details_scroll = QScrollArea()
        self.table_details_scroll.setWidgetResizable(True)
        self.table_details_scroll.setWidget(self.table_details_text)
        self.table_details_scroll.setFixedWidth(300)  # Adjust width as needed
        self.table_details_scroll.setFixedHeight(800)  # Adjust height as needed

        right_layout.addWidget(self.query_database_label)
        right_layout.addWidget(self.query_database_dropdown)
        right_layout.addWidget(self.query_table_label)
        right_layout.addWidget(self.query_table_dropdown)
        right_layout.addWidget(self.table_details_label)
        right_layout.addWidget(self.table_details_scroll)
        right_layout.addStretch()  # Push the details to the top

        # Combine left and right layouts
        main_layout.addLayout(left_layout, stretch=3)
        main_layout.addLayout(right_layout, stretch=1)

        self.query_tab.setLayout(main_layout)



    def populate_dynamic_reports_dropdowns(self, column_names: list):
        """Populate the X and Y-axis dropdowns in the Dynamic Reports tab."""
        if not column_names:
            QMessageBox.warning(self, "No Columns", "The executed query returned no columns to plot.")
            return

        # Clear existing items
        self.dynamic_reports_tab_x_axis_dropdown.clear()
        self.dynamic_reports_tab_y_axis_dropdown.clear()

        # Add column names to X and Y-axis dropdowns
        self.dynamic_reports_tab_x_axis_dropdown.addItems(column_names)
        self.dynamic_reports_tab_y_axis_dropdown.addItems(column_names)

        # Optionally, set default selections
        if len(column_names) >= 2:
            self.dynamic_reports_tab_x_axis_dropdown.setCurrentIndex(0)
            self.dynamic_reports_tab_y_axis_dropdown.setCurrentIndex(1)

    def load_query_tables(self, selected_database: str):
        """Load tables from the selected database into the table dropdown for queries."""
        try:
            tables = self.sql_manager.get_tables(selected_database)
            self.query_table_dropdown.clear()
            self.query_table_dropdown.addItems(tables)
        except Exception as e:
            logging.error(f"Failed to load tables for database {selected_database}: {e}")
            QMessageBox.critical(self, "Error", f"Failed to load tables for database {selected_database}: {e}")

    def display_table_details(self, selected_table: str):
        """Display details of the selected table."""
        if not selected_table:
            self.table_details_text.clear()
            return

        try:
            selected_database = self.query_database_dropdown.currentText()
            table = selected_table
            columns = self.sql_manager.get_table_columns(selected_database, table)
            primary_keys = self.sql_manager.get_primary_keys(selected_database, table)

            # Start constructing the details string
            details = f"Database: {selected_database}\nTable: {table}\n\nColumns:\n"
            for col in columns:
                details += f" - {col['COLUMN_NAME']} ({col['DATA_TYPE']}) {'NULL' if col['IS_NULLABLE'] == 'YES' else 'NOT NULL'}\n"

            details += "\nPrimary Keys:\n"
            if primary_keys:
                for pk in primary_keys:
                    details += f" - {pk}\n"
            else:
                details += " - None\n"

            self.table_details_text.setPlainText(details)
        except Exception as e:
            logging.error(f"Failed to display table details: {e}")
            QMessageBox.critical(self, "Error", f"Failed to display table details: {e}")

    def execute_sql_query(self):
        """Execute the SQL query entered by the user."""
        query = self.query_input.toPlainText().strip()
        selected_database = self.query_database_dropdown.currentText()

        if not query:
            QMessageBox.warning(self, "Warning", "Please enter a SQL query.")
            return

        try:
            # Execute the query in the selected database
            results, column_names = self.sql_manager.execute_query(selected_database, query)
            self.display_query_results(results, column_names)
            
            # Populate Dynamic Reports Dropdowns
            self.populate_dynamic_reports_dropdowns(column_names)
            
            # Update last query results for further use in reporting and exporting
            self.last_query_results = results
            self.last_query_columns = column_names
            
        except RuntimeError as e:
            QMessageBox.critical(self, "Execution Error", f"Failed to execute query:\n{str(e)}")
            logging.error(f"Failed to execute query: {str(e)}")

            
    def save_query_as(self):
        """Save the current query with a custom name."""
        query = self.query_input.toPlainText().strip()
        if not query:
            QMessageBox.warning(self, "Warning", "Please enter a SQL query to save.")
            return

        # Prompt for a query name
        query_name, ok = QInputDialog.getText(self, "Save Query", "Enter a name for the query:")
        if not ok or not query_name.strip():
            return  # Do nothing if the dialog is canceled or the name is empty

        query_name = query_name.strip()
        self.saved_queries[query_name] = query
        self.save_queries_to_file()
        self.load_saved_queries()
            

    def display_query_results(self, results: list, column_names: list):
        """Display the results of the executed SQL query in a table."""
        self.query_result_table.clearContents()
        self.query_result_table.setRowCount(len(results))
        self.query_result_table.setColumnCount(len(column_names))
        self.query_result_table.setHorizontalHeaderLabels(column_names)

        for row_idx, row in enumerate(results):
            for col_idx, value in enumerate(row):
                item = QTableWidgetItem(str(value))
                self.query_result_table.setItem(row_idx, col_idx, item)

    # ------------------ Dynamic Reports Tab ------------------

    def init_dynamic_reports_ui(self):
        """Initialize the Dynamic Reports UI elements with dropdowns on top and graph display below."""
        layout = QVBoxLayout()

        # Instruction Label
        instruction_label = QLabel("Generate dynamic reports based on the latest executed query in the 'Execute Query' tab.")
        instruction_label.setWordWrap(True)
        layout.addWidget(instruction_label)

        # Graph Configuration Section (Dropdowns)
        graph_config_layout = QHBoxLayout()

        # X-axis Selection
        self.dynamic_reports_tab_x_axis_dropdown = QComboBox()
        self.dynamic_reports_tab_x_axis_dropdown.setPlaceholderText("Select X-axis")

        # Y-axis Selection
        self.dynamic_reports_tab_y_axis_dropdown = QComboBox()
        self.dynamic_reports_tab_y_axis_dropdown.setPlaceholderText("Select Y-axis")

        # Graph Type Selection
        self.dynamic_reports_tab_graph_type_dropdown = QComboBox()
        self.dynamic_reports_tab_graph_type_dropdown.addItems(["Bar", "Line", "Pie"])

        # Generate Graph Button
        self.dynamic_reports_tab_generate_graph_button = QPushButton("Generate Graph")
        self.dynamic_reports_tab_generate_graph_button.clicked.connect(self.generate_dynamic_graph)

        # Add widgets to graph_config_layout
        graph_config_layout.addWidget(QLabel("X-axis:"))
        graph_config_layout.addWidget(self.dynamic_reports_tab_x_axis_dropdown)
        graph_config_layout.addWidget(QLabel("Y-axis:"))
        graph_config_layout.addWidget(self.dynamic_reports_tab_y_axis_dropdown)
        graph_config_layout.addWidget(QLabel("Graph Type:"))
        graph_config_layout.addWidget(self.dynamic_reports_tab_graph_type_dropdown)
        graph_config_layout.addWidget(self.dynamic_reports_tab_generate_graph_button)

        layout.addLayout(graph_config_layout)

        # Graph Display Section
        graph_display_layout = QVBoxLayout()
        graph_display_label = QLabel("Graph:")
        self.dynamic_reports_tab_graph_display_label = QLabel("Graph will be displayed here.")
        self.dynamic_reports_tab_graph_display_label.setAlignment(Qt.AlignCenter)
        self.dynamic_reports_tab_graph_display_label.setScaledContents(True)  # Enable automatic scaling

        graph_display_layout.addWidget(graph_display_label)
        graph_display_layout.addWidget(self.dynamic_reports_tab_graph_display_label, stretch=1)

        layout.addLayout(graph_display_layout)

        # Export Report Button
        self.dynamic_reports_tab_export_report_button = QPushButton("Export Report")
        self.dynamic_reports_tab_export_report_button.clicked.connect(self.export_dynamic_report)
        layout.addWidget(self.dynamic_reports_tab_export_report_button)

        # Set stretch factors to maximize graph display area
        main_container = QWidget()
        main_container.setLayout(layout)

        # Use a main vertical layout with stretch
        outer_layout = QVBoxLayout()
        outer_layout.addWidget(main_container)
        outer_layout.setStretch(0, 1)

        self.dynamic_reports_tab.setLayout(outer_layout)

    def generate_dynamic_graph(self):
        """Generate and display the graph based on the latest query results."""
        if not self.last_query_results or not self.last_query_columns:
            QMessageBox.warning(self, "No Data", "Please execute a query in the 'Execute Query' tab first.")
            return

        x_col = self.dynamic_reports_tab_x_axis_dropdown.currentText()
        y_col = self.dynamic_reports_tab_y_axis_dropdown.currentText()
        graph_type = self.dynamic_reports_tab_graph_type_dropdown.currentText()

        if not x_col or not y_col:
            QMessageBox.warning(self, "Selection Error", "Please select both X and Y axes for the graph.")
            return

        # Convert the results to a pandas DataFrame
        try:
            df = pd.DataFrame([tuple(row) for row in self.last_query_results], columns=self.last_query_columns)
        except Exception as e:
            QMessageBox.critical(self, "Data Error", f"Failed to process query results:\n{str(e)}")
            logging.error(f"Failed to convert query results to DataFrame: {e}")
            return

        # Check if selected columns exist in the DataFrame
        if x_col not in df.columns or y_col not in df.columns:
            QMessageBox.critical(self, "Column Error", "Selected columns do not exist in the query results.")
            return

        # Determine if Y-axis is suitable for aggregation (e.g., numerical)
        if graph_type in ["Bar", "Line"] and not pd.api.types.is_numeric_dtype(df[y_col]):
            QMessageBox.warning(self, "Data Type Error", "For Bar and Line graphs, the Y-axis must be numeric.")
            return

        try:
            plt.close('all')  # Close previous plots

            sns.set(style="whitegrid")

            fig, ax = plt.subplots(figsize=(10, 6))

            if graph_type == "Bar":
                sns.barplot(x=x_col, y=y_col, data=df, ax=ax)
                ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha="right")  # Rotate x-axis labels

            elif graph_type == "Line":
                sns.lineplot(x=x_col, y=y_col, data=df, ax=ax, marker='o')
                ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha="right")  # Rotate x-axis labels

            elif graph_type == "Pie":
                if y_col != x_col:
                    pie_data = df.groupby(x_col)[y_col].sum()
                else:
                    pie_data = df[x_col].value_counts()
                ax.pie(pie_data, labels=pie_data.index, autopct='%1.1f%%', startangle=140)
                ax.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
                plt.title(f'Pie Chart of {y_col}' if y_col != x_col else f'Pie Chart of {x_col}')
                fig = plt.gcf()

            if graph_type in ["Bar", "Line"]:
                plt.title(f'{graph_type} Graph of {y_col} vs {x_col}')
                plt.xlabel(x_col)
                plt.ylabel(y_col)

            plt.tight_layout()

            # Save the plot to a temporary file
            temp_plot_path = "dynamic_report_graph.png"
            plt.savefig(temp_plot_path)

            # Display the image in the QLabel
            pixmap = QPixmap(temp_plot_path)
            self.dynamic_reports_tab_graph_display_label.setPixmap(pixmap.scaled(
                self.dynamic_reports_tab_graph_display_label.size(),
                Qt.KeepAspectRatio,
                Qt.SmoothTransformation
            ))

            # Store the figure for exporting
            self.current_dynamic_fig = fig

        except Exception as e:
            QMessageBox.critical(self, "Graph Generation Error", f"Failed to generate the graph:\n{str(e)}")
            logging.error(f"Failed to generate dynamic graph: {e}")


    def export_dynamic_report(self):
        """Export the dynamic graph and associated data as a PDF or image file."""
        # Check if a graph has been generated
        if not hasattr(self, 'current_dynamic_fig') or self.current_dynamic_fig is None:
            QMessageBox.warning(self, "Export Error", "There is no graph to export. Please generate a graph first.")
            return

        # Prompt user to select save location and format
        options = QFileDialog.Options()
        file_path, _ = QFileDialog.getSaveFileName(
            self,
            "Save Report",
            "",
            "PDF Files (*.pdf);;PNG Files (*.png)",
            options=options
        )

        if not file_path:
            return  # User canceled the dialog

        try:
            if file_path.endswith('.pdf'):
                # Create a PDF with the graph and table
                from matplotlib.backends.backend_pdf import PdfPages

                with PdfPages(file_path) as pdf:
                    # Add the graph
                    pdf.savefig(self.current_dynamic_fig)
                    plt.close()

                    # Add the table as a figure
                    fig, ax = plt.subplots(figsize=(12, len(self.last_query_results) * 0.25))
                    ax.axis('tight')
                    ax.axis('off')

                    # Extract data from the last query results
                    table_data = []
                    headers = self.last_query_columns
                    for row in self.last_query_results:
                        row_data = [str(value) for value in row]
                        table_data.append(row_data)

                    table = ax.table(cellText=table_data, colLabels=headers, loc='center', cellLoc='center')
                    table.auto_set_font_size(False)
                    table.set_fontsize(8)
                    table.auto_set_column_width(col=list(range(len(headers))))
                    pdf.savefig(fig)
                    plt.close()

            elif file_path.endswith('.png'):
                # Save the graph as a PNG file
                self.current_dynamic_fig.savefig(file_path)
            else:
                QMessageBox.warning(self, "Export Error", "Unsupported file format selected.")
                return

            QMessageBox.information(self, "Export Successful", f"Report successfully saved to {file_path}")

        except Exception as e:
            QMessageBox.critical(self, "Export Error", f"Failed to export the report:\n{str(e)}")
            logging.error(f"Failed to export report: {e}")

    # ------------------ Recent Queries Methods ------------------



    def load_saved_queries(self):
        """Load saved queries into the saved_queries_dropdown by name."""
        self.saved_queries_dropdown.clear()
        self.saved_queries_dropdown.addItem("")  # Empty default for unselected
        for name in self.saved_queries:
            self.saved_queries_dropdown.addItem(name)


    def load_selected_saved_query(self):
        """Load the selected saved query into the query editor."""
        selected_query_name = self.saved_queries_dropdown.currentText()
        if selected_query_name and selected_query_name in self.saved_queries:
            self.query_input.setPlainText(self.saved_queries[selected_query_name])



    def load_queries_from_file(self):
        """Load saved queries from a JSON file."""
        try:
            if os.path.exists("saved_queries.json"):
                with open("saved_queries.json", "r") as f:
                    self.saved_queries = json.load(f)
        except Exception as e:
            logging.error(f"Failed to load saved queries from file: {e}")
            self.saved_queries = {}

    def save_queries_to_file(self):
        """Save named queries to a JSON file."""
        try:
            with open("saved_queries.json", "w") as f:
                json.dump(self.saved_queries, f)
        except Exception as e:
            logging.error(f"Failed to save named queries to file: {e}")



def main():
    """Main entry point for the application."""
    app = QtWidgets.QApplication(sys.argv)
    app.setStyleSheet(qdarkstyle.load_stylesheet_pyqt5())

    # Show Login Dialog
    login_dialog = LoginDialog()
    if login_dialog.exec_() == QDialog.Accepted:
        server, auth_method, username, password = login_dialog.get_credentials()

        if not server:
            QMessageBox.critical(None, "Input Error", "Server address is required to continue.")
            sys.exit(1)

        if auth_method == "SQL" and (not username or not password):
            QMessageBox.critical(None, "Input Error", "Username and password are required for SQL Server Authentication.")
            sys.exit(1)

        # Initialize SQLManager
        try:
            sql_manager = SQLManager(server, auth_method, username, password)
        except RuntimeError as e:
            QMessageBox.critical(None, "Connection Error", str(e))
            sys.exit(1)

        # Initialize and show the main application window
        window = DataIntegrationTool(sql_manager)
        window.show()
        sys.exit(app.exec_())
    else:
        sys.exit(0)


if __name__ == '__main__':
    main()
