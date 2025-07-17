import sqlite3
import json
import sys, os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir)))
from database import DatabaseManager

def sqlite_to_json(sqlite_db_path, json_output_path):
    """
    Converts an SQLite database to a JSON file.
    The JSON will be a dict with table names as keys and lists of row dicts as values.
    """
    conn = sqlite3.connect(sqlite_db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Get all table names
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row['name'] for row in cursor.fetchall()]

    db_dict = {}
    for table in tables:
        cursor.execute(f"SELECT * FROM {table}")
        rows = [dict(row) for row in cursor.fetchall()]
        db_dict[table] = rows

    with open(json_output_path, 'w', encoding='utf-8') as f:
        json.dump(db_dict, f, indent=2, ensure_ascii=False)

    conn.close()

def remove_entry_by_ein_util(sqlite_db_path, table_name, ein):
    """
    Removes an entry from the specified table in the SQLite database by EIN.
    """
    db = DatabaseManager(sqlite_db_path)
    db.remove_entry_by_ein(table_name, ein)
    db.close()

def count_entries_in_db(sqlite_db_path):
    """
    Prints the number of entries in each table of the SQLite database.
    """
    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"Table '{table}': {count} entries")
    conn.close()

def standardize_db(sqlite_db_path, table_name='charities'):
    """
    Ensures the specified table has all required fields. Adds missing fields and sets values as needed.
    - id: unique identifier, set to value of 'ein'
    - cause: set to value of 'categories' if exists
    """
    required_fields = [
        'id', 'name', 'website', 'budget', 'cause', 'geography', 'mission',
        'vision', 'programs_summary', 'outcomes', 'impact', 'funding_needs',
        'match_rating', 'fundable projects'
    ]
    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()

    # Get current columns
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [row[1] for row in cursor.fetchall()]

    # Add missing columns
    for field in required_fields:
        if field not in columns:
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN '{field}'")

    # Set 'id' = 'ein' if 'ein' exists
    if 'ein' in columns:
        cursor.execute(f"UPDATE {table_name} SET id = ein WHERE ein IS NOT NULL")

    # Set 'cause' = 'categories' if 'categories' exists
    if 'categories' in columns:
        cursor.execute(f"UPDATE {table_name} SET cause = categories WHERE categories IS NOT NULL")

    conn.commit()
    conn.close()


if __name__ == "__main__":
    #sqlite_to_json('charity_data.db', 'charity_data.json')
    #remove_entry_by_ein_util('charity_data.db', 'charities', '010445046')
    #count_entries_in_db('charity_data.db')
    standardize_db('charity_data.db')