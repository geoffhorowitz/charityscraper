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


if __name__ == "__main__":
    sqlite_to_json('charity_data.db', 'charity_data.json')
    #remove_entry_by_ein_util('charity_data.db', 'charities', '010445046')