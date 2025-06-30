import sqlite3

class DatabaseManager:
  """A class for managing a database with sqlite3.

  This class provides methods for creating tables, inserting data,
  and retrieving data from a sqlite3 database.
  """

  def __init__(self, database_name):
    """Initializes the database connection.

    Args:
        database_name: The name of the database file.
    """
    self.connection = sqlite3.connect(database_name)
    self.cursor = self.connection.cursor()

  def create_table(self, table_name, columns):
    """Creates a table in the database.

    Args:
        table_name: The name of the table to create.
        columns: A list of dictionaries, where each dictionary defines
                  a column with keys 'name' (column name) and 'data_type'
                  (e.g., 'TEXT', 'INTEGER', 'REAL').
    """
    # Build the CREATE TABLE statement dynamically
    column_defs = [f"{col['name']} {col['data_type']}" for col in columns]
    create_table_query = f"""CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(column_defs)})"""
    self.cursor.execute(create_table_query)
    self.connection.commit()

  def insert_data(self, table_name, data, on_conflict='REPLACE'):
    """Inserts a row of data into a table, with conflict resolution.

    Args:
        table_name: The name of the table to insert data into.
        data: A dictionary where keys are column names and values are
              the corresponding data to insert.
        on_conflict: The conflict resolution strategy. Can be 'REPLACE',
                     'IGNORE', 'ABORT'. Defaults to 'REPLACE'.
    """
    # Build placeholder string and value list for the INSERT query
    placeholders = ', '.join(['?'] * len(data))
    column_names = ', '.join(data.keys())
    # Using INSERT OR {on_conflict} to handle potential UNIQUE constraint conflicts, e.g., for EINs.
    # This is a SQLite-specific syntax.
    insert_query = f"""INSERT OR {on_conflict} INTO {table_name} ({column_names}) VALUES ({placeholders})"""
    self.cursor.execute(insert_query, list(data.values()))
    self.connection.commit()

  def fetch_data(self, table_name, query=None, selection=None):
    """Fetches data from a table.

    Args:
        table_name: The name of the table to fetch data from.
        query: An optional SQL query string for specific selection 
               (e.g., "WHERE id > 10").
        selection: A list of column names to select (defaults to all).

    Returns:
        A list of dictionaries, where each dictionary represents a row
        of data from the table.
    """
    # Build the SELECT query dynamically
    if selection:
      select_clause = ', '.join(selection)
    else:
      select_clause = '*'
    base_query = f"""SELECT {select_clause} FROM {table_name}"""
    if query:
      base_query += f" {query}"
    self.cursor.execute(base_query)
    rows = self.cursor.fetchall()
    return [dict(zip([col[0] for col in self.cursor.description], row)) for row in rows]

  def remove_entry_by_ein(self, table_name, ein):
    """Removes an entry from the table based on EIN.

    Args:
        table_name: The name of the table.
        ein: The EIN value to match for deletion.
    """
    delete_query = f"DELETE FROM {table_name} WHERE ein = ?"
    self.cursor.execute(delete_query, (ein,))
    self.connection.commit()

  def close(self):
    """Closes the database connection."""
    self.connection.close()

if __name__ == "__main__":
  # Example usage
  database_manager = DatabaseManager('charity_data.db')

  database_manager.create_table(
      'charities',
      [
          {'name': 'name', 'data_type': 'TEXT'},
          {'name': 'rating', 'data_type': 'REAL'},
          {'name': 'ein', 'data_type': 'TEXT UNIQUE'},
          {'name': 'categories', 'data_type': 'TEXT'},
          {'name': 'website', 'data_type': 'TEXT'},
          {'name': 'address', 'data_type': 'TEXT'},
          {'name': 'phone_number', 'data_type': 'TEXT'},
          {'name': 'mission', 'data_type': 'TEXT'},
          {'name': 'review', 'data_type': 'TEXT'},
          {'name': 'nonprofitStatus', 'data_type': 'TEXT'}
      ]
  )

  # Insert some sample data
  charity_data = {
    'name': 'Sample Charity',
    'rating': 85.5,
    'ein': '123456789',
    'categories': 'Health, Human Services',
    'website': 'https://www.example.com',
    'address': '123 Sample St, Anytown, USA',
    'phone_number': '555-123-4567',
    'mission': 'To do good things.'
  }
  # Using INSERT OR REPLACE (the default) to avoid errors on re-running with the same EIN.
  database_manager.insert_data('charities', charity_data, on_conflict='REPLACE')

  # Fetch all charity data
  all_charities = database_manager.fetch_data('charities')
  print(all_charities)

  # Fetch charities with rating above 80
  high_rated_charities = database_manager.fetch_data('charities', query="WHERE rating > 80")
  print(high_rated_charities)

  # Remove the sample charity by EIN
  database_manager.remove_entry_by_ein('charities', '123456789')

  database_manager.close()
