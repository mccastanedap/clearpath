def load_to_database(df, table_name, db_path='data/clearpath.db'):
    """
    Loads a clean dataframe into a SQLite table.
    Creates the table if it doesn't exist.
    Appends data if it already exists.
    """
    conn = get_connection(db_path)
    
    df.to_sql(
        name=table_name,
        con=conn,
        if_exists='append',
        index=False
    )
    
    conn.close()
    print(f"Loaded {len(df)} rows into table '{table_name}'")