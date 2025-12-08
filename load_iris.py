import os
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv


# -----------------------------------------
# Initialize Supabase Client
# -----------------------------------------
def get_supabase_client():
    load_dotenv()

    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")

    if not url or not key:
        raise ValueError("Missing Supabase URL or Supabase KEY in .env")

    return create_client(url, key)


# -----------------------------------------
# Create iris_data table (if not exists)
# -----------------------------------------
def create_table_if_not_exists():
    supabase = get_supabase_client()

    create_table_sql = """
    CREATE TABLE IF NOT EXISTS iris_data (
        id BIGSERIAL PRIMARY KEY,
        sepal_length FLOAT,
        sepal_width FLOAT,
        petal_length FLOAT,
        petal_width FLOAT,
        species TEXT,
        sepal_ratio FLOAT,
        petal_ratio FLOAT,
        is_petal_long INTEGER
    );
    """

    try:
        print("Attempting to create table using RPC...")
        supabase.rpc("execute_sql", {"query": create_table_sql}).execute()
        print("Table 'iris_data' created or already exists.")

    except Exception as e:
        print(f"RPC failed: {e}")
        print("Make sure the SQL function exists:")
        print("""
        CREATE OR REPLACE FUNCTION execute_sql(query text)
        RETURNS void AS $$
        BEGIN
            EXECUTE query;
        END;
        $$ LANGUAGE plpgsql SECURITY DEFINER;
        """)
        print("Skipping table creation (assuming exists).")


# -----------------------------------------
# Load CSV into Supabase
# -----------------------------------------
def load_to_supabase(staged_path: str, table_name: str = "iris_data"):

    # Resolve relative paths safely
    if not os.path.isabs(staged_path):
        base_path = os.path.dirname(os.path.abspath(__file__))
        staged_path = os.path.abspath(os.path.join(base_path, staged_path))

    print(f"Searching for CSV file at: {staged_path}")

    if not os.path.exists(staged_path):
        print(f"❌ ERROR: File not found at {staged_path}")
        print("Run transform_iris.py first.")
        return

    try:
        supabase = get_supabase_client()
        df = pd.read_csv(staged_path)

        # Clean NaN for Supabase
        df = df.where(pd.notnull(df), None)

        total_rows = len(df)
        batch_size = 50
        print(f"📤 Loading {total_rows} rows into '{table_name}'...")

        for i in range(0, total_rows, batch_size):
            batch = df.iloc[i: i + batch_size].copy()
            records = batch.to_dict("records")

            try:
                supabase.table(table_name).insert(records).execute()
            except Exception as e:
                print(f"⚠️ Insert error batch {i//batch_size+1}: {e}")
                print("Attempting UPSERT...")
                supabase.table(table_name).upsert(records).execute()

            print(f"✔️ Inserted rows {i + 1} to {min(i + batch_size, total_rows)}")

        print(f"🎉 FINISHED loading data into '{table_name}'.")

    except Exception as e:
        print(f"❌ ERROR loading data: {e}")


# -----------------------------------------
# Main Execution
# -----------------------------------------
if __name__ == "__main__":
    staged_csv_path = os.path.join("..", "data", "staged", "iris_transformed.csv")

    create_table_if_not_exists()
    load_to_supabase(staged_csv_path)
