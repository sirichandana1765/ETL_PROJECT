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
        raise ValueError("Missing Supabase URL or SUPABASE_KEY in .env")

    return create_client(url, key)


# -----------------------------------------
# Load CSV into Supabase
# -----------------------------------------
def load_to_supabase(staged_path: str, table_name: str = "titanic_data"):

    # Resolve relative paths safely
    if not os.path.isabs(staged_path):
        base_path = os.path.dirname(os.path.abspath(__file__))
        staged_path = os.path.abspath(os.path.join(base_path, staged_path))

    print(f"Searching for CSV file at: {staged_path}")

    if not os.path.exists(staged_path):
        print(f"❌ ERROR: File not found at {staged_path}")
        print("Run transform_titanic.py first.")
        return

    try:
        supabase = get_supabase_client()
        df = pd.read_csv(staged_path)

        # -----------------------------
        # Convert boolean/text → integer
        # -----------------------------
        bool_cols = ["adult_male", "is_child", "alone"]
        for col in bool_cols:
            if col in df.columns:
                df[col] = df[col].map(lambda x: 1 if str(x).lower() in ["true", "1"] else 0)

        # -----------------------------
        # Clean NaN for Supabase
        # -----------------------------
        df = df.where(pd.notnull(df), None)

        # -----------------------------
        # Insert in batches
        # -----------------------------
        total_rows = len(df)
        batch_size = 50
        print(f"📤 Loading {total_rows} rows into '{table_name}'...")

        for i in range(0, total_rows, batch_size):
            batch = df.iloc[i: i + batch_size].copy()
            records = batch.to_dict("records")

            try:
                supabase.table(table_name).insert(records).execute()
            except Exception as e:
                print(f"⚠️ Insert error batch {i//batch_size + 1}: {e}")
                print("Attempting UPSERT...")
                supabase.table(table_name).upsert(records).execute()

            print(f"✔️ Inserted {i + 1}–{min(i + batch_size, total_rows)}")

        print(f"🎉 FINISHED loading data into '{table_name}'.")

    except Exception as e:
        print(f"❌ ERROR loading data: {e}")


# -----------------------------------------
# Main Execution
# -----------------------------------------
if __name__ == "__main__":
    staged_csv_path = os.path.join("..", "data", "staged", "titanic_transformed.csv")
    load_to_supabase(staged_csv_path)
