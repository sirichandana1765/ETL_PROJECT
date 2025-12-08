import os
import pandas as pd
from extract_titanic import extract_data  # Make sure this exists

# Function to transform and stage data
def transform_data(raw_path):
    # Set staged directory path
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    staged_dir = os.path.join(base_dir, "data", "staged")
    os.makedirs(staged_dir, exist_ok=True)

    # Read extracted CSV
    df = pd.read_csv(raw_path)

    # 1. Handling Missing Values
    numeric_cols = ["age", "fare"]
    for col in numeric_cols:
        df[col] = df[col].fillna(df[col].median())

    # Fill missing categorical values with mode
    for col in ["sex", "embarked", "class", "who", "deck", "embark_town", "alive"]:
        if col in df.columns:
            df[col] = df[col].fillna(df[col].mode()[0])

    # 2. Feature Engineering
    # Family size = sibsp + parch + passenger
    df["family_size"] = df["sibsp"] + df["parch"] + 1

    # Binary flag: passenger is a child (<18)
    df["is_child"] = (df["age"] < 18).astype(int)

    # Fare per person within a family
    df["fare_per_person"] = df["fare"] / df["family_size"]

    # 3. Save transformed data
    staged_path = os.path.join(staged_dir, "titanic_transformed.csv")
    df.to_csv(staged_path, index=False)

    print(f"🚢 Titanic data transformed and saved at: {staged_path}")
    return staged_path

# Run transformation if executed directly
if __name__ == "__main__":
    raw_path = extract_data()
    transform_data(raw_path)
