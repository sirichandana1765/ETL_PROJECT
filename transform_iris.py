import os
import pandas as pd
from extract_iris import extract_data

# Function to transform and stage data
def transform_data(raw_path):
    # Set staged directory path
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    staged_dir = os.path.join(base_dir, "data", "staged")
    os.makedirs(staged_dir, exist_ok=True)

    # Read extracted CSV
    df = pd.read_csv(raw_path)

    # 1. Handling Missing Values
    numeric_cols = ["sepal_length", "sepal_width", "petal_length", "petal_width"]
    for col in numeric_cols:
        df[col] = df[col].fillna(df[col].median())

    # Fill missing species values with mode
    df["species"] = df["species"].fillna(df["species"].mode()[0])

    # 2. Feature Engineering
    df["sepal_ratio"] = df["sepal_length"] / df["sepal_width"]
    df["petal_ratio"] = df["petal_length"] / df["petal_width"]

    # Create binary feature (1 if petal_length > median else 0)
    df["is_petal_long"] = (df["petal_length"] > df["petal_length"].median()).astype(int)

    # 3. Save transformed data
    staged_path = os.path.join(staged_dir, "iris_transformed.csv")
    df.to_csv(staged_path, index=False)

    print(f"Data transformed and saved at: {staged_path}")
    return staged_path

# Run transformation if executed directly
if __name__ == "__main__":
    raw_path = extract_data()
    transform_data(raw_path)
