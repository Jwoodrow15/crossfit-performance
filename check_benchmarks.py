import pandas as pd

# Load your CSV file
df = pd.read_csv('crossfit_open_2024_with_benchmarks_updated.csv')

# Check non-null counts for benchmark columns
for col in ["Back Squat", "Clean and Jerk", "Deadlift", "Fran", "Run 5k", "Snatch"]:
    count = df[col].notnull().sum()
    print(f"{col}: {count} non-null entries")
