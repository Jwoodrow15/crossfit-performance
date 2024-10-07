import pandas as pd

# Load your CSV file
df = pd.read_csv('crossfit_open_2024_male_with_benchmarks.csv')

# Reset benchmark columns to NaN
for col in ["Back Squat", "Clean and Jerk", "Deadlift", "Fran", "Run 5k", "Snatch"]:
    df[col] = pd.NA

# Save the updated DataFrame back to CSV
df.to_csv('crossfit_open_2024_male_with_benchmarks.csv', index=False)
