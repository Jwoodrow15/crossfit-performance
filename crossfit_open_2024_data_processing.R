# Load necessary libraries
library(crossfit)
library(dplyr)
library(tidyr)

# Fetch the 2024 CrossFit Open data
open_data_2024 <- cf_open(2024)

# Unnest 'entrant' to extract competitor details and 'scores' to extract performance details
open_data_2024_unnested <- open_data_2024 %>%
  tidyr::unnest_wider(entrant) %>%
  tidyr::unnest_wider(scores, names_sep = "_")  # Automatically generate names for 'scores' elements

# Now unnest each of the individual score columns (scores_1, scores_2, scores_3) to extract their details
open_data_2024_fully_unnested <- open_data_2024_unnested %>%
  tidyr::unnest_wider(scores_1, names_sep = "_1") %>%
  tidyr::unnest_wider(scores_2, names_sep = "_2") %>%
  tidyr::unnest_wider(scores_3, names_sep = "_3")

# Convert list columns (like scores) to character format to handle unimplemented types
open_data_2024_flat <- open_data_2024_fully_unnested %>%
  mutate(across(where(is.list), ~ sapply(., toString)))  # Convert any lists to strings

# View the structure of the unnested and flattened data
colnames(open_data_2024_flat)

# Save the cleaned data to a CSV file
write.csv(open_data_2024_flat, "crossfit_open_2024_full_data.csv", row.names = FALSE)

# Output a confirmation message
print("All 2024 CrossFit Open data saved to 'crossfit_open_2024_full_data.csv'")
