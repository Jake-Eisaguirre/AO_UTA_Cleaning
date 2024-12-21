# Check if 'librarian' package is installed, if not, install it and load it
if (!require(librarian)){
  install.packages("librarian")
  library(librarian)
}

# Load necessary packages using librarian, ensuring they are installed if missing
librarian::shelf(tidyverse, here, DBI, odbc, readxl, janitor, lubridate, writexl)


############# Dynamic Actual Hours Data Update ###############


# Read the clean_static_data
clean_static_data <- read_csv(here("data", "clean_static_data.csv")) %>% 
  mutate(work_summary_work_date = as.Date(work_summary_work_date),
         work_summary_start_time = as.POSIXct(work_summary_start_time, format = "%Y-%m-%dT%H:%M:%S"),
         work_summary_end_time = as.POSIXct(work_summary_end_time, format = "%Y-%m-%dT%H:%M:%S"),
         work_detail_start_time = as.POSIXct(work_detail_start_time, format = "%Y-%m-%dT%H:%M:%S"),
         work_detail_end_time = as.POSIXct(work_detail_end_time, format = "%Y-%m-%dT%H:%M:%S"))

# Filter the files to exclude unwanted ones, such as files from 2022
file_list <- list.files("//nas01/depts/Share/Airport Operations/UTAData/Archive/", pattern = "*.csv", full.names = TRUE, recursive = TRUE) %>%
  .[!grepl("AirportOps_Daily_Hours_Fake Data.csv$", .)] %>%
  .[!grepl("EOM Archive", .)] %>%
  .[!grepl("/2022/", .)]

# Extract dates from filenames (assuming filenames are like 'AirportOps_Daily_Hours_20240101.csv')
file_dates <- gsub(".*_(\\d{8})\\.csv$", "\\1", basename(file_list))
file_dates <- as.Date(file_dates, format = "%Y%m%d")

# Keep the 70 most recent files (sort by date)
file_list <- file_list[order(file_dates, decreasing = TRUE)][1:45]

# Loop through the files and compare with clean_static_data
for (file in file_list) {
  
  # Read the current file
  current_data <- read_csv(file, col_types = cols()) %>%
    clean_names() %>%
    mutate(
      # Clean and parse the work_summary_work_date
      work_summary_work_date = str_squish(work_summary_work_date),
      work_summary_work_date = str_sub(work_summary_work_date, 1, 11),
      work_summary_work_date = str_squish(work_summary_work_date),
      work_summary_work_date = mdy(work_summary_work_date),
      work_summary_work_date_formatted = format(work_summary_work_date, "%Y-%m-%d"),
      
      # Parse start and end times to POSIXct (no format here)
      work_summary_start_time = mdy_hm(work_summary_start_time),
      work_summary_end_time = mdy_hm(work_summary_end_time),
      work_detail_start_time = mdy_hm(work_detail_start_time),
      work_detail_end_time = mdy_hm(work_detail_end_time),
      
      # Create year_month column
      year_month = format(work_summary_work_date, "%Y-%m")
    ) %>% 
    # Remove unwanted columns
    select(-work_summary_work_date_formatted) %>% 
    relocate(year_month, .after = work_summary_work_date)
  
  # Ensure the datetime columns are of the same type as clean_static_data
  current_data <- current_data %>%
    mutate(
      project_code = as.double(project_code),  # Convert project_code to double
      work_summary_start_time = as.POSIXct(work_summary_start_time, format = "%Y-%m-%dT%H:%M:%S"),
      work_summary_end_time = as.POSIXct(work_summary_end_time, format = "%Y-%m-%dT%H:%M:%S"),
      work_detail_start_time = as.POSIXct(work_detail_start_time, format = "%Y-%m-%dT%H:%M:%S"),
      work_detail_end_time = as.POSIXct(work_detail_end_time, format = "%Y-%m-%dT%H:%M:%S")
    )
  
  # Compare current file with clean_static_data and keep only records not already in clean_static_data
  new_data <- anti_join(current_data, clean_static_data, by = c("employee_id", "work_summary_work_date"))
  
  # Add the new records to clean_static_data
  clean_static_data <- bind_rows(clean_static_data, new_data)
  
  # Print which file has been processed
  print(paste("Processed file:", file))
}

# Save the updated clean_static_data
write.csv(clean_static_data, here("data", "updated_clean_static_data.csv"), row.names = FALSE)


############# End Dynamic Actual Data Update ###############




############# Combine Actual and Forecasted Data ###############



clean_static_data <- read_csv(here("data", "updated_clean_static_data.csv"))

as_bin_key <- read_csv(here("masters", "as_bin_key.csv")) %>% 
  mutate(Key = as.character(Key)) %>% 
  rename(AS_Code_Key = Key)

ha_bin_key <- read_csv(here("masters", "ha_bin_key.csv"))

stations <- read_csv(here("masters", "stations.csv")) %>% 
  clean_names() %>% 
  select(dept_name, station, workgroup)


tasks_binned_as <- clean_static_data %>% 
  left_join(ha_bin_key) %>% 
  left_join(as_bin_key) %>% 
  drop_na("AS Categories") %>% 
  rename(as_task_bins = "AS Categories") %>% 
  inner_join(stations, by = c("dept" = "dept_name")) %>% 
  mutate(department = if_else(str_detect(job_desc, "RAMP"), "RAMP", NA),
         department = if_else(str_detect(job_desc, "CLEANER"), "CLEANER", department),
         department = if_else(job_desc %in% c("J9-AGENTSPECIALIST", 
                                              "Y61-CHIEF MAINLAND CSR",
                                              "J3E2-CUSTOMER SVC CHIEF AGENT",
                                              "J5E2-CUSTOMER SVC AGENT"), "GS", department),
         department = if_else(str_detect(job_desc, "WEIGHT"), "WEIGHT", department),
         department = if_else(str_detect(job_desc, "LINE SERVICEMAN"), "LINE SERVICEMAN", department)) %>% 
  filter(!is.na(department))


forecast_file <- list.files("D:/StaffPlanner/Budget Headcount/Tableau Forecast Report", pattern = ".xlsx")
forecast_data_file_path <- "D:/StaffPlanner/Budget Headcount/Tableau Forecast Report/"

forecast_data <- read_xlsx(paste0(forecast_data_file_path,forecast_file), sheet = "")

# forecast_data <- read_csv(here("data", "budget.csv")) %>% 
#   clean_names()  %>% 
#   mutate(month = format(mdy(month), "%Y-%m")) %>% 
#   select(-matches("pt|ft")) %>% 
#   rename(year_month = month) %>% 
#   select(station, department, year_month, days_in_month, 
#          total_hours_bud, total_sick_hours_bud, total_vac_hours_bud, total_training_hours_bud) %>% 
#   rename("Regular Hours" = total_hours_bud,
#          "Sick Leave" = total_sick_hours_bud,
#          "Vacation" = total_vac_hours_bud,
#          "Training" = total_training_hours_bud) %>% 
#   mutate("Actual vs Forecasted" = "Forecasted") %>% 
#   pivot_longer(
#     cols = `Regular Hours`:Training, # Select columns to pivot
#     names_to = "as_task_bins",       # New column for variable names
#     values_to = "hours"              # New column for values
#   ) %>% 
#   group_by(station, department, year_month, as_task_bins) %>% 
#   mutate(hours = sum(hours)) %>% 
#   reframe(
#     "Forecast Hours" = round(hours/days_in_month, 0)) %>% 
#   distinct() %>% 
#   filter(!is.na(department))


agg_data <- tasks_binned_as %>%
  mutate(year_month = format(work_summary_work_date, "%Y-%m")) %>%
  group_by(work_summary_work_date,year_month, as_task_bins, broad_cat, station, department) %>% # add work group
  reframe("Actual Hours" = as.integer(sum(work_detail_hours))) %>% 
  left_join(forecast_data, by = c("year_month", "as_task_bins", "station", "department"))

task_bins_mapping <- as_bin_key %>%
  select("AS Categories", broad_cat) %>% 
  rename(as_task_bins = "AS Categories")

# Create a full grid of dates and task bins
full_dates_task_bins <- expand.grid(
  work_summary_work_date = unique(agg_data$work_summary_work_date),
  as_task_bins = unique(task_bins_mapping$as_task_bins),
  station = unique(agg_data$station),
  department = unique(agg_data$department),
  stringsAsFactors = FALSE
) %>%
  left_join(task_bins_mapping, by = "as_task_bins")  # Ensure task bins match their broad_cat




# Define the date range: current month and the previous 11 months
current_date <- Sys.Date()
start_date <- floor_date(current_date, "month") - months(11)
end_date <- floor_date(current_date, "month") + months(1) - days(1)  # Include the full current month

# Join with the original data and fill missing hours with zero
agg_data_complete <- full_dates_task_bins %>%
  left_join(agg_data, by = c("work_summary_work_date", "as_task_bins", "broad_cat", "station", "department")) %>% 
  mutate(
    year_month = ifelse(is.na(year_month), 
                        format(as.Date(work_summary_work_date), "%Y-%m"), 
                        year_month),
    # Replace NA values in Actual Hours and Forecast Hours with 0
    `Actual Hours` = ifelse(is.na(`Actual Hours`), 0, `Actual Hours`),
    `Forecast Hours` = ifelse(is.na(`Forecast Hours`), 0, `Forecast Hours`)
  ) %>%
  pivot_longer(
    cols = c(`Actual Hours`, `Forecast Hours`), # Specify the columns to pivot
    names_to = "Actual vs Forecast",           # New column for the column names
    values_to = "hours"                        # New column for the values
  ) %>%
  # Filter for the last 12 months (current month + 11 historical months)
  filter(work_summary_work_date >= start_date & work_summary_work_date <= end_date)

############# End Combine Actual and Forecasted Data ###############


############# Push to Bridge ###############

write_xlsx(agg_data_complete, "Z:/OperationsResourcePlanningAnalysis/UTA_AO/daily_agg.xlsx")
