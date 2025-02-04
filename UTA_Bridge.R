# Check if 'librarian' package is installed, if not, install it and load it
if (!require(librarian)){
  install.packages("librarian")
  library(librarian)
}

# Load necessary packages using librarian, ensuring they are installed if missing
librarian::shelf(tidyverse, here, DBI, odbc, readxl, janitor, lubridate, writexl, padr)



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


# OT forecast data

ot_2025 <- read_csv(here("data", "forecast_OT_2025.csv")) %>% 
  clean_names() %>% 
  mutate(department = if_else(str_detect(job_desc, "Ramp"), "RAMP", NA),
         department = if_else(str_detect(job_desc, "Cleaner"), "CLEANER", department),
         department = if_else(str_detect(job_desc, "CSR|Customer|Agent/Specialist"), "GS", department),
         department = if_else(str_detect(job_desc, "Weight"), "WEIGHT", department),
         department = if_else(str_detect(job_desc, "Line"), "LINE SERVICEMAN", department),
         department = if_else(job_key == "KOA400603J5E2P", "GS", department)) %>% 
  pivot_longer(
    cols = starts_with("jan_25"):ends_with("dec_25"), # Select all columns with monthly data
    names_to = "year_month",                          # Create a new column for month-year
    values_to = "value"                      # Move the values to this column
  ) %>%
  mutate(
    year_month = str_replace(year_month, "_", "-"),   # Replace underscore with hyphen in month names
    year_month = str_to_title(year_month),           # Ensure correct format (e.g., Jan-2025)
    year_month = as.Date(paste0("01-", year_month), "%d-%b-%y"), # Convert to Date format
    year_month = format(year_month, "%Y-%m"),        # Ensure correct format (e.g., 2025-01)
    days_in_month = days_in_month(as.Date(paste0(year_month, "-01"))) # Calculate days in the month
  ) %>%
  select(station, department, year_month, value, days_in_month) %>% 
  mutate(as_task_bins = "Total Overtime") %>% 
  rename(Department = department,
         Station = station)



ot_2024 <- read_csv(here("data", "forecast_OT_2024.csv")) %>% 
  clean_names() %>% 
  mutate(department = if_else(str_detect(job_desc, "Ramp"), "RAMP", NA),
         department = if_else(str_detect(job_desc, "Cleaner"), "CLEANER", department),
         department = if_else(str_detect(job_desc, "CSR|Customer|Agent/Specialist"), "GS", department),
         department = if_else(str_detect(job_desc, "Weight"), "WEIGHT", department),
         department = if_else(str_detect(job_desc, "Line"), "LINE SERVICEMAN", department),
         department = if_else(job_key == "KOA400603J5E2P", "GS", department)) %>% 
  pivot_longer(
    cols = starts_with("jan_2024"):ends_with("dec_2024"), # Select all columns with monthly data
    names_to = "year_month",                              # Create a new column for month-year
    values_to = "value"                          # Move the values to this column
  ) %>%
  mutate(
    year_month = str_replace(year_month, "_", "-"),        # Replace underscore with hyphen in month names
    year_month = str_to_title(year_month),                # Ensure correct format (e.g., Jan-2024)
    year_month = as.Date(paste0("01-", year_month), "%d-%b-%Y"), # Convert to Date format
    days_in_month = days_in_month(year_month),            # Calculate number of days in the month
    year_month = format(year_month, "%Y-%m")              # Remove the day portion of year_month
  ) %>%
  select(station, department, year_month, value, days_in_month) %>% 
  mutate(as_task_bins = "Total Overtime") %>% 
  rename(Department = department,
         Station = station)



 # Define the directory path
forecast_data_file_path <- "D:/StaffPlanner/Budget Headcount/Tableau Forecast Report/"

# List all .xlsx files in the directory
forecast_files <- list.files(path = forecast_data_file_path, pattern = "\\.csv$", full.names = TRUE)

# Loop through each file and assign it to a new object
for (forecast_file in forecast_files) {
  # Generate a valid object name based on the file name (remove directory and extension)
  object_name <- gsub("\\.csv$", "", basename(forecast_file))
  object_name <- make.names(object_name) # Ensure it's a valid R variable name
  
  # Read the data and assign to the new object
  assign(object_name, read_csv(forecast_file))
}


f_2025 <- X2025_forecast %>% 
  pivot_longer(
    cols = starts_with(c("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")),
    names_to = "year_month",
    values_to = "value"
  ) %>%
  mutate(
    year_month = format(as.Date(paste0("01-", year_month), format = "%d-%b-%y"), "%Y-%m"),
    days_in_month = days_in_month(ymd(paste0(year_month, "-01")))
  ) %>% 
  mutate(
    Department = if_else(Department == "Wt/Bal", "WEIGHT", Department),
    Department = if_else(Department == "LS", "LINE SERVICEMAN", Department),
    Department = if_else(Department == "Cabin", "CLEANER", Department),
    Department = if_else(Department %in% c("CSA", "CSR"), "GS", Department),
    Department = if_else(Department == "Ramp", "RAMP", Department)
  ) %>% 
  mutate(
    Category = if_else(Category == "Sick Hours", "Sick Leave", Category),
    Category = if_else(Category == "Total Hours", "Regular Hours", Category),
    Category = if_else(Category == "Cabin", "Cleaner", Category),
  ) %>% 
  rename(as_task_bins = Category) %>% 
  rbind(ot_2025)

f_2024 <- X2024_forecast %>% 
  pivot_longer(
    cols = starts_with(c("24-Jan", "24-Feb", "24-Mar", "24-Apr", "24-May", "24-Jun", "24-Jul", "24-Aug", 
                         "24-Sep", "24-Oct", "24-Nov", "24-Dec")),
    names_to = "year_month",
    values_to = "value"
  ) %>%
  mutate(
    # Append "20" to convert "24-Jan" into "Jan-2024"
    year_month = format(lubridate::my(paste0(substr(year_month, 4, 6), "-20", substr(year_month, 1, 2))), "%Y-%m"),
    # Calculate the number of days in the month
    days_in_month = lubridate::days_in_month(lubridate::ymd(paste0(year_month, "-01")))
  ) %>% 
    mutate(
      Department = if_else(Department == "Wt/Bal", "WEIGHT", Department),
      Department = if_else(Department == "LS", "LINE SERVICEMAN", Department),
      Department = if_else(Department == "Cabin", "CLEANER", Department),
      Department = if_else(Department %in% c("CSA", "CSR"), "GS", Department),
      Department = if_else(Department == "Ramp", "RAMP", Department)
    ) %>% 
    mutate(
      Category = if_else(Category == "Sick Hours", "Sick Leave", Category),
      Category = if_else(Category == "Total Hours", "Regular Hours", Category),
      Category = if_else(Category == "Cabin", "Cleaner", Category),
    ) %>% 
    rename(as_task_bins = Category) %>% 
  rbind(ot_2024)


forecast_data <- rbind(f_2025, f_2024) %>%
  mutate(
    value = str_remove_all(value, ","),
    value = as.integer(if_else(grepl("^[0-9]+$", value), value, NA_character_)) # Keep only numeric
  ) %>%
  group_by(Station, Department, year_month, as_task_bins) %>%
  mutate(value = mean(value, na.rm = TRUE)) %>%
  reframe(
    "Forecast Hours" = round(value / days_in_month, 0)
  ) %>%
  distinct() %>% 
  rename(station=Station,
         department=Department) %>% 
  filter(as_task_bins %in% c("Regular Hours", "Sick Leave", "Training", "Vacation", "Total Overtime"))


training <- forecast_data %>% 
  filter(as_task_bins == "Training")

agg_data_int <- tasks_binned_as %>%
  mutate(year_month = format(work_summary_work_date, "%Y-%m")) %>%
  group_by(work_summary_work_date,year_month, as_task_bins, broad_cat, station, department) %>% # add work group
  reframe("Actual Hours" = as.integer(sum(work_detail_hours))) 

tot_ot_agg <- agg_data_int %>% 
  filter(as_task_bins %in% c("Overtime 1.5", "Overtime 2.0")) %>% 
  group_by(work_summary_work_date, year_month, broad_cat, station, department) %>% 
  reframe(`Actual Hours` = sum(`Actual Hours`)) %>% 
  mutate(as_task_bins = "Total Overtime")

agg_data <- rbind(agg_data_int, tot_ot_agg)%>% 
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
agg_data_complete_int <- full_dates_task_bins %>%
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
  filter(work_summary_work_date >= start_date & work_summary_work_date <= end_date) %>% 
  mutate(hours = if_else(as_task_bins %in% c("All Other",  "Holiday Not Worked",
                                             "Overtime 2.0", "Overtime 1.5",
                                             "Holiday Worked", "Trainers", "Coaches",
                                             "Schedulers") & `Actual vs Forecast` == "Forecast Hours",
                         NA,
                         hours)) %>% 
  mutate(as_task_bins = if_else(as_task_bins == "Regular Hours", "Regular", as_task_bins),
         as_task_bins = if_else(as_task_bins == "Overtime 1.5", "OT 1.5", as_task_bins),
         as_task_bins = if_else(as_task_bins == "Overtime 2.0", "OT 2.0", as_task_bins),
         as_task_bins = if_else(as_task_bins == "Total Overtime", "Total OT", as_task_bins),
         as_task_bins = if_else(as_task_bins == "Sick Leave", "Sick", as_task_bins)) %>% 
  pivot_wider(
    names_from = `Actual vs Forecast`,
    values_from = hours,
    names_prefix = ""
  ) %>%
  left_join(training, by = c("as_task_bins", "station", "department", "year_month")) %>% 
  mutate(`Forecast Hours.x` = if_else(!is.na(`Forecast Hours.y`), `Forecast Hours.y`, `Forecast Hours.x`)) %>% 
  select(!`Forecast Hours.y`) %>% 
  rename(`Forecast Hours` = `Forecast Hours.x`) %>% 
  mutate(date_data = Sys.Date())

############# End Combine Actual and Forecasted Data ###############


############ Flights per Day #######################


tryCatch({
  db_connection <- DBI::dbConnect(odbc::odbc(),  # Establish a database connection using ODBC for the playground database
                                  Driver = "SnowflakeDSIIDriver",  # Specify the Snowflake ODBC driver
                                  Server = "hawaiianair.west-us-2.azure.snowflakecomputing.com",  # Server address
                                  WAREHOUSE = "DATA_LAKE_READER",  # Specify the Snowflake warehouse
                                  Database = "ENTERPRISE",  # Specify the database name
                                  UID = "jacob.eisaguirre@hawaiianair.com",  # User ID for authentication
                                  authenticator = "externalbrowser")  # Use external browser for authentication
  print("Database Connected!")  # Print success message if connection is established
}, error = function(cond) {
  print("Unable to connect to Database.")  # Print error message if connection fails
})

# Set schema and retrieve data from `AA_FINAL_PAIRING` table
dbExecute(db_connection, "USE SCHEMA CREW_ANALYTICS") 


q_flight_history <- paste0("SELECT * FROM CT_FLIGHT_HISTORY WHERE FLIGHT_DATE > '2022-12-31' and POSITION = 'CA';")

raw_fh <- dbGetQuery(db_connection, q_flight_history)

flights_per_day <- raw_fh %>% 
  group_by(FLIGHT_NO, FLIGHT_DATE, DEPARTING_CITY, ARRIVAL_CITY, FLIGHT_LEG_NO, SCHED_DEPARTURE_DATE, SCHED_DEPARTURE_TIME_RAW, SCHED_ARRIVAL_DATE,
           SCHED_ARRIVAL_TIME) %>% 
  mutate(temp_id = cur_group_id()) %>% 
  filter(!duplicated(temp_id)) %>% 
  ungroup() %>% 
  group_by(FLIGHT_DATE, DEPARTING_CITY) %>% 
  reframe(dep_flights_per_day = n()) %>% 
  filter(DEPARTING_CITY %in% c(agg_data_complete_int$station)) %>% 
  rename(station = DEPARTING_CITY,
         work_summary_work_date = FLIGHT_DATE)

agg_data_complete <- agg_data_complete_int %>% 
  left_join(flights_per_day, by = c("work_summary_work_date", "station"))


############# Push to Bridge ###############

write_xlsx(agg_data_complete, "Z:/OperationsResourcePlanningAnalysis/UTA_AO/daily_agg.xlsx")

# worked_hours <- agg_data_complete %>% 
#   filter(broad_cat == "Worked Hours") %>% 
#   group_by(station, as_task_bins, department, broad_cat, year_month, `Actual vs Forecast`) %>%   
#   arrange(work_summary_work_date) %>%  # Ensure data is ordered by date for cumulative sum
#   mutate(mtd = round(cumsum(hours), 0)) %>%  # Cumulative sum for MTD calculation
#   ungroup() %>% 
#   group_by(station, as_task_bins, department, broad_cat, year_month) %>%   
#   mutate(
#     red_black = if_else(
#       `Actual vs Forecast` == "Actual Hours" & 
#         hours > hours[`Actual vs Forecast` == "Forecast Hours"],
#       "red",
#       "black"
#     )
#   ) %>% 
#   ungroup() %>% 
#   write_xlsx("Z:/OperationsResourcePlanningAnalysis/UTA_AO/worked_hours.xlsx")
# 
# unworked_hours <- agg_data_complete %>% 
#   filter(broad_cat == "Unworked Hours") %>% 
#   group_by(station, as_task_bins, department, broad_cat, year_month, `Actual vs Forecast`) %>%   
#   arrange(work_summary_work_date) %>%  # Ensure data is ordered by date for cumulative sum
#   mutate(mtd = round(cumsum(hours), 0)) %>%  # Cumulative sum for MTD calculation
#   ungroup() %>% 
#   group_by(station, as_task_bins, department, broad_cat, year_month) %>%   
#   mutate(
#     red_black = if_else(
#       `Actual vs Forecast` == "Actual Hours" & 
#         hours > hours[`Actual vs Forecast` == "Forecast Hours"],
#       "red",
#       "black"
#     )
#   ) %>% 
#   ungroup() %>% 
#   write_xlsx("Z:/OperationsResourcePlanningAnalysis/UTA_AO/unworked_hours.xlsx")
# 
# training_hours <- agg_data_complete %>% 
#   filter(broad_cat == "Training Hours") %>% 
#   group_by(station, as_task_bins, department, broad_cat, year_month, `Actual vs Forecast`) %>%   
#   arrange(work_summary_work_date) %>%  # Ensure data is ordered by date for cumulative sum
#   mutate(mtd = round(cumsum(hours), 0)) %>%  # Cumulative sum for MTD calculation
#   ungroup() %>% 
#   group_by(station, as_task_bins, department, broad_cat, year_month) %>%   
#   mutate(
#     red_black = if_else(
#       `Actual vs Forecast` == "Actual Hours" & 
#         hours > hours[`Actual vs Forecast` == "Forecast Hours"],
#       "red",
#       "black"
#     )
#   ) %>% 
#   ungroup() %>% 
#   write_xlsx("Z:/OperationsResourcePlanningAnalysis/UTA_AO/training_hours.xlsx")

