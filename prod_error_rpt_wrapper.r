# Code objectives
# ---------------
# This code aggregates all available data within the production error extract in different ways, which will be used to
# 1) generate production error report
# 2) generate summary tables in wiki page - Prediction Accuracy

code_path <- "/home/joel/PER" #dirname(rstudioapi::getActiveDocumentContext()$path)
output_path <- paste0(code_path, "/output")
if (!dir.exists(output_path)) {
  dir.create(output_path, recursive = TRUE)
}
logFile = file(paste0(output_path, "/log_", Sys.time(), ".txt"), open = "a")

################# Parameter assignment #################

plot_window_wk <- 26 #define the rolling window for plotting
get_data_from_s3 <- FALSE #download data from s3 bucket or not
if (get_data_from_s3) {
  cat("We will download all the production error data from science s3 bucket.", file = logFile)
}

################# Initial set-ups, data loading #################

# Load essential libraries
library(ggplot2)
library(dplyr)
library(lubridate)
library(zoo)
library(plotly)
library(crayon)
library(data.table)
library(bit64)
library(stringr)

# Get data files from the s3 bucket and save locally
data_path = paste0(code_path, "/ErrorExtracts/")
s3_path = "infor-infor-gtn-devlp-gtndpsupq-datasci-us-west-2/ProductionErrorExtract/"
if (ifelse(exists("get_data_from_s3"), get_data_from_s3, TRUE)) {
  if (dir.exists(data_path)) {
    #if data exists, delete old data folder firstly
    unlink("/home/data/production_error_report/ProductionErrorExtract/", recursive = TRUE)
  }
  command = paste0("aws s3 cp s3://", s3_path, " ", data_path, " --recursive --profile science")
  system(command)
}

# Read milestone and shipment error data from all CSV files
datalist <- list()
for (dataType in c("msData", "shipData")) {
  keywd <- ifelse(dataType == "msData", "Milestone", "Shipment")
  files <- dir(data_path, recursive = TRUE, full.names = TRUE, pattern = paste0(keywd, ".*\\.csv$"))
  df <- do.call(rbind, lapply(files, function(x) fread(x))) 
  df <- df[!(duplicated(df))]
  datalist[[dataType]] <- df
  rm(files, df, keywd)
}

# Handle known issues in raw error data extract
# Known issue: some shipment predict records have multiple actual create time/modify time, we'd like to keep the on with max create time/modify time
if (length(unique(datalist$shipData$SHIPMENTPREDICTREPORTID)) != nrow(datalist$shipData)) {
  datalist$shipData <- datalist$shipData %>% group_by(SHIPMENTPREDICTREPORTID) %>% 
    mutate(maxCreateTime = max(ACTUALCREATETIMESTAMP), maxModifyTime = max(MODIFYTIMESTAMP)) %>% ungroup()
  datalist$shipData$badCreateTime <- ifelse(datalist$shipData$ACTUALCREATETIMESTAMP != datalist$shipData$maxCreateTime, 1, 0)
  datalist$shipData$maxCreateTime <- NULL
  datalist$shipData$badModifyTime <- ifelse(datalist$shipData$MODIFYTIMESTAMP != datalist$shipData$maxModifyTime, 1, 0)
  datalist$shipData$maxModifyTime <- NULL
  datalist$shipData$badRecord <- ifelse(datalist$shipData$badCreateTime + datalist$shipData$badModifyTime > 0, 1, 0)
  cat("Note:", sum(datalist$shipData$badRecord), paste("(", round(sum(datalist$shipData$badRecord)/nrow(datalist$shipData),4) * 100, "%)", sep = ""),
      "shipment records will be removed because they are duplicate with later created/modified records\n", file = logFile)
}

# Known issue: some milestone predict records have multiple actual create time/modify time, we'd like to keep the on with max create time/modify time
if (length(unique(datalist$msData$MILESTONEPREDICTREPORTID)) != nrow(datalist$msData)) {
  datalist$msData <- datalist$msData %>% group_by(MILESTONEPREDICTREPORTID) %>% 
    mutate(maxModifyTime = max(MODIFYTIMESTAMP)) %>% ungroup()
  datalist$msData$badModifyTime <- ifelse(datalist$msData$MODIFYTIMESTAMP != datalist$msData$maxModifyTime, 1, 0)
  datalist$msData$maxModifyTime <- NULL
  cat("Note:", sum(datalist$msData$badModifyTime), paste("(", round(sum(datalist$msData$badModifyTime) / nrow(datalist$msData), 4) * 100, "%)", sep = ""),
      "milestone records will be removed because they are duplicate with later modified records\n", file = logFile)
}

# Check whether each record is unique in the data
cleanedShipData = subset(datalist$shipData, badRecord != 1)
# Check whether shipment predict report is unique id in shipment data
if (length(unique(cleanedShipData$SHIPMENTPREDICTREPORTID)) != nrow(cleanedShipData)) {
  stop(cat("Warning: There are repeated shipment predict report id in raw data, please check!\n", file = logFile))
}

cleanedMsData = subset(datalist$msData, badModifyTime != 1)
# Check whether milestone predict report is unique in shipment data
if (length(unique(cleanedMsData$MILESTONEPREDICTREPORTID)) != nrow(cleanedMsData)) {
  stop(cat("Warning: There are repeated milestone predict report id in raw data, please check!\n", file = logFile))
}

# Join shipment data with milestone data to get aggregated info
MSbase <- full_join(cleanedShipData, cleanedMsData, by = c("SHIPMENTPREDICTREPORTID", "OWNERORGID"), suffix = c("_ship", "_ms"))

# Remove shipment records didn't have any milestones error data
if (sum(is.na(MSbase$MILESTONEPREDICTREPORTID)) > 0) {
  cat("Note:", sum(is.na(MSbase$MILESTONEPREDICTREPORTID)), "shipment records will be removed because they cannot match to any milestone error data\n", file = logFile)
  MSbase = subset(MSbase,!(is.na(MSbase$MILESTONEPREDICTREPORTID)))
}

# Fix known issue in raw data extract
# Known issue: Remove some milestone records that have different shipment completed time from shipment data
if (sum(MSbase$COMPLETEDTIME_ship != MSbase$COMPLETEDTIME_ms) > 0) {
  bad_record_num <- sum(MSbase$COMPLETEDTIME_ship != MSbase$COMPLETEDTIME_ms)
  cat("Note:", bad_record_num, paste("(", round(bad_record_num / nrow(MSbase), 4) * 100, "%)", sep = ""), "milestone-level records will be removed because they have different COMPLETEDTIME from shipment predict report\n", file = logFile)
  MSbase <- subset(MSbase, COMPLETEDTIME_ship == COMPLETEDTIME_ms)
}

# Check whether there are other repeated records
if (length(unique(MSbase$MILESTONEPREDICTREPORTID)) != nrow(MSbase)) {
  stop(cat("Warning: There are some repeated milestone predict report id in MSbase data, please check!", file = logFile))
}

# Convert timestamp to readable time
# MSbase$actual_createtime <- as.POSIXct(MSbase$COMPLETEDTIME_ship, origin = "1970-01-01", tz = "UTC")
MSbase$shipment_starttime <- as.POSIXct(MSbase$SHIPMENTSTARTTIME, origin = "1970-01-01", tz = "UTC")
MSbase$shipment_endtime <- as.POSIXct(MSbase$COMPLETEDTIME_ship, origin = "1970-01-01", tz = "UTC")
MSbase$MSEVENTTIME <- as.POSIXct(MSbase$MSEVENTTIME,  format = "%Y-%m-%d %H:%M:%S", tz = "UTC")

rm(dataType, bad_record_num, cleanedMsData, cleanedShipData)

################# Data preprocessing #################
MSbase <- subset(MSbase, shipment_endtime <= Sys.Date())

# Add column week, week start date and week end date
MSbase$week <- format(MSbase$shipment_endtime, "%Y-%W")

# Remove last week if the last week is not complete week
if (format(max(as.Date(MSbase$shipment_endtime)), "%a") != "Sun") {
  maxWk <- max(MSbase$week)
  MSbase <- subset(MSbase, week != maxWk)
}

# Get the min&max date in each week
MSbase <- MSbase %>% group_by(week) %>% mutate(week_start = min(as.Date(shipment_endtime)), week_end = max(as.Date(shipment_endtime))) %>% ungroup()

# If there is a week across two years, above function will separate the week to two weeks of two years.
# Take the last week of 2019 (2019-12-30 Mon to 2020-01-05 Sun) as an example
# For records of 2019-12-30 and 2019-12-31, the format function will assign the week "2019-52" to them
# For records from 2010-01-01 to 2020-01-05, the format function will assign the week "2020-00" to them
# So here, we want to combine these days as one week 2020-00
# For the last week of each previous year, the code will check whether the last day of the week is Sunday.
# If yes, then we keep the week assinment. If not, we will modify the week assignment.

# Get some useful numbers
MSbase$year_num <- as.numeric(format(MSbase$shipment_endtime, "%Y"))
MSbase$week_num <- as.numeric(format(MSbase$shipment_endtime, "%W"))
# get the number of last week for each year
MSbase <- MSbase %>% group_by(year_num) %>% mutate(max_week_num = max(week_num)) %>% ungroup()
current_year <- max(MSbase$year_num)
# Review and modify the week column for last weeks
MSbase$week <- ifelse(MSbase$year_num != current_year & MSbase$week_num == MSbase$max_week_num & format(MSbase$week_end, "%a") != "Sun", paste0(MSbase$year_num + 1, "-00"), MSbase$week)
# Modify the week start date and week end date based on new week column
MSbase <- MSbase %>% group_by(week) %>% mutate(week_start = min(week_start), week_end = max(max(week_end))) %>% ungroup()
# Remove some columns that won't be used anymore
MSbase$max_week_num <- NULL
MSbase$year_num <- NULL
MSbase$week_num <- NULL

# Create week index which help locate the start week of rolling window
MSbase$week_idx <- group_indices(MSbase, desc(week_start))
# Calculate several key dates for the report
max_date <- max(as.Date(MSbase$shipment_endtime))
min_date_plot_window <- as.Date(unique(MSbase$week_start[MSbase$week_idx == plot_window_wk]))
# period_end <- paste(month.abb[month(max_date)], year(max_date), sep = "")

# Remove redundant customer data
MSbase <- subset(MSbase, MSbase$OWNERORGID != "9948")

# Label customer name
cust_mapping <- read.csv(paste0(code_path, "/custNameMap.csv"), stringsAsFactors = FALSE)
MSbase <- left_join(MSbase, cust_mapping, by = "OWNERORGID")

# Label new customers - customer enabled during the last 8 weeks
MSbase <- MSbase %>% group_by(org_name) %>% mutate(org_max_week_idx = max(week_idx)) %>% ungroup()
MSbase$isNewCust <- ifelse(MSbase$org_max_week_idx > 8, "old", "new")
MSbase$org_max_week_idx <- NULL
new_cust_list <- unique(MSbase$org_name[MSbase$isNewCust == "new"])

# Label modes
mode_mapping <- data.frame(MODEID = c(1, 2, 3, 4, 5, 6, 7), mode_name = c("Ocean", "Air", "Truck", "Rail", "Intermodal", "Parcel", "LTL"), stringsAsFactors = FALSE)
MSbase <- left_join(MSbase, mode_mapping, by = "MODEID")

# Remove suppressed milestones as per the global suppression list
MSbase <- subset(MSbase,!(MSTYPEID %in% c(7, 8, 14, 15, 25, 42, 2530)))

# Exclude any milestones where the event time or the processed time are after the completion time
MSbase <- subset(MSbase, as.numeric(MSEVENTTIME) <= COMPLETEDTIME_ship & MSPROCESSEDTIME <= COMPLETEDTIME_ship)

rm(maxWk, mode_mapping, cust_mapping)

###########################################################
################# Shipment-level analysis #################

############ Get the shipment-level data

# Get shipment-level data
base <- MSbase %>% group_by(SHIPMENTPREDICTREPORTID, OWNERORGID, CMID, POLINESHIPMENTID, MODEID, CARRIERORGID, ORIGINCITYID, ORIGINCOUNTRYCODE,
                            POLCITYID, POLCOUNTRYCODE, PODCITYID,PODCOUNTRYCODE, FINALDESTCITYID, FINALDESTCOUNTRYCODE, SHIPMENTSTARTTIME,
                            COMPLETEDTIME_ship, week, week_start, week_end, week_idx, org_name, isNewCust, mode_name, shipment_starttime, shipment_endtime) %>% 
  summarise(ml_error_in_days = mean(abs(ERROR)), ml_ms_count = n()) %>% ungroup()

base$shipment_duration = (base$COMPLETEDTIME_ship - base$SHIPMENTSTARTTIME)/(24 * 3600)
base$error_pct <- base$ml_error_in_days/base$shipment_duration
base$org_name = ifelse(base$org_name %in% new_cust_list, paste0(base$org_name, "(new)"), base$org_name)

########### Generate data for reports ######################

# Aggregate stats and plot by week/org/mode
# Get the aggregated data
# Note: The output will also evaluate errors for other modes besides Air， Ocean and Truck
basegroup_byweek <- base %>% group_by(org_name, mode_name, week, week_idx, week_start, week_end) %>% 
  summarize(MedError = median(ml_error_in_days), MedPctError = median(error_pct), AvgMs = mean(ml_ms_count), Ships = n(), MedDuration = median(shipment_duration)) %>%
  arrange(mode_name, org_name, desc(week_idx)) %>% ungroup()

# Write the summary of current calendar year to csv file
# Note: The result can be used to update the wiki page
# write.csv(basegroup_byweek[substr(basegroup_byweek$week, 1, 4) == current_year, !colnames(basegroup_byweek) %in% c("week_idx", "week_dates")],
#           file = paste0(output_path, '/ETAcustomerreport_YTD_', max_date, '.csv'), row.names = FALSE)

# Aggregate stats and plot by org/mode
# Get the aggregated data of current calendar year
basegroup_bycust <- base[substr(base$week, 1, 4) == current_year, ] %>% group_by(org_name, mode_name) %>%
  summarize(MedError = median(ml_error_in_days), MedPctError = median(error_pct), AvgMs = mean(ml_ms_count), Ships = n()) %>% ungroup()

# Write the summary to csv file
# Note: The result can be used to update the wiki page
# write.csv(basegroup_bycust, file = paste0(output_path, '/SummaryTable_YTD_', max_date, '.csv'), row.names = FALSE)

# Aggregate stats and plot by week/mode
basegroup_byweekbymode <- base %>% group_by(mode_name, week, week_idx, week_start, week_end) %>% 
  summarize(MedError = median(ml_error_in_days), Ships = n(), MedDuration = median(shipment_duration)) %>% ungroup()

# Aggregate stats and plot by week/cust
basegroup_byweekbycust <- base %>% group_by(org_name, isNewCust, week, week_idx, week_start, week_end) %>%
  summarize(MedError = median(ml_error_in_days), Ships = n()) %>% ungroup()

# Generates alerts table
# Get the number of weeks with data for each customer_mode
basegroup_byweek =  basegroup_byweek %>% group_by(org_name, mode_name) %>% mutate(org_mode_max_week_idx = max(week_idx)) %>% ungroup()

# remove several problematic weeks - we know that these weeks have bad error due to bugs
bad_weeks = c("2019-50", "2019-51", "2020-00", "2020-01", "2020-02", "2020-03", "2020-04", "2020-05", "2020-06")

# create function for error slope
getcoeff = function(df) {
  df = ts(df)
  reg <- lm(df ~ time(df), data = df)
  return (round(as.numeric(coefficients(reg)[2]), 4))
}

tbl_data = subset(basegroup_byweek, week_idx == 1)[c("org_name", "mode_name", "MedError", "Ships")]
# Get data for previous 8 weeks (excluding current week)
last8Wks = subset(basegroup_byweek, week_idx <= 9 & week_idx != 1 & !(week %in% bad_weeks)) %>% group_by(org_name, mode_name) %>% 
  summarise(cnt_last8Wks = n(), AvgErrLast8wk = mean(MedError), AvgShipLast8wk = mean(Ships)) %>% ungroup()
tbl_data = full_join(tbl_data, last8Wks, by = c("org_name", "mode_name"))
# Get data for previous 25 weeks (excluding current week)
last25Wks = subset(basegroup_byweek, week_idx <= plot_window_wk & week_idx != 1 & !(week %in% bad_weeks)) %>% group_by(org_name, mode_name) %>%
  summarise(cnt_last25Wks = n(), AvgErrPrevWks = mean(MedError), AvgShipPrevWks = mean(Ships)) %>% ungroup()
tbl_data = full_join(tbl_data, last25Wks, by = c("org_name", "mode_name"))
# Get data for last 9 weeks
last9Wks = subset(basegroup_byweek, week_idx <= 9 & !(week %in% bad_weeks)) %>% group_by(org_name, mode_name) %>%
  summarise(ErrSlopeLast9Wk = getcoeff(MedError)) %>% ungroup()
tbl_data = full_join(tbl_data, last9Wks, by = c("org_name", "mode_name")) 
# Get data for last 26 weeks
last26Wks = subset(basegroup_byweek, week_idx <= plot_window_wk & !(week %in% bad_weeks)) %>% group_by(org_name, mode_name) %>%
  summarise(ErrSlopePrevWk = getcoeff(MedError)) %>% ungroup()
tbl_data = full_join(tbl_data, last26Wks, by = c("org_name", "mode_name"))

# get tier number for each customer mode based on weekly shipment counts in last 26 wks
shipsPerWk_byCustMode <- subset(basegroup_byweek, week_idx <= plot_window_wk & !(week %in% bad_weeks)) %>% 
  mutate(num_wk = ifelse(org_mode_max_week_idx >= plot_window_wk, plot_window_wk, org_mode_max_week_idx)) %>%
  group_by(org_name, mode_name, num_wk) %>%
  summarise(totShips = sum(Ships)) %>%
  mutate(MedShipsPerWk = totShips / num_wk) %>%
  group_by(mode_name) %>%
  mutate(modeMedShipsPerWk = median(MedShipsPerWk)) %>%
  ungroup()
shipsPerWk_byCustMode$Tier = ifelse(shipsPerWk_byCustMode$MedShipsPerWk >= shipsPerWk_byCustMode$modeMedShipsPerWk, "tier 1", "tier 2")

tbl_data = left_join(shipsPerWk_byCustMode[c("org_name", "mode_name", "Tier")], tbl_data, by = c("org_name", "mode_name"))

tbl_err = subset(tbl_data %>%
                   mutate(MedError = round(MedError, 4), 
                          Ships = ifelse(is.na(Ships), 0, Ships), 
                          cnt_last8Wks = ifelse(is.na(cnt_last8Wks), 0, cnt_last8Wks),
                          cnt_last25Wks = ifelse(is.na(cnt_last25Wks), 0, cnt_last25Wks),
                          AvgShipPrevWks = ifelse(is.na(AvgShipPrevWks), 0, round(AvgShipPrevWks,0)),
                          PctLast8wk = ifelse(cnt_last8Wks < ceiling(8*0.1), NA, MedError/AvgErrLast8wk - 1),
                          PctLastPrevWks = ifelse(cnt_last25Wks < ceiling(plot_window_wk * 0.1), NA, MedError/AvgErrPrevWks - 1),
                          ShipCntPctLastPrevWks = ifelse(cnt_last25Wks < ceiling(plot_window_wk * 0.1), NA,Ships/AvgShipPrevWks - 1),
                          Flag = ifelse(((PctLast8wk >= 0.25) | (ErrSlopePrevWk >= 0.1 & PctLastPrevWks >= 0.25)) & Ships >= 3, "X", "")) %>%
                   ungroup(),
                 select = c("org_name", "mode_name", "Tier", "Ships", "AvgShipPrevWks","ShipCntPctLastPrevWks", "MedError",  "PctLast8wk", "PctLastPrevWks", "ErrSlopePrevWk", "Flag"))

rm(last8Wks, last25Wks, last9Wks, last26Wks)
############ Generate production error report and supplemental reports ##########

# Generate main report
main_rpt <- rmarkdown::render(paste0(code_path, "/production_error_report.Rmd"), 
                              output_file = paste0(output_path, "/production_error_report_", max_date, ".html"))

# Generate report - error analysis by training files
# Get shorthand for training file
MSbase$modelfile = substr(str_match(MSbase$PREDICTFILE, "TrainingRuns/(.*?)/xgb_training_model_NReg")[,2], 1, 15)
supp_rpt_1 <-rmarkdown::render(paste0(code_path, "/error_analysis_by_training_file.Rmd"),
                               output_file = paste0(output_path, "/error_analysis_by_training_file_", max_date,".html"))

# Generate report - boxplot_shipment_duration
supp_rpt_2 <-rmarkdown::render(paste0(code_path, "/boxplot_shipment_duration.Rmd"),
                               output_file = paste0(output_path, "/boxplot_shipment_duration_", max_date,".html"))                              