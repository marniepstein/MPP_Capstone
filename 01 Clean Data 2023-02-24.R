##################################
# Merge JRRP Smartsheet Data with Jail Data
# Uses files from 00 Database ConnectionS

##################################

library(tidyverse)
library(lubridate)
library(dplyr)

##################################
# Read in data from 00 Database Connection
##################################
load("Data/rawdata.RData")


##################################
# JRRP DATA
##################################

# Clean JRRP data set
jrrp <- jrrp %>% 
  # Format variables as date'
  mutate(ENTRY_DATE = as_date(ENTRY_DATE),
         RELEASE_DATE = as_date(RELEASE_DATE),
         
         # If the release date is from before JRRP was opened, it is probably the birthday. Replace with the date entry date
         RELEASE_DATE = if_else(RELEASE_DATE < ymd('2022-04-01'), ENTRY_DATE, RELEASE_DATE),
         # If the release date is missing, use the entry date
         RELEASE_DATE = if_else(is.na(RELEASE_DATE), ENTRY_DATE, RELEASE_DATE),
         ) %>% 
  # Filter to the end date of March 31, 2023
  filter(ENTRY_DATE < ymd("2023-04-01"))

# Save a copy of entries where the release date is different from the entry date
reldatedif <- jrrp %>% 
  mutate(dif = ENTRY_DATE - RELEASE_DATE) %>% 
  filter(RELEASE_DATE != ENTRY_DATE)



# Check that release dates from before 4/1/22 were changed to the entry date
jrrp %>%
  group_by(year(RELEASE_DATE))%>%
  summarize(n=n())

# Tab missing variables and empty strings
map(jrrp, ~sum(is.na(.) | nzchar(.)==FALSE))



#### DUPLICATES ####

## Aggregate by SO number and release date
dup <- jrrp %>%
  group_by(SO_NUMBER, RELEASE_DATE) %>%
  filter(n()>1) %>%
  # ungroup() %>%
  arrange(SO_NUMBER, RELEASE_DATE) 

# By looking at the data, there are 4 entries where the SO number is duplicated and the person's name is different (2 instances)
# Look each person up in the jail data and drop the incorrect rows
# DROP by Smartsheet IDS: 2297, 2822
# OLD 4/10/23: these were in incorrect SO numbers 633, 2297, 2575, 2822, 3196, 3199


# Make a separate dataframe with these values to come back to
errors <-  jrrp %>% 
  filter(ID %in% c(2297, 2822)) 
  
# Delete these from the main JRRP dataset
jrrp <-  jrrp %>% 
  filter(!ID %in% c(2297, 2822))

# Only keep the 1 duplicate with the same name
dup <-  jrrp %>% 
  group_by(SO_NUMBER, RELEASE_DATE) %>%
  filter(n()>1)
  
# For all numeric values, use the max value. These are all 0/1, so we want to make sure to take the value of 1 if there are multiple entries
dupsum <-  dup %>% 
  summarize(client_transport = max(client_transport, na.rm=TRUE),
            court_info = max(court_info, na.rm=TRUE),
            jra = max(jra, na.rm=TRUE),
            ref_health = max(ref_health, na.rm=TRUE),
            ref_abuse = max(ref_abuse, na.rm=TRUE),
            ref_transport = max(ref_transport, na.rm=TRUE),
            ref_employ = max(ref_employ, na.rm=TRUE),
            ref_legal = max(ref_legal, na.rm=TRUE),
            ref_food = max(ref_food, na.rm=TRUE),
            ref_mhsu = max(ref_mhsu, na.rm=TRUE),
            ref_house = max(ref_house, na.rm=TRUE),
            ref_educ = max(ref_educ, na.rm=TRUE),
            ref_specpop = max(ref_specpop, na.rm=TRUE),
            ref_other = max(ref_other, na.rm=TRUE),
            sup_info = max(sup_info, na.rm=TRUE),
            medicaid = max(medicaid, na.rm=TRUE),
            )

# Remove the newly aggregated columns from the original data to merge them on
dup <-  dup %>% 
  select(-c(client_transport, court_info, jra, ref_health, ref_abuse, ref_transport, ref_employ, ref_legal, ref_food, ref_mhsu, 
            ref_house, ref_educ, ref_specpop, ref_other, sup_info, medicaid)) %>% 
  arrange(SO_NUMBER, RELEASE_DATE) %>% 
  # Delete duplicates by SO number and release date
  distinct(SO_NUMBER, RELEASE_DATE, .keep_all= TRUE)

# Merge the aggregated variables back in
dup <- inner_join(dup, dupsum, by=c('SO_NUMBER', 'RELEASE_DATE'))

# Delete the duplicate rows from the original dataframe. Anti_join returns all rows in dataset 1 that are not contained in dataset 2
jrrp <- anti_join(jrrp, dup, by=c('SO_NUMBER', 'RELEASE_DATE'))

# Append the new rows
jrrp <- bind_rows(jrrp, dup)

# Delete dataframes that we no longer need
rm(dup, dupsum)


##################################
# JAIL DATA
##################################

jail <- jail %>% 
  # release date and come to datetime
  mutate(RELDATE = as_datetime(RELDATE),
         COMDATE = as_datetime(COMDATE),
         # New variable, RELEASE_DATE, is just date (not datetime), to merge with JRRP data
         RELEASE_DATE = as_date(RELDATE),
         # PCP (SO Number) has extra spaces. Convert to int
         PCP = as.numeric(PCP),
         # Degree in this query is the max degree of any charges associated with the booking. This can include warrant charges
         MAXDEGREE = DEGREE) %>% 
  # Filter to the end date of March 31, 2023
  filter(RELEASE_DATE < ymd("2023-04-01"))

# Tab missing variables
map(jail, ~sum(is.na(.)))



##################################
# MERGE JAIL AND JRRP DATA
##################################

# Merge data
m <- full_join(jail, jrrp, by=c('PCP'='SO_NUMBER', 'RELEASE_DATE'='RELEASE_DATE'), na_matches="never") %>% 
  # Indicate whether each row was joined, left only (jail no join), or right only (jrrp no join)
  mutate(join_type = case_when(is.na(SYSID) & !is.na(ID) ~ "jrrp_only",
                               !is.na(SYSID) & is.na(ID) ~ "jail_only",
                               !is.na(SYSID) & !is.na(ID) ~ "merged"
                               )) %>% 
  # Variable for whether or not received JRRP services
  # If there is a value in ID, which is the Smartsheet ID,  the release went through jrrp
  mutate(jrrp = if_else(is.na(ID), 0, 1))

## RIGHT_ONLY shows the number of JRRP entries that are not merged into the jail data
m %>% count(join_type)


# Save these records in their own dataframe to look at later
unmatched <- m %>%
  filter(join_type == "jrrp_only")

# In the dataset of JRRP entries where the release date was different from the entry date, mark which ones didn't match to the jail data
reldatedif <- reldatedif %>% 
  mutate(mergedJail = if_else(ID %in% unmatched$ID, "No jail record", "Merged with jail record"))


write.csv(reldatedif %>% select(ID, CLIENT_NAME, ENTRY_DATE, RELEASE_DATE,
dif, mergedJail), "Data/Smartsheet Records with Different Entry and Release Date 5-4-23.csv")


##### MARNI: as of 4/14/23, there were 148 JRRP rows that did not merge
# For now, drop these records and drop the merge variable
m <- m %>% 
  filter(!is.na(SYSID)) %>% 
  select(-join_type)

 
##################################
# Merge in demographic info
##################################
# Format demographic data
demog <- demog %>% 
   mutate(DOB = as_date(DOB),
          PCP = as.numeric(PCP)
          )

# Merge  in demographic data
m <- left_join(m, demog, by="PCP") %>% 
  mutate(age =  year(as.period(interval(start=DOB, end=RELEASE_DATE))), # age at the time of release
         demog_join_type = case_when(!is.na(RELDATE) & is.na(DOB) ~ "left_only",
                                     !is.na(RELDATE) & !is.na(DOB) ~ "merged"
  ))

# How many bookings are missing demographic info?
# Tab the merge variable
m %>% count(demog_join_type)

# Delete the merge variable
m <- select(m, -demog_join_type)



##################################
# Clean and merge in charge data

# The CHG_RANK variable indicates which charge is the highest charge from the first case.
# The first case is generally the reason someone was booked
# Older unresolved cases can also be added as charges
# Occasionally someone may receive a new charge, indicated by a larger case number, for a higher degree
# For now, only keep the first charge

# CHGDEGREE is the degree associated with the reason someone was booked.
# MAXDEGREE from the earlier jail query is the max degree associated with the booking
# For example, someone may be brought in for a DUI. The CHGDEGREE would show the degree related to the DUI. 
# But while they are in jail they may receive a new charge (it takes the DA a while to process larger charges), 
# or, maybe they had a warrent out for a larger charge. MAXDEGREE shows the most serious degree of all charges during that booking.
##################################


chgsm <- chg %>% 
  filter(CHG_RANK == 1) %>% 
  mutate(CHGDEGREE = DEGREE) %>% 
  select(SYSID, OFFENSE_CODE, OFFENSE_DESCRIPTION, CHGDEGREE, CASE_NUMBER, 
         ADM_TYPE_DESC, MY_NCIC_TYPE_MOD, CRIME_TYPE)

# Merge with main data
m <- left_join(m, chgsm, by="SYSID") %>% 
  mutate(chg_join_type = case_when(!is.na(RELDATE) & is.na(OFFENSE_CODE) ~ "left_only",
                                     !is.na(RELDATE) & !is.na(OFFENSE_CODE) ~ "merged"
         ))

# How many bookings are missing charge info?
# Tab the merge variable
m %>% count(chg_join_type)

# Delete the merge variable
m <- select(m, -chg_join_type)


##################################
# Merge in lifetime booking data
# Thhis data is not aggregated - it is just a file of all lifetime bookings
##################################

# Format variables and drop rows with missing PCP
lifebk <- lifebk %>% 
  mutate(RELDATE = as_datetime(RELDATE),
         COMDATE = as_datetime(COMDATE),
         PCP = as.numeric(PCP)) %>% 
  filter(!is.na(PCP))


# For each booking in m, we want to count how many bookings occured before the commitement date
m <- m %>% 
  filter(!is.na(PCP)) %>% 
  mutate(lifeBookings = map2_int(PCP, COMDATE, function(x,y) sum(lifebk$PCP == x & lifebk$COMDATE < y) ))


# How many bookings are missing charge info?
# Tab the merge variable
m %>% count(is.na(lifeBookings))



##################################
# Add in outcome variables
##################################

# # Filter out 2 obs where the release date is less than the commitment date
m <- m %>% 
  filter(RELDATE >= COMDATE)

# Re-booking variables
m <- m %>% 
  group_by(PCP) %>%
  mutate(
    # Rebooked with any admission type
    rebook30 = map_lgl(RELDATE, function(x) any(COMDATE > x & COMDATE <= x + days(30))),
    rebook60 = map_lgl(RELDATE, function(x) any(COMDATE > x & COMDATE <= x + days(60))),
    rebook90 = map_lgl(RELDATE, function(x) any(COMDATE > x & COMDATE <= x + days(90))),
    rebook182 = map_lgl(RELDATE, function(x) any(COMDATE > x & COMDATE <= x + days(182))),
    # Rebooked on a new charge
    rebook30_nc = map_lgl(RELDATE, function(x) any(COMDATE > x & COMDATE <= x + days(30) & ADM_TYPE_DESC=="New Charge")),
    rebook60_nc = map_lgl(RELDATE, function(x) any(COMDATE > x & COMDATE <= x + days(60) & ADM_TYPE_DESC=="New Charge")),
    rebook90_nc = map_lgl(RELDATE, function(x) any(COMDATE > x & COMDATE <= x + days(90) & ADM_TYPE_DESC=="New Charge")),
    rebook182_nc = map_lgl(RELDATE, function(x) any(COMDATE > x & COMDATE <= x + days(182) & ADM_TYPE_DESC=="New Charge")),
    # Rebooked on warrant services 
    rebook30_w = map_lgl(RELDATE, function(x) any(COMDATE > x & COMDATE <= x + days(30) & ADM_TYPE_DESC=="Warrant Service")),
    rebook60_w = map_lgl(RELDATE, function(x) any(COMDATE > x & COMDATE <= x + days(60) & ADM_TYPE_DESC=="Warrant Service")),
    rebook90_w = map_lgl(RELDATE, function(x) any(COMDATE > x & COMDATE <= x + days(90) & ADM_TYPE_DESC=="Warrant Service")),
    rebook182_w = map_lgl(RELDATE, function(x) any(COMDATE > x & COMDATE <= x + days(182) & ADM_TYPE_DESC=="Warrant Service"))
  ) %>%
  ungroup()

m %>% count(rebook30)
m %>% count(rebook60)
m %>% count(rebook90)


####################################
# Variables for survival analysis
# https://www.datacamp.com/tutorial/survival-analysis-R
####################################

# Survival analysis variables - days to next booking for any reason
m <- m %>% 
  arrange(PCP, COMDATE) %>% 
  group_by(PCP) %>% 
  # create a new variable with the release date of the next booking
  mutate(next_comdate = lead(COMDATE)) %>% 
  ungroup %>% 
  # calculate the number of days between the release date and the next booking. Convert both to dates rather than date-time
  mutate(daysToNextBk = as.numeric(difftime(as_date(next_comdate), as_date(RELDATE), units="days")),
         # If there is no subsequent booking, we record that in the "censored" variable. Censored means we record follow up time until our cut-off point. 
         # So the time doesn't mean time to event (here, rebooking), it means time till the end of follow-up
         hasRB = if_else(is.na(daysToNextBk), 0, 1),
         # For people who are censored (i.e. no subsequent re-booking), use the time from their release till the end of the time period (March 31, 2023)
         daysToNextBk = if_else(hasRB==0, as.numeric(difftime(ymd("2023-03-31"), as_date(RELDATE), units="days")), daysToNextBk)
         )


# Days to next booking for new charges
m <- m %>% 
  # Days to next New Charges Booking
  mutate(daysToNextBk_nc = map2_int(RELDATE, PCP, function(x, y){
    # Filter the dataset to the same person and all bookings where the comdate is after this release date and for this type
    temp <- m %>% filter(PCP==y & COMDATE > x & ADM_TYPE_DESC=="New Charge")
    # Save the earliest comdate
    if (nrow(temp)>0) {
      nextdate <- min(temp$COMDATE)
      days <- as.numeric(difftime(as_date(nextdate), as_date(x), units="days"))
    } else {          # Otherwise code as 999 to change later 
      days <- 999
    }

    return(days)
  }),
  hasRB_nc = if_else(daysToNextBk_nc==999, 0, 1),
  # For people who are censored (i.e. no subsequent re-booking), use the time from their release till the end of the time period (March 31, 2023)
  daysToNextBk_nc = if_else(daysToNextBk_nc==999, as.numeric(difftime(ymd("2023-03-31"), as_date(RELDATE), units="days")), daysToNextBk_nc)
  )

# Days to next booking for warrants
m <- m %>% 
  # Days to next New Charges Booking
  mutate(daysToNextBk_w = map2_int(RELDATE, PCP, function(x, y){
    # Filter the dataset to the same person and all bookings where the comdate is after this release date and for this type
    temp <- m %>% filter(PCP==y & COMDATE > x & ADM_TYPE_DESC=="Warrant Service")
    # Save the earliest comdate
    if (nrow(temp)>0) {
      nextdate <- min(temp$COMDATE)
      days <- as.numeric(difftime(as_date(nextdate), as_date(x), units="days"))
    } else {    # Otherwise code as 999 to change later 
      days <- 999
    }
    
    return(days)
  }),
  hasRB_w = if_else(daysToNextBk_w==999, 0, 1),
  # For people who are censored (i.e. no subsequent re-booking), use the time from their release till the end of the time period (March 31, 2023)
  daysToNextBk_w = if_else(daysToNextBk_w==999, as.numeric(difftime(ymd("2023-03-31"), as_date(RELDATE), units="days")), daysToNextBk_w)
  )





# view(m %>% select(PCP, COMDATE, RELDATE, next_comdate, daysToNextBk, hasRB))


# Add date variables
m <- m %>% 
  # Days in jail
  mutate(daysinjail = as.numeric(as_date(RELDATE) - as_date(COMDATE)),
         # Day of the week, hour, and month of release
         RELEASE_WKDAY = wday(RELDATE, label = TRUE),
         RELEASE_HR = hour(RELDATE),
         RELEASE_MONYR = factor(format(RELDATE, "%b-%y"),
                                levels = c(levels = "Apr-22", "May-22", "Jun-22", "Jul-22", "Aug-22", "Sep-22", "Oct-22", "Nov-22", "Dec-22", "Jan-23", "Feb-23", "Mar-23")
                                )
  )


# JRRP Hours
m <- m %>% 
  mutate(relJRRPhrs = factor(if_else(
    # April 1, 2022 – 4-hour blocks three days per week (hours varied) - we won't use data from here
    # April 27, 2022 – Monday – Friday, 7am-3pm
    (ymd('2022-04-27') <= RELEASE_DATE & RELEASE_DATE < ymd('2022-05-31') & RELEASE_WKDAY %in% c("Mon", "Tue", "Wed", "Thu", "Fri") & 7 <= RELEASE_HR & RELEASE_HR < 15) |
    
    # May 31, 2022 – Monday – Friday, 7am-7pm
    (ymd('2022-05-31') <= RELEASE_DATE & RELEASE_DATE < ymd('2022-10-03') & RELEASE_WKDAY %in% c("Mon", "Tue", "Wed", "Thu", "Fri") & 7 <= RELEASE_HR & RELEASE_HR < 19) |
      
    # October 3, 2022 – Monday – Friday, 7am-11pm
    (ymd('2022-10-03') <= RELEASE_DATE & RELEASE_WKDAY %in% c("Mon", "Tue", "Wed", "Thu", "Fri") & 7 <= RELEASE_HR & RELEASE_HR < 23) |
    
    # October 30, 2022 – Saturday/Sunday 7am-3pm added
    (ymd('2022-10-30') <= RELEASE_DATE & RELEASE_WKDAY %in% c("Sat", "Sun") & 7 <= RELEASE_HR & RELEASE_HR < 15) |
      
    # If someone received JRRP services, regardless of the time, mark as a 1
    jrrp == 1,
    
    # If any of the above are true, mark as 1
    1,
    # Else, 0
    0
  ), levels = c(0, 1), labels = c("No", "Yes")))


# Turn JRRP variable into a factor and add labels
m <- m %>%
  mutate(jrrp = factor(jrrp,
                       levels = c(0,1),
                       labels = c("No", "Yes")))

# Check missing
m %>%
  summarize(missing_jrrp = sum(is.na(jrrp)))

table(m$RELEASE_MONYR, m$jrrp)



# And add variable for first record in the time period
m <- m %>% 
  # New variables for first record in the time period and first record in the time period during JRRP hours
  group_by(PCP) %>% 
  mutate(firstrec = ifelse(RELDATE == min(RELDATE), 1, 0),
         firstrec_jrrphr = ifelse(RELDATE == min(RELDATE) & relJRRPhrs == "Yes", 1, 0)
         ) %>% 
  ungroup()

m %>% count(firstrec, jrrp)
m %>% count(firstrec_jrrphr, jrrp)

#Variable for qualifying releases. Qualifying releases are those release types that pass through the JRRP room
m <- m %>% 
  mutate(qualrel = factor(if_else(REL_DESC %in% c(
    "BAL - CASH BAIL",
    "BND - BONDED",
    "DIS - CHARGES DISMISSED",
    "NOC - NO COMPLAINT",
    "NPC - NO PROBABLE CAUSE",
    "OCD - OTHER COUNTY DECLINED",
    "OCR - OVERCROWD RELEASE",
    "ORL - ORDER RELEASE",
    "ORP - ORDER REL PRETRIAL SERV",
    "PFR - PREFILE RELEASE",
    "PRB - PROPERTY BOND",
    "PT$ - PRE-TRIAL & CASH",
    "PTA - PROMISE TO APPEAR",
    "PTB - PRE-TRIAL & BOND",
    "PTR - PRE-TRIAL OWN RECOG",
    "PTS - PRE-TRIAL SUPERVISED RELEASE",
    "SUM - BOOK & RELEASE SUMMONS",
    "TMS - TIME SERVED",
    "UNSECURED BOND"), 
    "Yes", "No")))


# Turn some variables into bins

m <- m %>% 
  mutate(
    daysinjail_bins = factor(case_when(
      daysinjail == 0 ~ "0",
      daysinjail <= 1 ~ "1",
      daysinjail <= 3 ~ "2-3",
      daysinjail <= 6 ~ "4-6",
      daysinjail <= 18 ~ "7-18",
      daysinjail <= 70 ~ "19-70",
      daysinjail >= 71 ~ "71+"
    ), levels = c("0", "1", "2-3", "4-6", "7-18", "19-70", "71+")),
    # Previous bookings. This is the number of bookings before this one
    prevBookingsbins = factor(case_when(
      lifeBookings == 0 ~ "0",
      lifeBookings <= 3 ~ "1-3",
      lifeBookings <= 8 ~ "4-8",
      lifeBookings <= 17 ~ "9-17",
      lifeBookings >= 18 ~ "18+"
    ), levels = c("0", "1-3", "4-8", "9-17", "18+")),
    relsubstr = substr(REL_DESC, 1, 3),
    reltype_bins = factor(case_when(
      relsubstr %in% c("USP", "UOU", "TSI", "RST", "HSP", "FBI") ~ "Release to Other Authority",
      relsubstr %in% c("PTS", "PTB", "PT$", "ORP") ~ "Release to Supervision",
      relsubstr %in% c("SUM", "OCR", "NPC", "Inf", "DIS", "BND", "BAL", "NOC", "OCD", "ORL", "PFR", "PRB", "PTA", "PTR", "TMS", "UNS") ~ "Release to Own Recognizance",
      relsubstr %in% c("RIN", "LDA" , "ADM") ~ "Release to Treatment",
      TRUE ~ relsubstr
    )),
    ADM_TYPE_DESC = factor(ADM_TYPE_DESC),
    age = trunc((DOB %--% RELDATE) / years(1)),
    ageBins = factor(case_when(
      age <= 26 ~ "18-26",
      age <= 32 ~ "27-32",
      age <= 38 ~ "33-38",
      age <= 45 ~ "39-45",
      age > 45 ~ "46+"
    ), levels = c("18-26", "27-32", "33-38", "39-45", "46+"))
  )

# Combine race and ethnicity variables
m <- m %>% 
  mutate(raceethn = factor(case_when(
    ETHNICITY == "Hispanic" ~ "Hispanic",
    ETHNICITY != "Hispanic" & RACE == "White" ~ "Non-Hispanic White",
    ETHNICITY != "Hispanic" & RACE == "Black" ~ "Non-Hispanic Black",
    TRUE ~ RACE
  )))

# Use quintiles (or more) to determine the bin groups
quantile(m$daysinjail, probs = seq(0, 1, 1/10))
quantile(m$prevBookings, probs = seq(0, 1, 1/5))
quantile(m$age, probs = seq(0, 1, 1/5), na.rm= TRUE)


# Clean a few variables
m %>% count(MAXDEGREE)
m %>% count(CHGDEGREE)
m %>% count(CRIME_TYPE)

m <- m %>% 
  mutate(MAXDEGREE = str_trim(toupper(MAXDEGREE)),
         MAXDEGREE = if_else(MAXDEGREE %in% c("", "F", "GW", "IN", "M4", "NA"), NA, MAXDEGREE),
         CHGDEGREE = str_trim(toupper(CHGDEGREE)),
         CHGDEGREE = if_else(CHGDEGREE %in% c("", "F", "GW", "IN", "M4", "MBM", "NA"), NA, CHGDEGREE)
         )

# Save
save(m, jrrp, file="Data/mergeddata.RData")

# write.csv(m, "Data/m.csv")




