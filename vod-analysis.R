# Libraries
library(jsonlite)
library(tibble)
library(tidyr)
library(beepr)
library(dplyr)
library(purrr)
library(stringr)
library(stringi)
library(readr)
library(chron)
library(lubridate)
library(suncalc)

# Set working directory
setwd("~/Desktop/sdut-vod-analysis")

###################################################################
######################## IMPORT 1ST DF ############################
###################################################################

# Import
library(jsonlite)
test1 <- stream_in(file("JUL_-_DEC_2018.txt"))

# Flatten
test1 <- jsonlite::flatten(test1)

# Create df
library(tibble)
test1_tbl <- as_data_frame(test1)

library(tidyr)
test1_tbl <- test1_tbl %>% 
  # take Person_Stopped out of its nested structure and unfurl it
  # Change the data from one line per stop to one line per person in a stop 
  unnest(cols = c(ListPerson_Stopped.Person_Stopped)) %>% 
  # flatten what came out of it
  jsonlite::flatten() %>% 
  # convert back to tibble
  as_tibble()

library(beepr)
beepr::beep()

# check ethnicity, which is still a list
library(dplyr)
test1_tbl %>% 
  select(Perc.ListEthn.Ethn) %>%
  unnest(c(Perc.ListEthn.Ethn)) %>% 
  count(Perc.ListEthn.Ethn, sort = TRUE)

library(purrr)
library(stringr)
# all single digit codes, but many people have more than one ethnicity
# smash it together, sort the numbers and return a character
jd18 = test1_tbl %>% 
  mutate(Perc.ListEthn.Ethn = map_chr(Perc.ListEthn.Ethn, ~str_c(sort(.x), collapse = '|')))

nrow(jd18) # this should be the number of people stopped in the data 
n_distinct(jd18$LEARecordID) # this should match the number of lines in original df

# Select columns we need for vod
jd18 <- jd18 %>% 
  select(LEARecordID, SDate, STime, SDur, Location.Loc, Location.City, PID,
         Is_Stud, Perc.Age, Perc.Is_LimEng, Perc.Gend, Perc.GendNC, Perc.LGBT,
         Perc.ListEthn.Ethn, PrimaryReason.StReas, PrimaryReason.StReas_N,
         PrimaryReason.Tr_ID, PrimaryReason.Tr_O_CD)

# Remove originals
remove(test1, test1_tbl)

###################################################################
######################## IMPORT 2ND DF ############################
###################################################################

# Import
test2 <- stream_in(file("JAN_-_JUN_2019.txt"))

# Flatten
test2 <- jsonlite::flatten(test2)

# Create df
test2_tbl <- as_data_frame(test2)

test2_tbl <- test2_tbl %>% 
  # take Person_Stopped out of its nested structure and unfurl it
  # Change the data from one line per stop to one line per person in a stop 
  unnest(cols = c(ListPerson_Stopped.Person_Stopped)) %>% 
  # flatten what came out of it
  jsonlite::flatten() %>% 
  # convert back to tibble
  as_tibble()

beepr::beep()

# check ethnicity, which is still a list
test2_tbl %>% 
  select(Perc.ListEthn.Ethn) %>%
  unnest(c(Perc.ListEthn.Ethn)) %>% 
  count(Perc.ListEthn.Ethn, sort = TRUE)

# all single digit codes, but many people have more than one ethnicity
# smash it together, sort the numbers and return a character
jj19 = test2_tbl %>% 
  mutate(Perc.ListEthn.Ethn = map_chr(Perc.ListEthn.Ethn, ~str_c(sort(.x), collapse = '|')))

nrow(jj19) # this should be the number of people stopped in the data 
n_distinct(jj19$LEARecordID) # this should match the number of lines in original df

# Select columns we need for vod
jj19 <- jj19 %>% 
  select(LEARecordID, SDate, STime, SDur, Location.Loc, Location.City, PID,
         Is_Stud, Perc.Age, Perc.Is_LimEng, Perc.Gend, Perc.GendNC, Perc.LGBT,
         Perc.ListEthn.Ethn, PrimaryReason.StReas, PrimaryReason.StReas_N,
         PrimaryReason.Tr_ID, PrimaryReason.Tr_O_CD)

# Remove originals
remove(test2, test2_tbl)

###################################################################
######################## IMPORT 3RD DF ############################
###################################################################

# Import
test3 <- stream_in(file("JUL-DEC_2019.txt"))

# Flatten
test3 <- jsonlite::flatten(test3)

# Create df
test3_tbl <- as_data_frame(test3)

test3_tbl <- test3_tbl %>% 
  # take Person_Stopped out of its nested structure and unfurl it
  # Change the data from one line per stop to one line per person in a stop 
  unnest(cols = c(ListPerson_Stopped.Person_Stopped)) %>% 
  # flatten what came out of it
  jsonlite::flatten() %>% 
  # convert back to tibble
  as_tibble()

beepr::beep()

# check ethnicity, which is still a list
test3_tbl %>% 
  select(Perc.ListEthn.Ethn) %>%
  unnest(c(Perc.ListEthn.Ethn)) %>% 
  count(Perc.ListEthn.Ethn, sort = TRUE)

# all single digit codes, but many people have more than one ethnicity
# smash it together, sort the numbers and return a character
jd19 = test3_tbl %>% 
  mutate(Perc.ListEthn.Ethn = map_chr(Perc.ListEthn.Ethn, ~str_c(sort(.x), collapse = '|')))

nrow(jd19) # this should be the number of people stopped in the data 
n_distinct(jd19$LEARecordID) # this should match the number of lines in original df

# Select columns we need for vod
jd19 <- jd19 %>% 
  select(LEARecordID, SDate, STime, SDur, Location.Loc, Location.City, PID,
         Is_Stud, Perc.Age, Perc.Is_LimEng, Perc.Gend, Perc.GendNC, Perc.LGBT,
         Perc.ListEthn.Ethn, PrimaryReason.StReas, PrimaryReason.StReas_N,
         PrimaryReason.Tr_ID, PrimaryReason.Tr_O_CD)

# Remove originals
remove(test3, test3_tbl)

###################################################################
######################## IMPORT 4TH DF ############################
###################################################################

# Import
test4 <- stream_in(file("JAN_-_JUN_2020.txt"))

# Flatten
test4 <- jsonlite::flatten(test4)

# Create df
test4_tbl <- as_data_frame(test4)

test4_tbl <- test4_tbl %>% 
  # take Person_Stopped out of its nested structure and unfurl it
  # Change the data from one line per stop to one line per person in a stop 
  unnest(cols = c(ListPerson_Stopped.Person_Stopped)) %>% 
  # flatten what came out of it
  jsonlite::flatten() %>% 
  # convert back to tibble
  as_tibble()

beepr::beep()

# check ethnicity, which is still a list
test4_tbl %>% 
  select(Perc.ListEthn.Ethn) %>%
  unnest(c(Perc.ListEthn.Ethn)) %>% 
  count(Perc.ListEthn.Ethn, sort = TRUE)

# all single digit codes, but many people have more than one ethnicity
# smash it together, sort the numbers and return a character
jj20 = test4_tbl %>% 
  mutate(Perc.ListEthn.Ethn = map_chr(Perc.ListEthn.Ethn, ~str_c(sort(.x), collapse = '|')))

nrow(jj20) # this should be the number of people stopped in the data 
n_distinct(jj20$LEARecordID) # this should match the number of lines in original df

# Select columns we need for vod
jj20 <- jj20 %>% 
  select(LEARecordID, SDate, STime, SDur, Location.Loc, Location.City, PID,
         Is_Stud, Perc.Age, Perc.Is_LimEng, Perc.Gend, Perc.GendNC, Perc.LGBT,
         Perc.ListEthn.Ethn, PrimaryReason.StReas, PrimaryReason.StReas_N,
         PrimaryReason.Tr_ID, PrimaryReason.Tr_O_CD)

# Remove originals
remove(test4, test4_tbl)

###################################################################
######################## CLEANING #################################
###################################################################

# Bind dfs together
master <- rbind(jd18, jj19, jd19, jj20)

# Rename columns
names(master)
names(master) <- c("id", "date", "time", "dur", 
                   "location", "city", "pid", "stud", 
                   "age", "lim_eng", "gender", "gendernc",
                   "lgbt", "eth", "reasonid", "reason_desc", "tr_id", "tr_o")

# Remove leading and trailing whitespace
master <- master %>% 
  mutate_if(is.character, str_trim)

# Remove all types of whitespace inside strings
master <- master %>% 
  mutate_if(is.character, str_squish)

# Remove floating commas
master$location <- gsub(" , ", ", ", master$location)
master$reason_desc <- gsub(" , ", ", ", master$reason_desc)

# Convert date into date format
master$date <- as.Date(master$date, "%m/%d/%Y")

# Time doesn't always have seconds / ending ":00")
## Count number of characters in string to insert :00
library(stringi)
master$count <- nchar(stri_escape_unicode(master$time))

# Paste ":00" at the end of times that have 5 characters in count column
master$time <- ifelse(master$count == 5, paste0(master$time, ":00"), master$time)

# Filter to traffic stops
## tr_id will be NA if it's not a traffic stop
### New row count == 82,826
final <- master %>% 
  filter(!is.na(master$tr_id))

# Remove count column
final <- final %>% 
  select(-count)

###################################################################
#################### GET LAT AND LONGS ############################
###################################################################

# Import city lat and long coordinates data
library(readr)
cities <- read_delim("cities500.txt", 
                     "\t", escape_double = FALSE, col_names = FALSE, 
                     trim_ws = TRUE)

# Name columns
names(cities) <- c("geonameid", "name", "asciiname", "alternatenames", 
                   "lat", "long", "feature_class", "feature_code", 
                   "country_code", "cc2", "fips", "county", "code1",
                   "code2", "pop", "elev", "dem",
                   "time_zone", "mod_date")

# Filter to US / CA / SD County
cities <- cities %>% 
  filter(country_code == "US" &
           fips == "CA" &
           county == "073")

# Select columns we need
cities <- cities %>% 
  select(geonameid, name, lat, long)

# Import list of smaller cities, collected manually
extra_cities <- read_csv("extra-unincorporated-cities-lat-long.csv")

# Bind two city dfs
cities_final <- rbind(cities, extra_cities)
remove(cities, extra_cities)

# Make city names uppercase for merge
cities_final <- mutate_each(cities_final, funs(toupper))

# Rename column for merge
cities_final <- rename(cities_final, city = name)

# Remove leading and trailing whitespace
cities_final <- cities_final %>% 
  mutate_if(is.character, str_trim)

# Remove all types of whitespace inside strings
cities_final <- cities_final %>% 
  mutate_if(is.character, str_squish)

# Join final and cities
final <- left_join(final, cities_final, by = "city")

# Remove cities_final
remove(cities_final)

# Remove originals and master to clean environment (Optional)
# remove(jd18, jd19, jj19, jj20, master)

###################################################################
####################### CLEAN RACE ################################
###################################################################

# Create new column for race, written out
final <- final %>% 
  mutate(race_words = str_replace_all(eth, "7", "white") %>% 
           str_replace_all("6", "pi") %>% 
           str_replace_all("5", "nam") %>% 
           str_replace_all("4", "me_sa") %>% 
           str_replace_all("3", "hisp") %>% 
           str_replace_all("2", "black") %>% 
           str_replace_all("1", "asian"))

# Create new race category for vod test
## Hispanic + other race == "hisp"
## More than one race (but not hisp) == "mixed"
final <- final %>% 
  mutate(race_condensed = case_when(str_detect(race_words, "hisp") ~ "hisp", # if contains "hisp", add to hispanic category
                                    str_detect(race_words, "\\|") ~ "mixed", # if contains "|", create mixed category
                                    TRUE ~ race_words)) # if neither above is true, paste original from race_words

# Remove race_words column
final <- final %>% 
  select(-race_words)

# Create second time column that's in time format
library(chron)
final$time2 <- times(final$time)

###################################################################
############### IMPORT TRAFFIC CITATION DESCRIPTIONS ##############
###################################################################

offenses <- read_csv("offense_codes.csv", 
                     col_types = cols(code1 = col_skip(), 
                                      code10 = col_skip(), code11 = col_skip(), 
                                      code2 = col_skip(), code3 = col_skip(), 
                                      code4 = col_skip(), code5 = col_skip(), 
                                      code6 = col_skip(), code7 = col_skip(), 
                                      code8 = col_skip(), code9 = col_skip(), 
                                      id = col_skip(), tr_o = col_character()))

# Merge with final
final <- left_join(final, offenses, by = "tr_o")

remove(offenses)

###################################################################
########################## VOD PREP ###############################
###################################################################

# Filter to stops in 2019
### New row count == 44,524
library(lubridate)
final19 <- final %>% 
  filter(year(date) == 2019)

# Set lubridate time zone
tz <- "America/Los_Angeles"

# Create tables of SD County center_lat and center_long
center_lat <- 33.02573574851674
center_lng <- -116.74773330323929

# Time helper function
time_to_minute <- function(time) {
  hour(hms(time)) * 60 + minute(hms(time))
}

library(suncalc)
# Compute sunset time for each date in the dataset
sunset_times19 <- 
  final19 %>%
  mutate(
    lat = center_lat,
    lon = center_lng
  ) %>% 
  select(date, lat, lon) %>%
  distinct() %>%
  getSunlightTimes(
    data = ., 
    keep = c("sunset", "dusk"), 
    tz = tz
  ) %>% 
  mutate_at(vars("sunset", "dusk"), ~format(., "%H:%M:%S")) %>% 
  mutate(
    sunset_minute = time_to_minute(sunset),
    dusk_minute = time_to_minute(dusk),
    date = ymd(str_sub(date, 1, 10))) %>% 
  select(date, sunset, dusk, ends_with("minute"))

# Determine inter-twilight period
sunset_times19 %>% 
  filter(dusk == min(dusk) | dusk == max(dusk))
# date   sunset        dusk       sunset_minute dusk_minute
# 2019-06-29 20:00:55 20:29:35          1200        1229
# 2019-06-28 20:00:53 20:29:35          1200        1229
# 2019-12-04 16:41:26 17:08:31          1001        1028
# 2019-12-03 16:41:28 17:08:31          1001        1028

###################################################################
######################## VOD ANALYSIS #############################
###################################################################

# Merge sunset_times19 with final19
## Create TRUE/FLASE columns for race (to be used in logistic regression tests)
all19 <- 
  final19 %>% 
  left_join(
    sunset_times19,
    by = "date"
  ) %>% 
  mutate(
    minute = time_to_minute(time),
    minutes_after_dark = minute - dusk_minute,
    is_dark = minute > dusk_minute,
    min_dusk_minute = min(dusk_minute),
    max_dusk_minute = max(dusk_minute),
    is_white = race_condensed == "white",
    is_hisp = race_condensed == "hisp",
    is_black = race_condensed == "black",
    is_asian = race_condensed == "asian",
    is_me_sa = race_condensed == "me_sa",
    is_pi = race_condensed == "pi",
    is_nam = race_condensed == "nam",
    is_mixed = race_condensed == "mixed",
  )

# Filter to the inter-twilight period;
## Remove ambiguous pre-dusk period;
it19 <- all19 %>% 
  filter(
    # Filter to get only the inter-twilight period
    minute >= min_dusk_minute,
    minute <= max_dusk_minute,
    # Remove ambiguous period between sunset and dusk
    !(minute > sunset_minute & minute < dusk_minute)
  )

# Compute proportion of stops by race when it was dark vs proportion when it was light
it19 %>% 
  group_by(is_dark) %>% 
  summarise(prop_white = mean(is_white),
            prop_hisp = mean(is_hisp),
            prop_black = mean(is_black),
            prop_asian = mean(is_asian),
            prop_me_sa = mean(is_me_sa),
            prop_pi = mean(is_pi),
            prop_nam = mean(is_nam),
            prop_mixed = mean(is_mixed))

# is_dark prop_white prop_hisp prop_black prop_asian prop_me_sa prop_pi prop_nam prop_mixed
# DAY        0.499     0.330     0.0686     0.0452     0.0388 0.00904  0.00585    0.00319
# NIGHT      0.468     0.364     0.0772     0.0397     0.0372 0.00826  0.00248    0.00289

###################################################################
######################### VOD LOG REG #############################
###################################################################

# Pacific Islander glm test
pi_test <- glm(
  is_pi ~ is_dark + splines::ns(minute, df = 6),
  family = binomial,
  data = it19
)

summary(pi_test)$coefficients["is_darkTRUE", c("Estimate", "Std. Error")]
# Estimate   Std. Error 
# 0.1569318  0.4176738

summary(pi_test)
# p value = 0.707
## Not significant

# Asian glm test
asian_test <- glm(
  is_asian ~ is_dark + splines::ns(minute, df = 6),
  family = binomial,
  data = it19
)

summary(asian_test)$coefficients["is_darkTRUE", c("Estimate", "Std. Error")]
# Estimate   Std. Error 
# -0.3752199  0.1959879

summary(asian_test)
# p value = 0.0556 .
## Significant, though borderline

# Run Middle Eastern / South Asian glm test
mesa_test <- glm(
  is_me_sa ~ is_dark + splines::ns(minute, df = 6),
  family = binomial,
  data = it19
)

summary(mesa_test)$coefficients["is_darkTRUE", c("Estimate", "Std. Error")]
# Estimate   Std. Error 
# -0.03027813  0.20643665

summary(mesa_test)
# p value = 0.88339
## Not significant

# Native American glm test
nam_test <- glm(
  is_nam ~ is_dark + splines::ns(minute, df = 6),
  family = binomial,
  data = it19
)

summary(nam_test)$coefficients["is_darkTRUE", c("Estimate", "Std. Error")]
# Estimate Std. Error 
# -0.6115345  0.5907973 

summary(nam_test)
# p value = 0.301
## Not significant

# Mixed race glm test
mixed_test <- glm(
  is_mixed ~ is_dark + splines::ns(minute, df = 6),
  family = binomial,
  data = it19
)

summary(mixed_test)$coefficients["is_darkTRUE", c("Estimate", "Std. Error")]
# Estimate   Std. Error 
# 0.0321382  0.6819646

summary(mixed_test)
# p value = 0.9624
## Not significant

###################################################################
################## FURTHER ANALYSIS OF STOPS ######################
###################################################################

# Calculate percents for race of individuals in all stops (July 2018 through June 2020)
race_all <- final %>% 
  group_by(race_condensed) %>% 
  summarise(total = n()) %>% 
  mutate(per_total = round((total / sum(total))*100,1))

# Calculate percents for race of 2019 stops
race19 <- final19 %>% 
  group_by(race_condensed) %>% 
  summarise(total = n()) %>% 
  mutate(per_total = round((total / sum(total))*100,1))

######################################################################
# Analysis of traffic offense types in 2019 stops
off19 <- final19 %>% 
  group_by(traffic_desc) %>% 
  summarise(total = n()) %>% 
  mutate(per_total = round((total / sum(total))*100,1))

######################################################################
# Percent of stops by day and by night for stops in 2019
dn19 <- all19 %>% 
  group_by(is_dark) %>% 
  summarise(total = n()) %>% 
  mutate(per_total = round((total / sum(total))*100,1))

# Percent of stops by day and night for stops in 2019
## During inter-twilight period
dn19_it <- it19 %>% 
  group_by(is_dark) %>% 
  summarise(total = n()) %>% 
  mutate(per_total = round((total / sum(total))*100,1))

######################################################################
# Percent of stops for each race that occurred during day and night
## 2019 stops
race_dn <- all19 %>% 
  group_by(race_condensed, is_dark) %>% 
  summarise(total = n())

# Spread
race_dn <- race_dn %>%
  spread(key = is_dark, value = total, fill = 0)

# Rename columns
names(race_dn) <- c("race_condensed", "day", "night")

# Add percentages
race_dn <- race_dn %>%
  mutate(day_per = round((day / (day + night))*100,1),
         night_per = round((night / (day + night))*100,1))

# Percent of stops for each race that occurred during day and night
## 2019 stops
## Inter-twilight period
race_dn_it <- it19 %>% 
  group_by(race_condensed, is_dark) %>% 
  summarise(total = n())

# Spread
race_dn_it <- race_dn_it %>%
  spread(key = is_dark, value = total, fill = 0)

# Rename columns
names(race_dn_it) <- c("race_condensed", "day", "night")

# Add percentages
race_dn_it <- race_dn_it %>%
  mutate(day_per = round((day / (day + night))*100,1),
         night_per = round((night / (day + night))*100,1))

######################################################################
# Total number of "stops" vs individuals (based on stop ID) in 2019
stops19 <- final19 %>% 
  group_by(id) %>%
  summarise(total = n())
## Stops == 43,290
## vs Individuals == 44,524

