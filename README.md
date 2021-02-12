# Veil of Darkness: San Diego County Sheriff's Department
By: [Lauryn Schroeder](https://www.sandiegouniontribune.com/sdut-lauryn-schroeder-staff.html) and [Lyndsay Winkley](https://www.sandiegouniontribune.com/sdut-lyndsay-winkley-staff.html)

This repository contains data and code for the analysis [reported and published](XXXXXX) by *The San Diego Union-Tribune* on Feb. XXXXX, 2021.

### About

The Racial and Identity Profiling Act of 2015 (RIPA) requires nearly all California law enforcement agencies to submit demographic data on all
detentions and searches. The Union-Tribune obtained in January stop data from the San Diego County Sheriff's Department under the California Public Records Act.

The Union-Tribune collected this data to conduct what is known as a "Veil of Darkness" test, which is used by criminal justice researchers across the country. It tests for racial profiling by determining whether law officers pulled over people of a particular race at a higher rate when it’s light outside — and race is presumably more visible — than when it’s dark.

The Union-Tribune modeled its Veil of Darkness analysis on Stanford University's [Open Policing Project](https://openpolicing.stanford.edu/tutorials/) and the methodology of previous Veil of Darkness analyses conducted by the [RIPA Board](https://oag.ca.gov/sites/all/files/agweb/pdfs/ripa/ripa-board-report-2020.pdf).

### Methodology Notes

The sheriff department's data contains all pedestrian and traffic stops from July 2018 through June 2020. The original data tables are compiled in four, newline-deliminated JSON files, with nested data frames for each individual involved in each stop. Since more than one individual can be involved a stop (deputies are required to record the ethnicity of drivers and passengers) the Union-Tribune opted to analyze the race of each person involved, which is the same technique used by RIPA officials.

To avoid analyzing an incomplete set of data in 2018 and incorporating abnormalities in driving patterns due to the coronavirus pandemic in early 2020, the Union-Tribune opted to conduct its Veil of Darkness analysis on the 44,500 individuals involved in traffic stops in 2019.

Of the 44,500 stops, the Union-Tribune chose to analyze stops that occurred during what is known as the “inter-twilight period,” which is the range from the earliest time of dusk in a year to the latest time dusk occurs in that same year.

Analyzing stops in this inter-twilight period adjusts for both the time of day and the demographics of commuters, since commuters who leave work at 6 p.m. will most likely be on the road at the same time each day, regardless of whether it’s light or dark outside.

This filter also allows for a more accurate analysis of how darkness plays a role in traffic stops throughout the year, since sunset occurs at different times throughout the year - earlier in the winter and later in the summer.

The Union-Tribune removed any stops that occurred in an approximate 30-minute window between sunset and dusk, also known as the end of civil twilight, as the amount of light during this time is more open to interpretation and is not blatantly night or day.

In some circumstances, deputies list more than one race for an individual involved in traffic stops. 

Individuals who were perceived by deputies as Hispanic and any other race, were included in Hispanic totals. Individuals perceived as more than one race were categorized as those with two or more race. The remaining race categories were left the same.

When conducting a Veil of Darkness test, latitude and longitude coordinates for each stop, along with the time in which a stop occurred, are typically used to determine if a stop took place during the day or at night.

Since these coordinates were not provided by the sheriff’s department, the Union-Tribune used the latitude and longitude coordinates of the city in which the stop occurred.

Coordinates for cities in San Diego County with a population of more than 500 people were obtained through [GeoNames](http://download.geonames.org/export/dump/), an online geographical database that collects and maintains information for more than 25 million locations in the world. Coordinates for smaller cities and unincorporated areas of the county were collected manually by the Union-Tribune. When stops occurred in an unincorporated area of the county, the Union-Tribune manually collected the center of the unincorporated areas and matched these data points to the original table.

### The SDUT-vod repository contains the following:

- `cities500.txt` - Latitude and longitude coordinates of cities in world with population of 500 or more. Original data from [GeoNames](http://download.geonames.org/export/dump/).
- `extra-unincorporated-cities-lat-long.csv` - Latitude and longitude coordinates of cities not in GeoNames database. Collected manually by the Union-Tribune.
- `offense_codes.csv` - Table of offense codes and descriptions from the [California Department of Justice / Attorney General](https://oag.ca.gov/law/code-tables).
- `JAN_-_JUN_2019.txt` - Stop data from San Diego Sheriff's Department. Contains stops from January through June 2019.
- `JAN_-_JUN_2020.txt` - Stop data from San Diego Sheriff's Department. Contains stops from January through June 2020.
- `JUL_-_DEC_2018.txt` - Stop data from San Diego Sheriff's Department. Contains stops from July through December 2018.
- `JUL-DEC_2019.txt` - Stop data from San Diego Sheriff's Department. Contains stops from July through December 2019.
- `vod-analysis-sheriff.R` - Import and analysis R script documenting findings published by the Union-Tribune.

### Sourcing
Please link and source [*The San Diego Union-Tribune*](https://www.sandiegouniontribune.com/) when referencing any analysis or findings in published work.

### Questions / Feedback

Email Lauryn Schroeder at [lauryn.schroeder@sduniontribune.com](mailto:lauryn.schroeder@sduniontribune.com) or Lyndsay Winkley at [lyndsay.winkley@sduniontribune.com](mailto:lyndsay.winkley@sduniontribune.com).
