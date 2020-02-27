library(dplyr)
library(DBI)
library(Hmisc)
library(lubridate)
library(purrr)
library(readr)
library(snakecase)
library(RMariaDB)
library(stringi)
library(tidyr)
library(zoo)

# TODO: audit field defaults
# TODO: use groups
# TODO: add dummies instead of missing
# fill in notes fields to avoid losing data

setwd("~/CPE")
source("mdb.get_patch.R")
source("utilities.R")
source("private/credentials.R")
source("set_up.R")
source("contacts.R")
source("campaigns.R")
source("tags.R")
source("relationships.R")
source("groups.R")
source("events.R")
source("contact_info.R")
source("contributions.R")
source("upload.R")
