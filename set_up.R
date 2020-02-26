database = dbConnect(drv = MariaDB(), username = USERNAME, password = PASSWORD, host = "populareconomics.org", dbname = DATABASE)
# for a toy version of civicrm on localhost
# database = dbConnect(drv = MariaDB(), username = "wordpress", password = LOCAL_PASSWORD, host = "localhost", dbname = "wordpress")

# ignore purchase information
# ignore request information
# ignore report list
# ignore various mailing lists (assuming all can be recalculated if necessary)
tables = mdb.get("private/CPE.mdb", stringsAsFactors = FALSE, na.strings = "")

# snake case table names and column names
names(tables) = to_snake_case(names(tables))
for (name in names(tables)) {
  names(tables[[name]]) = to_snake_case(names(tables[[name]]))
}

# this is a bunch of miscellaneous lookup tables
option_value = 
  dbReadTable(database, "civicrm_option_value") %>%
  left_join(
    dbReadTable(database, "civicrm_option_group") %>%
      select(option_group_name = name, option_group_id = id)
  ) %>%
  select(option_group_name, name, id = value) %>%
  arrange(option_group_name, id)