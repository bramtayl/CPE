# WARNING: DO NOT RUN UNLESS YOU ARE VERY SURE YOU KNOW WHAT YOU ARE DOING
# WILL DELETE ALL CIVICRM DATA
dbSendStatement(database, "SET FOREIGN_KEY_CHECKS=0;")
for (table in dbListTables(database))
  if (stri_detect_regex(table, "^civicrm"))
    dbSendStatement(database, paste0("DROP TABLE ", table, ";"))
dbSendStatement(database, "SET FOREIGN_KEY_CHECKS=1;")
# DELETE the contents of /wp-content/uploads/civicrm IMMEDIATELY AFTERWARDS
