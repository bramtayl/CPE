# enable USPS standardization first
# note: this takes a LONG TIME
addresses = dbReadTable(database, "civicrm_address")

update_address = function(id) {
  print(id)
  POST(
    paste0(DOMAIN, "/wp-content/plugins/civicrm/civicrm/extern/rest.php"),
    body = list(
      entity = "Address",
      action = "update",
      api_key = API_KEY,
      key = SITE_KEY,
      id = id
    )
  )
}

walk(
  addresses$id,
  function(id) {
    tryCatch(update_address(id), error = function(err) update_address(id))
  }
)
