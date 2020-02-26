civicrm_note = 
  contact_info %>%
  # what's the difference between contact and narrative?
  select(contact_id = id, contact, narrative, modified_date) %>%
  # put notes in long form
  gather("subject", "note", -contact_id, -modified_date, na.rm = TRUE) %>%
  mutate(
    entity_id = contact_id,
    entity_table = "civicrm_contact",
    privacy = "None"
  )

civicrm_phone =
  contact_info %>%
  select(
    contact_id = id, cell, phone, wphone, fax, phone_ext
  ) %>%
  gather("phone_type_location_type_name", "phone", cell, fax, phone, wphone, na.rm = TRUE) %>%
  strict_join(
    # phone_type_location_type_name contains both phone type and location type information
    tibble(
      phone_type_location_type_name = c("phone", "wphone", "cell", "fax"),
      location_type_name = c("Home", "Work", "Home", "Work"),
      phone_type_name = c("Phone", "Phone", "Mobile", "Fax")
    ) %>%
      code_option(option_value, "phone_type") %>%
      code(dbReadTable(database, "civicrm_location_type"), "location_type"),
    "phone"
  ) %>%
  select(-phone_type_location_type_name) %>%
  mutate(
    is_primary = TRUE,
    is_billing = TRUE
  )

civicrm_website = 
  contact_info %>%
  select(
    contact_id = id, url
  ) %>%
  filter(!is.na(url)) %>%
  mutate(
    website_type_name = "Main"
  ) %>%
  code_option(option_value, "website_type")

civicrm_email = 
  contact_info %>%
  select(
    contact_id = id, email
  ) %>%
  filter(!is.na(email)) %>%
  mutate(
    is_billing = TRUE,
    is_bulkmail = TRUE,
    is_primary = TRUE,
    location_type_name = "Home"
  ) %>%
  code(dbReadTable(database, "civicrm_location_type"), "location_type")

countries = 
  dbReadTable(database, "civicrm_country") %>%
  mutate(name = stri_trans_toupper(name))

state_province = 
  dbReadTable(database, "civicrm_state_province") %>%
  mutate(name = stri_trans_toupper(name))

address = 
  bind_rows(
    # locations from events
    location_block %>%
      select(-id) %>%
      rename(id = address_id),
    # locations from contacts
    contact_info %>%
      select(
        contact_id = id, street_address, supplemental_address_1, 
        city, postal_code, state_province_abbreviation, country_name, fill_USA
      )
  ) %>%
  mutate(
    id = coalesce(id, 1:n()),
    # truncate long street addresses to fit into othe database (there's only one)
    street_address = stri_sub(street_address, to = 96)
  ) %>%
  filter(!is.na(street_address)) %>%
  mutate(
    is_billing = TRUE,
    is_primary = TRUE
  ) %>%
  # try joining countries both by name and abbreviation
  left_join(countries %>% select(country_name = name, country_id = id)) %>%
  left_join(countries %>% select(country_name = iso_code, country_id_2 = id)) %>%
  mutate(country_id = coalesce(country_id, country_id_2)) %>%
  select(-country_id_2) %>%
  # try joining states both by name and abbreviation
  left_join(
    state_province %>%
      select(
        country_id,
        state_province_abbreviation = abbreviation,
        state_province_id = id
      )
  ) %>%
  left_join(
    state_province %>%
      select(
        country_id,
        state_province_abbreviation = name,
        state_province_id_2 = id
      )
  ) %>%
  mutate(state_province_id = coalesce(state_province_id, state_province_id_2)) %>%
  select(-state_province_id_2)

# write out non matched values so they don't get lost
to_fix = 
  address %>%
  filter(
    (!is.na(country_name) & is.na(country_id)) | 
      (!is.na(state_province_abbreviation) & is.na(state_province_id))
  ) %>%
  select(contact_id, country_name, state_province_abbreviation, fill_USA)

write_csv(to_fix, "to_fix.csv", na = "")

civicrm_address = 
  address %>% 
  select(-state_province_abbreviation, -country_name, -fill_USA)
