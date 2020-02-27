# all potential contacts got a separate entry
# deduplication by hand is required

individuals = 
  tables$individuals %>%
  select(-brochure, -check_back, -d_100, 
         -d_50, -fall_d_mail, -fall_newsletter, 
         -group, -idcode, -interested, -lastattend, -lastgave, 
         -prospect, -spring_d_mail, -spring_newsletter, -type) %>%
  rename(
    birth_date = birthday,
    country_name = country,
    created_date = date_enter,
    do_not_phone = no_call,
    first_name = first,
    formal_title = title,
    id = individual_id,
    is_deleted = deleted,
    employer_organization_id = organization_id,
    last_name = last,
    # coa stands for change of address
    modified_date = coa,
    postal_code = zip,
    state_province_abbreviation = state,
    street_address = address_2,
    supplemental_address_1 = address_1,
    tag_id_1 = cat_1,
    tag_id_2 = cat_2,
    tag_id_3 = cat_3
  ) %>%
  mutate(
    # note: R interprets dates as occuring at midnight UTC
    # which is a couple of hours earlier in EST
    created_date = as.POSIXct(created_date),
    contact_type = "Individual",
    do_not_phone = as.logical(do_not_phone),
    # revese the logic
    do_not_trade = !as.logical(trade),
    is_deleted = as.logical(is_deleted),
    modified_date = as.POSIXct(modified_date)
  ) %>%
  select(-trade) %>%
  separate(partner, c("partner_first_name", "partner_last_name"), 
           sep = name_separator, 
           fill = "right"
  )

# I'm not sure what the difference between a contact and an individual is
contacts = 
  tables$contacts %>%
  select(-contact_id, -entry_id) %>%
  rename(
    employer_organization_id = organization_id,
    first_name = contact_first, 
    job_title = contact_title,
    last_name = contact_last,
    phone_ext = ext
  ) %>%
  mutate(
    contact_id = 1:n() + max(individuals$id),
    contact_type = "Individual"
  )

participants_2017 = 
  read_csv("private/participants_2017.csv") %>%
  mutate(
    contact_id = 1:n() + max(contacts$contact_id),
    contact_type = "Individual",
    event_title = "2017 Summer Institute",
    postal_code = as.character(postal_code),
    source = "participants 2017"
  ) %>%
  left_join(
    tibble(
      # hopefully, this napping is close enough
      role = c("Participant", "Participant & support staff", "Teacher", "Babysitter"),
      participant_role_name = c("Attendee", "Attendee", "Speaker", "Volunteer")
    )
  ) %>%
  select(-role)

participants_2018 = 
  read_csv("private/participants_2018.csv") %>%
  separate(name, c("first_name", "last_name"), sep = name_separator, fill = "right") %>%
  mutate(
    contact_id = 1:n() + max(participants_2017$contact_id),
    contact_type = "Individual",
    event_title = "2018 Summer Institute",
    participant_role_name = "Attendee",
    source = "participants 2018"
  )

participants_2019 = 
  read_csv("private/participants_2019.csv") %>%
  separate(name, c("first_name", "last_name"), sep = name_separator, fill = "right") %>%
  mutate(
    contact_id = 1:n() + max(participants_2018$contact_id),
    contact_type = "Individual",
    event_title = "2019 Summer Institute",
    participant_role_name = "Attendee",
    register_date = mdy_hms(register_date),
    source = "participants 2019"
  )

new_donations_info = 
  read_csv("private/new_donations.csv") %>%
  mutate(name = stri_trans_totitle(name)) %>%
  separate(name, c("full_name", "partner"), sep = " & ", fill = "right") %>%
  separate(full_name, c("first_name", "last_name"), sep = name_separator, fill = "right") %>%
  separate(partner, c("partner_first_name", "partner_last_name"), sep = name_separator, fill = "right") %>%
  mutate(
    contact_id = 1:n() + max(participants_2019$contact_id),
    contact_type = "Individual",
    city = stri_trans_totitle(city),
    source = "new donations",
    street_address = stri_trans_totitle(street_address),
    supplemental_address_1 = stri_trans_totitle(supplemental_address_1)
  )

cpe_members = 
  tables$cpe_members %>%
  rename(
    contact_id = individual_id,
    status_code = status,
    organization_name = workplace,
    city = wcity,
    phone = wphone,
    postal_code = wzip,
    state_province_abbreviation = wstate,
    street_address = waddress
  ) %>%
  mutate(
    group_name = "Member"
  )

# the mail chimp and rise up lists all get their own membership types

# I ignored the "deleted" and "cleaned" (i.e. email bouncing) mailchimp files
mailchimp = 
  read_csv("private/mailchimp.csv") %>%
  mutate(
    contact_id = 1:n() + max(new_donations_info$contact_id),
    contact_type = "Individual",
    group_name = "Mailchimp",
    # keeps this potentially useful info in source
    source = paste("Mailchimp:", coalesce(coalesce(event, website_subscriber), ""))
  ) %>%
  # not sure what the ratings scores mean
  select(-event, -organization, -rating, -website_subscriber)

rise_up_community = 
  read_csv("private/cpe_community.csv") %>%
  mutate(name = stri_trim(name)) %>%
  mutate(name = ifelse(name == "", NA, name)) %>%
  separate(name, c("first_name", "last_name"), sep = name_separator, fill = "right") %>%
  mutate(
    contact_id = 1:n() + max(mailchimp$contact_id),
    contact_type = "Individual",
    created_date = as.POSIXct(dmy(created_date)),
    # status is always "bounced" if it exists
    is_deleted = !is.na(status),
    modified_date = as.POSIXct(dmy(modified_date)),
    group_name = "Rise Up community",
    source = "Rise Up community"
  ) %>%
  select(-status)

rise_up_members = 
  read_csv("private/cpe_members.csv") %>%
  mutate(name = stri_trim(name)) %>%
  mutate(name = ifelse(name == "", NA, name)) %>%
  separate(name, c("first_name", "last_name"), sep = name_separator, fill = "right") %>%
  mutate(
    contact_id = 1:n() + max(rise_up_community$contact_id),
    contact_type = "Individual",
    created_date = as.POSIXct(dmy(created_date)),
    # status is always "bounced" if it exists
    is_deleted = !is.na(status),
    modified_date = as.POSIXct(dmy(modified_date)),
    group_name = "Rise Up member",
    source = "Rise Up member"
  ) %>%
  select(-status)

rise_up_local = 
  read_csv("private/cpe_local.csv") %>%
  mutate(name = stri_trim(name)) %>%
  mutate(name = ifelse(name == "", NA, name)) %>%
  separate(name, c("first_name", "last_name"), sep = name_separator, fill = "right") %>%
  mutate(
    contact_id = 1:n() + max(rise_up_members$contact_id),
    contact_type = "Individual",
    created_date = as.POSIXct(dmy(created_date)),
    # status is always "bounced" if it exists
    is_deleted = !is.na(status),
    modified_date = as.POSIXct(dmy(modified_date)),
    group_name = "Rise Up local",
    source = "Rise Up local"
  ) %>%
  select(-status)

partners = 
  bind_rows(
    # get partners both from individuals and new_donations_info
    individuals %>%
      select(partner_id = id, first_name = partner_first_name, last_name = partner_last_name),
    new_donations_info %>%
      select(partner_id = contact_id, first_name = partner_first_name, last_name = partner_last_name)
  ) %>%
  filter(!(is.na(first_name) & is.na(last_name))) %>%
  mutate(
    contact_id = 1:n() + max(rise_up_local$contact_id),
    contact_type = "Individual",
    source = "partners"
  )

organizations = 
  tables$organizations %>%
  # what do these codes in these two columns mean?
  select(-code, -group) %>%
  rename(
    created_date = date_enter,
    country_name = country,
    modified_date = coa,
    organization_name = name,
    is_deleted = deleted,
    postal_code = zip,
    state_province_abbreviation = state,
    street_address = address_2,
    supplemental_address_1 = address_1,
    tag_id_1 = category,
    tag_id_2 = cat_2,
    tag_id_3 = cat_3,
    url = web
  ) %>%
  mutate(
    contact_id = 1:n() + max(partners$contact_id),
    contact_type = "Organization",
    created_date = as.POSIXct(created_date),
    is_deleted = as.logical(is_deleted),
    modified_date = as.POSIXct(modified_date)
  )

new_employers_employees = 
  bind_rows(
    # make a new contact entry for all new employers
    cpe_members %>%
      rename(employee_id = contact_id) %>%
      select(-status_code),
    participants_2017 %>%
      select(employee_id = contact_id, organization_name = employer_organization_name),
    participants_2018 %>%
      select(employee_id = contact_id, organization_name = employer_organization_name),
    participants_2019 %>%
      select(employee_id = contact_id, organization_name = employer_organization_name),
    mailchimp %>%
      select(employee_id = contact_id, organization_name = employer_organization_name)
  ) %>%
  filter(!(
    is.na(organization_name) & is.na(street_address) & is.na(city) & 
      is.na(state_province_abbreviation) & is.na(postal_code) & is.na(phone)
  ))

new_employers = 
  new_employers_employees %>%
  select(-employee_id, -group_name) %>%
  distinct %>%
  mutate(
    contact_id = 1:n() + max(organizations$contact_id),
    contact_type = "Organization",
    source = "employers"
  )

hidden_partners = 
  bind_rows(
    individuals %>%
      select(-partner_first_name, -partner_last_name), 
    contacts %>%
      rename(id = contact_id),
    participants_2017 %>%
      rename(id = contact_id) %>%
      select(-employer_organization_name, -event_title, -participant_role_name), 
    participants_2018 %>%
      rename(id = contact_id) %>%
      select(-employer_organization_name, -event_title, -participant_role_name), 
    participants_2019 %>%
      rename(id = contact_id) %>%
      select(-employer_organization_name, -event_title, -participant_role_name, -register_date),
    new_donations_info %>%
      rename(id = contact_id) %>%
      select(-campaign_name, -new_donation, -partner_first_name, -partner_last_name, -receive_date, -total_amount),
    mailchimp %>%
      rename(id = contact_id) %>%
      select(-employer_organization_name, -group_name),
    rise_up_community %>%
      rename(id = contact_id) %>%
      select(-group_name),
    rise_up_members %>%
      rename(id = contact_id) %>%
      select(-group_name),
    rise_up_local %>%
      rename(id = contact_id) %>%
      select(-group_name),
    partners %>%
      rename(id = contact_id),
    organizations %>%
      rename(id = contact_id),
    new_employers %>%
      rename(id = contact_id)
  )

contact_info = 
  # split out partners which are included via ampersand
  hidden_partners %>%
  filter(first_name %>% stri_detect_fixed("&")) %>%
  rename(new_partner_id = id) %>%
  mutate(partner_id = coalesce(new_partner_id, partner_id)) %>%
  select(-new_partner_id) %>%
  separate(first_name, c("partner_name", "first_name"), sep = "\\s?&\\s?") %>%
  select(-partner_name) %>%
  mutate(
    id = 1:n() + max(hidden_partners$id),
    contact_type = "Individual",
    source = "hidden partners"
  ) %>%
  select(first_name, last_name, partner_id, id, contact_type, source) %>%
  bind_rows(
    # add back in the original data
    hidden_partners %>%
      separate(first_name, c("first_name", "discard"), 
               sep = "\\s?&\\s?", fill = "right") %>%
      select(-discard)
  ) %>%
  mutate(
    # fill in United States if only a state was given
    fill_USA = !is.na(state_province_abbreviation) & is.na(country_name),
    # uppercase countries and states to avoid capitalization issues
    country_name = stri_trans_toupper(country_name),
    country_name = ifelse(fill_USA, "UNITED STATES", country_name),
    country_name = 
      # United we fall
      ifelse(country_name %in% c("USA", "U.S.A."), "UNITED STATES", 
             ifelse(country_name %in% c("ENGLAND", "UK"), "UNITED KINGDOM", country_name)
      ),
    # remove punctionation e.g. D.C. from states
    state_province_abbreviation = 
      state_province_abbreviation %>%
      stri_trans_toupper %>%
      stri_replace_all_regex("[^[:alpha:][:space:]]", ""),
    # handle missing data
    state_province_abbreviation = 
      ifelse(state_province_abbreviation %in% c("", "NA", "NO REGION"),
             NA, state_province_abbreviation),
    # fill in false for deleted if not specified
    is_deleted = coalesce(is_deleted, FALSE)
  ) %>%
  separate(first_name, c("first_name", "middle_name"), sep = name_separator, fill = "right")

civicrm_contact = 
  contact_info %>%
  mutate(
    # truncate long organization names to fit into the database
    organization_name = stri_sub(organization_name, to = 128),
    # we need a display and a sort name for everyone
    display_name = coalesce(
      ifelse(contact_type == "Individual",
             paste(coalesce(first_name, ""), coalesce(last_name, "")),
             organization_name
      ),
      email
    ),
    sort_name = display_name
  ) %>%
  select(
    # address 
    -street_address, -supplemental_address_1, -city, -postal_code, -state_province_abbreviation, -country_name, -fill_USA,
    # email 
    -email, 
    # old id
    -employer_organization_id, -organization_id,
    # notes
    -contact, -narrative,
    # phone
    -cell, -fax, -phone, -wphone, -phone_ext,
    # relationships
    -employer_organization_id, -partner_id,
    # tags
    -tag_id_1, -tag_id_2, -tag_id_3,
    # website 
    -url
  )

