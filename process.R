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

setwd("~/Dropbox/CPE")

database = dbConnect(
  drv = MariaDB(),
  username = "wordpress",
  password = "YOUR_PASSWORD_HERE",
  host = "localhost",
  dbname = "wordpress"
)

tables = mdb.get("CPE.mdb", stringsAsFactors = FALSE, na.strings = "")

# snake case table names and column names
names(tables) = to_snake_case(names(tables))
for (name in names(tables)) {
  names(tables[[name]]) = to_snake_case(names(tables[[name]]))
}

# ignore purchase information
# ignore request information
# ignore report list
# ignore various mailing lists (assuming all can be recalculated if necessary)

# this just splits names at the last space: good enough?
name_separator = " (?=[^ ]+$)"

my_coalesce = function(vector_1, vector_2)
  ifelse(is.na(vector_1), vector_2, vector_1)

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
    birth_date = mdy_hms(birth_date),
    contact_type = "Individual",
    created_date = mdy_hms(created_date),
    do_not_phone = as.logical(do_not_phone),
    # revese the logic
    do_not_trade = !as.logical(trade),
    is_deleted = as.logical(is_deleted),
    modified_date = mdy_hms(modified_date)
  ) %>%
  select(-trade) %>%
  separate(partner, c("partner_first_name", "partner_last_name"), sep = name_separator, fill = "right")

# I gave up on trying on deduplication, instead, all potential contacts got a new entry
# You will have to deduplicate by hand. I'm sorry.

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
    contact_type = "Individual",
    source = "contacts"
  )

participants_2017 = 
  read_csv("participants_2017.csv") %>%
  mutate(
    contact_id = 1:n() + max(contacts$contact_id),
    contact_type = "Individual",
    event_title = "2017 Summer Institute",
    source = "participants 2017"
  ) %>%
  left_join(
    tibble(
      # UNSURE: is "support staff" meaningful?
      # UNSURE: is volunteer good enough for babysitter, or do i need to make a new category?
      role = c("Participant", "Participant & support staff", "Teacher", "Babysitter"),
      participant_role_name = c("Attendee", "Attendee", "Speaker", "Volunteer")
    )
  ) %>%
  select(-role)

participants_2018 = 
  read_csv("participants_2018.csv") %>%
  separate(name, c("first_name", "last_name"), sep = name_separator, fill = "right") %>%
  mutate(
    contact_id = 1:n() + max(participants_2017$contact_id),
    contact_type = "Individual",
    event_title = "2018 Summer Institute",
    participant_role_name = "Attendee",
    source = "participants 2018"
  )

participants_2019 = 
  read_csv("participants_2019.csv") %>%
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
  read_csv("new_donations.csv") %>%
  separate(name, c("full_name", "partner"), sep = " & ", fill = "right") %>%
  separate(full_name, c("first_name", "last_name"), sep = name_separator, fill = "right") %>%
  separate(partner, c("partner_first_name", "partner_last_name"), sep = name_separator, fill = "right") %>%
  mutate(
    contact_id = 1:n() + max(participants_2019$contact_id),
    contact_type = "Individual",
    source = "new donations"
  )

cpe_members = 
  tables$cpe_members %>%
  rename(
    contact_id = individual_id,
    membership_status_type_id = status,
    organization_name = workplace,
    city = wcity,
    phone = wphone,
    postal_code = wzip,
    state_province_abbreviation = wstate,
    street_address = waddress
  )

# I accidentally deleted the modified date from the mail chimp and rise up csvs
# I can add this info back in if you would like it
# There's one big mailing list on mailchimp and a bunch of small ones
# I just used the big one
# I ignored the "deleted" and "cleaned" (i.e. email bouncing) mailchimp files

# the mail chimp and rise up lists all get their own membership types
mailchimp = 
  read.csv("mailchimp.csv", stringsAsFactors = FALSE, check.names = FALSE, na.strings = "") %>%
  mutate(
    contact_id = 1:n() + max(new_donations_info$contact_id),
    contact_type = "Individual",
    membership_type_name = "Mailchimp",
    source = "Mailchimp"
  )

rise_up_community = 
  read_csv("cpe_community.csv") %>%
  separate(name, c("first_name", "last_name"), sep = name_separator, fill = "right") %>%
  mutate(
    contact_id = 1:n() + max(mailchimp$contact_id),
    contact_type = "Individual",
    # status is always "bounced" if it exists
    is_deleted = !is.na(status),
    membership_type_name = "Rise Up community",
    source = "Rise Up community"
  ) %>%
  select(-status)

rise_up_members = 
  read_csv("cpe_members.csv") %>%
  separate(name, c("first_name", "last_name"), sep = name_separator, fill = "right") %>%
  mutate(
    contact_id = 1:n() + max(rise_up_community$contact_id),
    contact_type = "Individual",
    # status is always "bounced" if it exists
    is_deleted = !is.na(status),
    membership_type_name = "Rise Up member",
    source = "Rise Up member"
  ) %>%
  select(-status)

rise_up_local = 
  read_csv("cpe_local.csv") %>%
  separate(name, c("first_name", "last_name"), sep = name_separator, fill = "right") %>%
  mutate(
    contact_id = 1:n() + max(rise_up_members$contact_id),
    contact_type = "Individual",
    # status is always "bounced" if it exists
    is_deleted = !is.na(status),
    membership_type_name = "Rise Up local",
    source = "Rise Up local"
  ) %>%
  select(-status)

partners = 
  bind_rows(
    individuals %>%
      select(partner_id = id, first_name = partner_first_name, last_name = partner_last_name),
    new_donations_info %>%
      select(partner_id = contact_id, first_name = partner_first_name, last_name = partner_last_name)
  ) %>%
  # remove all na rows
  gather(variable, value, -partner_id, na.rm = TRUE) %>%
  spread(variable, value) %>%
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
    created_date = mdy_hms(created_date),
    is_deleted = as.logical(is_deleted),
    modified_date = mdy_hms(modified_date)
  )

# I didn't deduplicate the employers either (sorry)
employers = 
  bind_rows(
    cpe_members %>%
      rename(employee_id = contact_id) %>%
      select(-membership_status_type_id),
    participants_2017 %>%
      select(employee_id = contact_id, organization_name = employer_organization_name),
    participants_2018 %>%
      select(employee_id = contact_id, organization_name = employer_organization_name),
    participants_2019 %>%
      select(employee_id = contact_id, organization_name = employer_organization_name),
    mailchimp %>%
      select(employee_id = contact_id, organization_name = employer_organization_name)
  ) %>%
  gather(variable, value, -employee_id, na.rm = TRUE) %>%
  spread(variable, value) %>%
  mutate(
    contact_id = 1:n() + max(organizations$contact_id),
    contact_type = "Organization",
    source = "employers"
  )

contact_info = 
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
      select(-employer_organization_name, -membership_type_name), 
    rise_up_community %>%
      rename(id = contact_id) %>%
      select(-membership_type_name), 
    rise_up_members %>%
      rename(id = contact_id) %>%
      select(-membership_type_name), 
    rise_up_local %>%
      rename(id = contact_id) %>%
      select(-membership_type_name),
    partners %>%
      rename(id = contact_id), 
    organizations %>%
      rename(id = contact_id),
    employers %>%
      rename(id = contact_id)
  ) %>%
  # fill in false for deleted if not specified
  mutate(is_deleted = my_coalesce(is_deleted, FALSE))

campaign = 
  tables$development_campaigns %>%
  rename(
    id = development_project_id,
    start_date = begin_date
  ) %>%
  mutate(
    end_date = mdy_hms(end_date),
    start_date = mdy_hms(start_date)
  )

civicrm_campaign = 
  bind_rows(
    campaign,
    # create new campaigns for those referenced in the new donations table
    new_donations_info %>%
      select(name = campaign_name) %>%
      filter(!is.na(name)) %>%
      distinct %>%
      anti_join(campaign) %>%
      mutate(id = max(campaign$id) + 1:n())
  )

event_address =
  tables$institutes_workshops_talks %>%
  rename(
    description = event_description,
    id = event_id,
    street_address = location,
    title = event_name
  ) %>%
  mutate(
    end_date = mdy_hms(end_date),
    start_date = mdy_hms(start_date),
    # inexact addresses for events
    street_address = 
      ifelse(is.na(street_address), NA, paste("Near", street_address))
  )

old_financial_type = 
  dbReadTable(database, "civicrm_financial_type") %>%
  select(id, name)

# add two new financial types
new_financial_type = 
  tibble(name = c("In Kind", "Grant")) %>%
  mutate(id = max(old_financial_type$id) + 1:n())

civicrm_financial_type = bind_rows(old_financial_type, new_financial_type)

civicrm_tag =
  tables$category_codes %>%
  rename(
    id = category_id,
    name = description
  )

committees =
  tables$committees %>%
  rename(
    id = committee_id,
    name = committee_name
  )

membership_status_type = 
  tables$cpe_member_status_codes %>%
  rename(
    membership_status_type_id = status_code
  ) %>%
  left_join(
    tibble(
      # these descriptions combine two separate ideas: membership type and status
      # split out into two separate columns
      # there could be potentially inactive or resigned staff or advisors
      description = c("Advisory", "Inactive", "Resigned", "Staff"),
      membership_status_name = c("Current", "Expired", "Expired", "Current"),
      membership_type_name = c("Advisor", "Member", "Member", "Staff")
    )
  ) %>%
  select(-description)

# this is a bunch of miscellaneous lookup tables
option_value = 
  dbReadTable(database, "civicrm_option_value") %>%
  left_join(
    dbReadTable(database, "civicrm_option_group") %>%
      select(option_group_name = name, option_group_id = id)
  )

civicrm_contact = 
  contact_info %>%
  select(
    # address 
    -street_address, -supplemental_address_1, -city, -postal_code, -state_province_abbreviation, -country_name, 
    # email 
    -email, 
    # old id
    -employer_organization_id, -organization_id,
    # notes
    -contact, -narrative,
    # phone
    -cell, -fax, -phone, -wphone, -phone_ext,
    # relationships
    -employer_organization_id, -partner_id, -employee_id,
    # tags
    -tag_id_1, -tag_id_2, -tag_id_3,
    # website 
    -url
  ) %>%
  # truncate long organization names to fit into the database
  mutate(
    organization_name = stri_sub(organization_name, to = 128)
  )

# a location block is basically an address for events
location_block = 
  event_address %>%
  select(street_address) %>%
  filter(!is.na(street_address)) %>%
  distinct %>%
  mutate(
    # create new location block ids
    id = 1:n(),
    # with identical addreess ids
    address_id = id
  )

civicrm_loc_block =
  location_block %>%
  select(-street_address)

civicrm_address = 
  bind_rows(
    # locations from events
    location_block %>%
      select(-id) %>%
      rename(id = address_id),
    # locations from contacts
    contact_info %>%
      select(
        contact_id = id, street_address, supplemental_address_1, 
        city, postal_code, state_province_abbreviation, country_name
      )
  ) %>%
  mutate(
    id = coalesce(id, 1:n()),
    # truncate long street addresses to fit into othe database (there's only one)
    street_address = stri_sub(street_address, to = 96)
  ) %>%
  # remove empty addresses
  gather(variable, value, -id, -contact_id, na.rm = TRUE) %>%
  spread(variable, value) %>%
  mutate(
    country_name = 
      # United we fall
      ifelse(country_name == "USA", "United States", 
             ifelse(country_name == "England", "United Kingdom", country_name)
      )
  ) %>%
  # replace country names with ids
  # this will discard any countries not in the civicrm database
  left_join(
    dbReadTable(database, "civicrm_country") %>%
      select(
        country_name = name,
        country_id = id
      )
  ) %>%
  select(-country_name) %>%
  # replace state abbreviations with ids
  # this will discard any state abbreviations not in the civicrm database
  left_join(
    dbReadTable(database, "civicrm_state_province") %>%
      select(
        country_id,
        state_province_abbreviation = abbreviation,
        state_province_id = id
      )
  ) %>%
  select(-state_province_abbreviation)

civicrm_email = 
  contact_info %>%
  select(
    contact_id = id, email
  ) %>%
  filter(!is.na(email))

civicrm_event = 
  event_address %>%
  # replace addresses with location block ids
  left_join(
    location_block %>% 
      select(loc_block_id = id, street_address)
  ) %>%
  select(-street_address)

# pull out tags from contact info in long form
civicrm_entity_tag = 
  contact_info %>%
  select(entity_id = id, tag_id_1, tag_id_2, tag_id_3) %>%
  gather("rank", "tag_id", tag_id_1, tag_id_2, tag_id_3, na.rm = TRUE) %>%
  # I assume the order doesn't mean anything?
  select(-rank) %>%
  mutate(entity_table = "civicrm_contact") %>%
  # if there is no matching tag, just forget it
  # this will discard tags with no matching id
  semi_join(civicrm_tag %>% select(tag_id = id))

new_members =
  bind_rows(
    mailchimp %>%
      select(contact_id, membership_type_name), 
    rise_up_community %>%
      select(contact_id, membership_type_name), 
    rise_up_members %>%
      select(contact_id, membership_type_name), 
    rise_up_local %>%
      select(contact_id, membership_type_name)
  )

civicrm_membership_type = 
  bind_rows(
    # create new membership types for non-committees
    # committees are their own membership type
    membership_status_type %>%
      select(name = membership_type_name),
    new_members %>%
      select(name = membership_type_name)
  ) %>%
  distinct %>%
  mutate(
    id = max(committees$id) + 1:n()
  ) %>%
  bind_rows(committees, .) %>%
  # what is a domain?
  mutate(domain_name = "Default Domain Name") %>%
  left_join(
    dbReadTable(database, "civicrm_domain") %>%
      select(domain_id = id, domain_name = name)
  ) %>%
  select(-domain_name) %>%
  # replace organization name with id
  # the default organization is CPE (and should be renamed as such)
  mutate(organization_name = "Default Organization") %>%
  left_join(
    dbReadTable(database, "civicrm_contact") %>%
      select(member_of_contact_id = id, organization_name)
  ) %>%
  select(-organization_name) %>%
  # replace financial type name with id. doesn't mean anything because none of the memberships require payment yet
  mutate(financial_type_name = "Donation") %>%
  left_join(
    civicrm_financial_type %>%
      select(financial_type_name = name, financial_type_id = id)
  ) %>%
  select(-financial_type_name)

civicrm_note = 
  contact_info %>%
  # what's the difference between contact and narrative?
  select(contact_id = id, contact, narrative) %>%
  # put notes in long form
  gather("note_type", "note", -contact_id, na.rm = TRUE) %>%
  select(-note_type) %>%
  mutate(
    entity_id = contact_id,
    entity_table = "civicrm_contact"
  )

attendance =
  tables$attendance %>%
  select(-student_class_id) %>%
  rename(
    contact_id = attendee_id
  ) %>%
  mutate(
    participant_role_name = "Attendee"
  )

teachers = 
  tables$teachers %>%
  rename(
    contact_id = teacher_id
  ) %>%
  mutate(
    participant_role_name = "Speaker"
  )

new_participants = 
  bind_rows(
    participants_2017 %>%
      select(event_title, participant_role_name, contact_id),
    participants_2019 %>%
      select(event_title, participant_role_name, contact_id),
    participants_2019 %>%
      select(event_title, participant_role_name, contact_id, register_date)
  ) %>%
  # replace event titles with ids
  left_join(
    civicrm_event %>% 
      select(event_title = title, event_id = id)
  ) %>%
  select(-event_title)
  
# put together attendees and speakers
civicrm_participant = 
  bind_rows(attendance, teachers, new_participants) %>%
  # replace participant role names with ids
  left_join(
    option_value %>%
      filter(option_group_name == "participant_role") %>%
      select(participant_role_name = name, role_id = value)
  ) %>%
  select(-participant_role_name) %>%
  # discard if the contact doesn't exist
  semi_join(contact_info %>% select(contact_id = id)) %>%
  # discard if the event doesn't exist
  semi_join(civicrm_event %>% select(event_id = id))

civicrm_phone =
  contact_info %>%
  select(
    contact_id = id, cell, phone, wphone, fax, phone_ext
  ) %>%
  gather("phone_type_location_type_name", "phone", cell, fax, phone, wphone, na.rm = TRUE) %>%
  left_join(
    # phone_type_location_type_name contains both phone type and location type information
    tibble(
      phone_type_location_type_name = c("phone", "wphone", "cell", "fax"),
      location_type_name = c("Home", "Work", "Home", "Work"),
      phone_type_name = c("Phone", "Phone", "Mobile", "Fax")
    ) %>%
      # replace phone type names with ids
      left_join(
        option_value %>%
          filter(option_group_name == "phone_type") %>%
          select(phone_type_name = name, phone_type_id = value)
      ) %>%
      select(-phone_type_name) %>%
      # replace location type names with ids
      left_join(
        dbReadTable(database, "civicrm_location_type") %>%
          select(location_type_id = id, location_type_name = name)
      ) %>%
      select(-location_type_name)
  ) %>%
  select(-phone_type_location_type_name)

civicrm_website = 
  contact_info %>%
  select(
    contact_id = id, url
  ) %>%
  filter(!is.na(url))

# use to replace old organization ids with new contact ids
organization_ids = 
  contact_info %>%
  select(id, organization_id) %>%
  filter(!is.na(organization_id))

civicrm_membership = 
  bind_rows(
    cpe_members %>%
      select(contact_id, membership_status_type_id) %>%
      # split up membership status and membership type
      left_join(membership_status_type) %>%
      select(-membership_status_type_id) %>%
      # replace membership type names with ids
      left_join(
        civicrm_membership_type %>%
          select(
            membership_type_id = id,
            membership_type_name = name
          )
      ) %>%
      select(-membership_type_name),
    tables$committee_members %>%
      select(-committee_member_id) %>%
      rename(
        membership_type_id = committee_id,
        contact_id = member_id,
        start_date = join_date,
        end_date = quit_date
      ) %>%
      mutate(
        end_date = mdy_hms(end_date),
        start_date = mdy_hms(start_date)
      ),
    new_members %>%
      left_join(
        civicrm_membership_type %>%
          select(
            membership_type_id = id,
            membership_type_name = name
          )
      ) %>%
      select(-membership_type_name)
  ) %>%
  # fill in active if there is no membership status
  mutate(membership_status_name = my_coalesce(membership_status_name, "Current")) %>%
  # replace status names with ids
  left_join(
    dbReadTable(database, "civicrm_membership_status") %>%
      select(status_id = id, membership_status_name = name)
  ) %>%
  select(-membership_status_name) %>%
  # discard if the contacts don't exist
  semi_join(contact_info %>% select(contact_id = id)) %>%
  # discard if the membership type doesn't exist
  semi_join(civicrm_membership_type %>% select(membership_type_id = id))

contact_relationship = 
  contact_info %>%
  select(contact_id = id, employer_organization_id, partner_id, employee_id) %>%
  # replace employer organization ids with contact ids
  # will discard organization ids if they don't exist
  left_join(
    organization_ids %>%
      select(employer_id = id, employer_organization_id = organization_id)
  ) %>%
  select(-employer_organization_id)

contribution_donors = 
  tables$donations %>%
  # these columns seem potentially useful, but i'm not sure how to fit them into the table
  select(-pledge_id, -pledge, -request) %>%
  rename(
    campaign_id = campaign,
    contact_id_1 = donor_id,
    contact_id_2 = donor_id_2,
    contact_id_3 = donor_id_3,
    receive_date = date,
    total_amount = amount
  ) %>%
  mutate(
    # use grant and in kind to create financial type
    # potentially could be in kind and a grant?
    financial_type_name = 
      ifelse(as.logical(grant), "Grant", 
             ifelse(as.logical(inkind), "In Kind", "Donation")
      ),
    receive_date = mdy_hms(receive_date),
    # just use a thank you date of today
    # ifelse removes the timestamp class, so I have to add it back in again (sigh)
    thankyou_date = as.POSIXct(ifelse(letter, Sys.time(), NA), origin = "1970-01-01"),
    # fill in 0 if the amount is missing
    total_amount = my_coalesce(total_amount, 0)
  ) %>%
  select(-grant, -inkind, -letter) %>%
  bind_rows(
    tables$grants %>%
      # ignoring projects
      # are these grants already in the donations table?
      # what units is duration in? how to fit it into the table?
      select(-grant_id, -project_id, -duration) %>%
      rename(
        contact_id_1 = individual_id,
        total_amount = amount,
        receive_date = dategiven,
        revenue_recognition_date = report 
      ) %>%
      mutate(
        financial_type_name = "Grant",
        receive_date = mdy_hms(receive_date),
        revenue_recognition_date = mdy_hms(revenue_recognition_date),
        total_amount = my_coalesce(total_amount, 0)
      )
  ) %>%
  bind_rows(
    new_donations_info %>%
      # only used marked new downations
      filter(!is.na(new_donation)) %>%
      select(contact_id_1 = contact_id, receive_date, source, total_amount, campaign_name) %>%
      mutate(
        financial_type_name = "Donation",
        # convert date to datetime
        receive_date = as.POSIXct(mdy(receive_date))
      ) %>%
      # replace campaign names with ids
      left_join(
        civicrm_campaign %>%
          select(
            campaign_name = name,
            campaign_id = id
          )
      ) %>%
      select(-campaign_name)
  ) %>%
  # replace financial type names with ids
  left_join(
    civicrm_financial_type %>%
      select(
        financial_type_name = name, 
        financial_type_id = id
      )
  ) %>%
  select(-financial_type_name) %>%
  # replace organization ids with contact ids
  left_join(
    organization_ids %>%
      rename(contact_id_4 = id)
  ) %>%
  select(-organization_id)

civicrm_relationship = 
  contact_relationship %>%
  left_join(
    # replace employer ids with employee ids
    contact_relationship %>%
      filter(!is.na(employer_id)) %>%
      select(contact_id = employer_id, new_employee_id = contact_id)
  ) %>%
  select(-employer_id) %>%
  # fill in old employer ids
  # will overwrite the employee ID's from the CPE table
  mutate(employee_id = my_coalesce(new_employee_id, employee_id)) %>%
  select(-new_employee_id) %>%
  # put relationships in long form
  rename(
    contact_id_a = contact_id, 
    `Employee of` = employee_id,
    `Partner of` = partner_id
  ) %>%
  gather("relationship_type_name_a_b", "contact_id_b", -contact_id_a, na.rm = TRUE) %>%
  # replace relationship type names with ids
  left_join(
    dbReadTable(database, "civicrm_relationship_type") %>%
      select(
        relationship_type_id = id,
        relationship_type_name_a_b = name_a_b
      )
  ) %>%
  select(-relationship_type_name_a_b) %>%
  # discard if contact a or b doesn't exist
  semi_join(contact_info %>% select(contact_id_a = id)) %>%
  semi_join(contact_info %>% select(contact_id_b = id))

contribution = 
  contribution_donors %>%
  select(
    donation_id, 
    contact_id_1, contact_id_2, contact_id_3, contact_id_4
  ) %>%
  # put contact ids for donations in long form
  gather(
    "rank", "contact_id", 
    contact_id_1, contact_id_2, contact_id_3, contact_id_4,
    na.rm = TRUE
  ) %>%
  group_by(donation_id) %>%
  # use the first contact listed as the donor (Civicrm only allows 1 donor)
  top_n(-1, rank) %>%
  select(-rank) %>%
  left_join(contribution_donors, .) %>%
  select(-donation_id, -contact_id_1, -contact_id_2, -contact_id_3, -contact_id_4)

# assign all contactless contributions to an arbitrary anonymous contact
# there probably should just be one contact id for anonymous donations
contribution_anonymous = 
  bind_rows(
    contribution %>%
      anti_join(contact_info %>% select(contact_id = id)) %>%
      mutate(
        contact_id = 
          contact_info %>%
          filter(first_name == "Anonymous") %>%
          .$id %>%
          first
      ),
    contribution %>%
      semi_join(contact_info %>% select(contact_id = id))
  )

civicrm_contribution = 
  bind_rows(
    contribution_anonymous %>%
      # fill in NA if the campaign doesn't exist
      anti_join(civicrm_campaign %>% select(campaign_id = id)) %>%
      mutate(campaign_id = NA),
    contribution_anonymous %>%
      semi_join(civicrm_campaign %>% select(campaign_id = id))
  )

pledges = 
  tables$pledges %>%
  select(-pledge_id) %>%
  rename(
    contact_id = pledger_id,
    create_date = pledge_date,
    start_date = donation_date
  ) %>%
  mutate(
    create_date = mdy_hms(create_date),
    original_installment_amount = amount,
    pledge_status_name = 
      # can't be cancelled and completed
      ifelse(done, "Completed", ifelse(cancel, "Cancelled", "Pending")),
    start_date = 
      # ifelse removes the timestamp class, so I have to add it back in again (sigh)
      as.POSIXct(my_coalesce(mdy_hms(start_date), Sys.time()), origin = "1970-01-01")
  ) %>%
  select(-done, -cancel) %>%
  # replace pledge status names with ids
  left_join(
    option_value %>%
      filter(option_group_name == "pledge_status") %>%
      select(pledge_status_name = name, status_id = value)
  ) %>%
  select(-pledge_status_name) %>%
  # discard if contact doesn't exist
  semi_join(civicrm_contact %>% select(contact_id = id))

civicrm_pledge = 
  bind_rows(
   pledges %>%
      # fill in NA if the campaign doesn't exist
      anti_join(civicrm_campaign %>% select(campaign_id = id)) %>%
      mutate(campaign_id = NA),
   pledges %>%
      semi_join(civicrm_campaign %>% select(campaign_id = id))
  )

dbAppendTable(database, "civicrm_campaign", civicrm_campaign)
dbAppendTable(database, "civicrm_financial_type", new_financial_type)
# replace old tags
dbSendQuery(database, "DELETE FROM civicrm_tag")
dbAppendTable(database, "civicrm_tag", civicrm_tag)

dbAppendTable(database, "civicrm_contact", civicrm_contact)

dbAppendTable(database, "civicrm_address", civicrm_address)
dbAppendTable(database, "civicrm_loc_block", civicrm_loc_block)
dbAppendTable(database, "civicrm_email", civicrm_email)

dbAppendTable(database, "civicrm_event", civicrm_event)

dbAppendTable(database, "civicrm_entity_tag", civicrm_entity_tag)
dbAppendTable(database, "civicrm_membership_type", civicrm_membership_type)

dbAppendTable(database, "civicrm_note", civicrm_note)
dbAppendTable(database, "civicrm_participant", civicrm_participant)

dbAppendTable(database, "civicrm_phone", civicrm_phone)
dbAppendTable(database, "civicrm_website", civicrm_website)
dbAppendTable(database, "civicrm_membership", civicrm_membership)

dbAppendTable(database, "civicrm_relationship", civicrm_relationship)
dbAppendTable(database, "civicrm_pledge", civicrm_pledge)
dbAppendTable(database, "civicrm_contribution", civicrm_contribution)
