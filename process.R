library(Hmisc)
library(dplyr)
library(tidyr)
library(lubridate)
library(snakecase)
library(purrr)
library(stringi)
library(DBI)
library(RMariaDB)
library(readr)

setwd("~/CPE")

# UNSURE: discard purchase information (hasn't been updated for a long time)
# UNSURE: discard request information (hasn't been updated for a long time)
# UNSURE: discare report informaation (hasn't been updated for a long time)
# UNSURE: discard various mailing lists (assuming all can be recalculated if necessary)

# UNSURE: this just splits names at the last space. Good enough?
name_separator = " (?=[^ ]+$)"

# if vector 1 is missing, use vector 2
my_coalesce = function(vector_1, vector_2)
  ifelse(is.na(vector_1), vector_2, vector_1)

new_participants = 
  bind_rows(
    read_csv("participants_2017.csv") %>%
      select(-Name) %>%
      rename(
        cell = `Cell phone`,
        city = City,
        email = `Email Address`,
        first_name = First,
        last_name = Last,
        organization_name = `Organization (if applicable)`,
        postal_code = Zip,
        role = Role,
        state_province_abbreviation = State,
        street_address = Address2
      ) %>%
      mutate(event_title = "2017 Summer Institute"),
    read_csv("participants_2018.csv") %>%
      rename(
        cell = `Phone  // Número telefónico`,
        city = `City/Town  // Ciudad o población`,
        country_name = `Country  // País`,
        email = `Email Address`,
        name = `Name // Nombre`,
        employer_organization_name = `Organizational Affiliation  // Pertenece a alguna organización?`,
        state_province_abbreviation = `State  // Estado`,
        street_address = `Street Address  // Dirección (calle y número)`,
        postal_code = `ZIP/Postal Code  // Código postal`
      ) %>%
      separate(name, c("first_name", "last_name"), sep = name_separator, fill = "right") %>%
      mutate(event_title = "2018 Summer Institute"),
    read_csv("participants_2019.csv") %>%
      rename(
        cell = `Phone  // Número telefónico`,
        city = `City/Town  // Ciudad o población`,
        country_name = `Country  // País`,
        email = `Email Address`,
        name = `Name // Nombre`,
        employer_organization_name = `Organizational Affiliation  // Pertenece a alguna organización?`,
        postal_code = `ZIP/Postal Code  // Código postal`,
        register_date = Timestamp,
        state_province_abbreviation = `State  // Estado`,
        street_address = `Street Address  // Dirección (calle y número)`
      ) %>%
      separate(name, c("first_name", "last_name"), sep = name_separator, fill = "right") %>%
      mutate(
        event_title = "2019 Summer Institute",
        register_date = mdy_hms(register_date)
      )
  ) %>%
  mutate(
    role = my_coalesce(role, "Participant")
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

new_donation_addresses = 
  read_csv("new_donations.csv") %>%
  rename(
    campaign_name = `Donation campaign`,
    new_donation = `Database: add this donation`,
    postal_code = zip,
    receive_date = `Donation Date`,
    source = `Payment method`,
    state_province_abbreviation = state,
    street_address = address2,
    supplemental_address_1 = address21,
    total_amount = `Donation Amt`
  ) %>%
  mutate(
    name = stri_trans_totitle(name)
  ) %>%
  separate(name, c("full_name", "partner"), sep = " & ", fill = "right") %>%
  separate(full_name, c("first_name", "last_name"), sep = name_separator, fill = "right") %>%
  # use partner last name if the first last name is missing
  separate(partner, c("partner_first_name", "partner_last_name"), sep = name_separator, fill = "right", remove = FALSE) %>%
  mutate(last_name = my_coalesce(last_name, partner_last_name)) %>%
  select(-partner_first_name, -partner_last_name)

# some donations might come from people not already in the database
new_addresses = 
  new_donation_addresses %>%
  select(-receive_date, -source, -total_amount, -campaign_name, -new_donation) %>%
  distinct

database = dbConnect(
  drv = MariaDB(),
  username = "wordpress",
  password = "Diddle22",
  host = "localhost",
  dbname = "wordpress"
)

tables = mdb.get("CPE.mdb", stringsAsFactors = FALSE, na.strings = "")

# snake case table names and column names
names(tables) = to_snake_case(names(tables))
for (name in names(tables)) {
  names(tables[[name]]) = to_snake_case(names(tables[[name]]))
}

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
    new_donation_addresses %>%
      filter(!is.na(campaign_name)) %>%
      select(name = campaign_name) %>%
      unique %>%
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

new_financial_type = 
  tibble(name = c("In Kind", "Grant")) %>%
  # make new ids
  mutate(id = max(old_financial_type$id) + 1:n())

civicrm_financial_type = bind_rows(old_financial_type, new_financial_type)

civicrm_tag =
  tables$category_codes %>%
  rename(
    id = category_id,
    name = description
  )

# members and their employers are in the same table
cpe_members_employers = 
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

committees =
  tables$committees %>%
  rename(
    id = committee_id,
    name = committee_name
  )

individuals = 
  tables$individuals %>%
  # UNSURE: are any of these columns important?
  # UNSURE: what do the codes in the type column mean?
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
    do_not_trade = !as.logical(trade),
    is_deleted = as.logical(is_deleted),
    modified_date = mdy_hms(modified_date)
  ) %>%
  select(-trade) %>%
  # update with addresses from new donations
  left_join(new_addresses, by = c("first_name", "last_name")) %>%
  mutate(
    # use new information if it exists
    city = my_coalesce(city.y, city.x),
    email = my_coalesce(email.y, email.x),
    partner = my_coalesce(partner.y, partner.x),
    postal_code = my_coalesce(postal_code.y, postal_code.x),
    phone = my_coalesce(phone.y, phone.x),
    state_province_abbreviation = my_coalesce(state_province_abbreviation.y, state_province_abbreviation.x),
    street_address = my_coalesce(street_address.y, street_address.x),
    supplemental_address_1 = my_coalesce(supplemental_address_1.y, supplemental_address_1.x)
  ) %>%
  select(
    -city.x, -city.y, -email.x, -email.y, -partner.x, -partner.y, 
    -phone.x, -phone.y, -postal_code.x, -postal_code.y,
    -state_province_abbreviation.x, -state_province_abbreviation.y,
    -street_address.x, -street_address.y, 
    -supplemental_address_1.x, -supplemental_address_1.y
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
      # UNSURE: there could be potentially inactive or resigned staff or advisors
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

organizations = 
  tables$organizations %>%
  # UNSURE: what do these codes in these two columns mean?
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
    contact_type = "Organization",
    created_date = mdy_hms(created_date),
    is_deleted = as.logical(is_deleted),
    modified_date = mdy_hms(modified_date)
  )

contact_info_without_partners =
  bind_rows(
    organizations,
    # add employers of cpe members: cpe members are all already in `individuals``
    cpe_members_employers %>%
      # UNSURE: the phone number is for the employer, not the member?
      select(-membership_status_type_id, -phone) %>%
      rename(employee_id = contact_id) %>%
      # remove missing employers
      filter(!is.na(organization_name)) %>%
      # could be an overlapping organization
      distinct %>%
      mutate(contact_type = "Organization") %>%
      # only if the organization isn't already a contact
      anti_join(organizations, by = "organization_name"),
    new_participants %>%
      select(-event_title, -participant_role_name, -register_date) %>%
      # could be an overlapping particant
      distinct %>%
      # ignore the contact info if they are already in the individuals table
      anti_join(individuals, by = c("first_name", "last_name")),
    new_participants %>%
      select(organization_name) %>%
      filter(!is.na(organization_name)) %>%
      # could be an overlapping organization
      distinct %>%
      # only if the organization isn't already a contact
      anti_join(organizations, by = "organization_name"),
    new_addresses %>%
      # could be an overlapping donation
      distinct %>%
      # only if not already in the table
      anti_join(individuals, by = c("first_name", "last_name")),
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
        contact_type = "Individual"
      ) %>%
      # only if not already in the table
      anti_join(individuals, by = c("first_name", "last_name"))
  ) %>%
  mutate(
    # create a new id for the new types of contacts
    id = max(individuals$id) + 1:n(),
    is_deleted = coalesce(is_deleted, FALSE)
  ) %>%
  # add to individuals
  bind_rows(
    individuals
  )

partners = 
  contact_info_without_partners %>%
  filter(!is.na(partner)) %>%
  select(partner, last_name, partner_id = id) %>%
  # split first and last names
  separate(partner, c("first_name", "other_last_name"), sep = name_separator, fill = "right") %>%
  mutate(
    contact_type = "Individual",
    # use partner's last name if no last name is given
    last_name = my_coalesce(other_last_name, last_name)
  ) %>%
  select(-other_last_name)

contact_info = 
  contact_info_without_partners %>%
  select(-partner) %>%
  # add partner id for existing contacts
  left_join(
    partners %>% select(first_name, last_name, partner_id)
  ) %>%
  bind_rows(
    # new partners
    partners %>%
      anti_join(contact_info_without_partners, by = c("first_name", "last_name")) %>%
      # new partner ids
      mutate(id = 1:n() + max(contact_info_without_partners$id))
  ) %>%
  mutate(is_deleted = my_coalesce(is_deleted, FALSE))

civicrm_contact = 
  contact_info %>%
  select(
    # address 
    -street_address, -supplemental_address_1, -city, -postal_code, -state_province_abbreviation, -country_name, 
    # email 
    -email, 
    # old id
    -organization_id,
    # notes
    -contact, -narrative,
    # phone
    -cell, -fax, -phone, -wphone, -phone_ext,
    # relationships
    -employee_id, -employer_organization_id, -partner_id, -employer_organization_name, 
    # tags
    -tag_id_1, -tag_id_2, -tag_id_3, 
    # website 
    -url
  )

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
    # truncate long street addresses (there's only one)
    # UNSURE: lose information
    street_address = stri_sub(street_address, to = 96)
  ) %>%
  # remove empty addresses
  filter(!(
    is.na(street_address) & 
      is.na(supplemental_address_1) & 
      is.na(city) &
      is.na(postal_code) & 
      is.na(state_province_abbreviation) & 
      is.na(country_name))) %>%
  mutate(
    country_name = 
      # United we fall
      ifelse(country_name == "USA", "United States", 
             ifelse(country_name == "England", "United Kingdom", country_name)
      )
  ) %>%
  # replace country names with ids
  # UNSURE: this will discard any countries not in the civicrm database
  left_join(
    dbReadTable(database, "civicrm_country") %>%
      select(
        country_name = name,
        country_id = id
      )
  ) %>%
  select(-country_name) %>%
  # replace state abbreviations with ids
  # UNSURE: this will discard any state abbreviations not in the civicrm database
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
  select(-rank) %>%
  mutate(entity_table = "civicrm_contact") %>%
  # if there is no matching tag, just forget it
  # UNSURE: this will discard tags with no matching id
  semi_join(civicrm_tag %>% select(tag_id = id))

civicrm_membership_type = 
  # create new membership types for non-committees
  membership_status_type %>%
  select(name = membership_type_name) %>%
  distinct %>%
  mutate(
    # new membership type ids for non-committees
    id = max(committees$id) + 1:n()
  ) %>%
  bind_rows(committees, .) %>%
  # UNSURE: what is a domain?
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
  # replace financial type name with id. doesn't matter because none of the memberships require payment yet
  mutate(financial_type_name = "Donation") %>%
  left_join(
    civicrm_financial_type %>%
      select(financial_type_name = name, financial_type_id = id)
  ) %>%
  select(-financial_type_name)

civicrm_note = 
  contact_info %>%
  # UNSURE: what's the difference between contact and narrative?
  select(contact_id = id, contact, narrative) %>%
  # put notes in long form
  gather("note_type", "note", -contact_id, na.rm = TRUE) %>%
  select(-note_type) %>%
  mutate(
    entity_id = contact_id,
    entity_table = "civicrm_contact"
  )

# put together attendees and speakers
civicrm_participant = 
  bind_rows(
    tables$attendance %>%
      select(-student_class_id) %>%
      rename(
        contact_id = attendee_id
      ) %>%
      mutate(
        participant_role_name = "Attendee"
      ), 
    tables$teachers %>%
      rename(
        contact_id = teacher_id
      ) %>%
      mutate(
        participant_role_name = "Speaker"
      ),
    new_participants %>%
      select(first_name, last_name, participant_role_name, event_title, register_date) %>%
      left_join(
        contact_info %>% 
          select(first_name, last_name, contact_id = id)) %>%
      select(-first_name, -last_name) %>%
      left_join(
        civicrm_event %>%
          select(event_id = id, event_title = title)
      ) %>%
      select(-event_title)
  ) %>%
  # replace participant role names with ids
  left_join(
    option_value %>%
      filter(option_group_name == "participant_role") %>%
      select(participant_role_name = name, role_id = value)
  ) %>%
  select(-participant_role_name) %>%
  # discard if the contact doesn't exist
  # UNSURE: potentially lose information
  semi_join(contact_info %>% select(contact_id = id)) %>%
  # discard if the event doesn't exist
  # UNSURE: potentially lose information
  semi_join(civicrm_event %>% select(event_id = id))

civicrm_phone = 
  contact_info %>%
  select(
    contact_id = id, cell, fax, phone, wphone, phone_ext
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
    cpe_members_employers %>%
    select(contact_id, membership_status_type_id) %>%
      # replace membership status ids with names so we can add new statuses by name
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
        # UNSURE: list committee members as expired because no information otherwise
        membership_status_name = "Expired",
        start_date = mdy_hms(start_date)
      )
  ) %>%
  # fill in expired if there is no status name
  mutate(membership_status_name = my_coalesce(membership_status_name, "Expired")) %>%
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
  select(contact_id = id, employer_organization_id, employee_id, partner_id, employer_organization_name) %>%
  # replace employer organization ids with contact ids
  # UNSURE: will discard organization ids if they don't exist
  left_join(
    organization_ids %>%
      select(employer_id_1 = id, employer_organization_id = organization_id)
  ) %>%
  select(-employer_organization_id) %>%
  # replace employer organzation names with contact ids
  left_join(
    contact_info %>%
      filter(!is.na(organization_name)) %>%
      select(employer_organization_name = organization_name, employer_id_2 = id)
  ) %>%
  select(-employer_organization_name) %>%
  mutate(
    # employers from the CPE members table takes precendence over employers from the new donations tabel
    employer_id = my_coalesce(employer_id_1, employer_id_2)
  ) %>%
  select(-employer_id_1, -employer_id_2)

contribution_donors = 
  tables$donations %>%
  # UNSURE: these columns seem potentially useful
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
    # UNSURE: potentially could be in kind and a grant?
    financial_type_name = 
      ifelse(as.logical(grant), "Grant", 
             ifelse(as.logical(inkind), "In Kind", "Donation")
      ),
    receive_date = mdy_hms(receive_date),
    # UNSURE: just use a thank you date of today
    thankyou_date = as.POSIXct(ifelse(letter, Sys.time(), NA), origin = "1970-01-01"),
    # fill in 0 if the amount is missing
    total_amount = my_coalesce(total_amount, 0)
  ) %>%
  select(-grant, -inkind, -letter) %>%
  bind_rows(
    tables$grants %>%
      # ignoring projects
      # UNSURE: are these grants already in the donations table?
      # UNSURE: what units is duration in?
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
    new_donation_addresses %>%
      filter(!is.na(new_donation)) %>%
      select(first_name, last_name, receive_date, source, total_amount, campaign_name) %>%
      # replace contact names with ids
      left_join(contact_info %>% select(first_name, last_name, contact_id_1 = id)) %>%
      select(-first_name, -last_name) %>%
      mutate(
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
  # UNSURE: will overwrite the employee ID's from the CPE table
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
  # UNSURE: lose information
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
  # UNSURE: use the first contact listed as the donor (Civicrm only allows 1 donor)
  top_n(-1, rank) %>%
  select(-rank) %>%
  left_join(contribution_donors, .) %>%
  select(-donation_id, -contact_id_1, -contact_id_2, -contact_id_3, -contact_id_4)

contribution_anonymous = 
  bind_rows(
    contribution %>%
      # UNSURE: there probably should just be one contact id for anonymous donations
      # fill in an anonymous donor if the contact id doesn't exist
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
      # unsure: lose information?
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
  # UNSURE: lose information
  semi_join(civicrm_contact %>% select(contact_id = id))

civicrm_pledge = 
  bind_rows(
   pledges %>%
      # fill in NA if the campaign doesn't exist
      # unsure: lose information?
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

civicrm_contact %>%
  filter(contact_type == "Individual") %>%
  select(id, first_name, last_name) %>%
  write_csv("individuals_reimport.csv", na = "")

civicrm_contact %>%
  filter(contact_type == "Organization") %>%
  select(id, organization_name) %>%
  write_csv("organizations_reimport.csv", na = "")
