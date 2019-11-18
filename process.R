library(Hmisc)
library(dplyr)
library(tidyr)
library(lubridate)
library(snakecase)
library(purrr)
library(stringi)
library(DBI)
library(RMariaDB)

# if vector 1 is missing, use vector 2
my_coalesce = function(vector_1, vector_2)
  ifelse(is.na(vector_1), vector_2, vector_1)

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

civicrm_campaign = 
  tables$development_campaigns %>%
  rename(
    id = development_project_id,
    start_date = begin_date
  ) %>%
  mutate(
    end_date = mdy_hms(end_date),
    start_date = mdy_hms(start_date)
  )
dbAppendTable(database, "civicrm_campaign", civicrm_campaign)

civicrm_event_address =
  tables$institutes_workshops_talks %>%
  rename(
    description = event_description,
    id = event_id,
    title = event_name,
  ) %>%
  mutate(
    end_date = mdy_hms(end_date),
    start_date = mdy_hms(start_date),
    # inexact addresses for events
    street_address = 
      ifelse(is.na(location), NA, paste("Near", location))
  ) %>% 
  select(-location)

old_financial_type = 
  dbReadTable(database, "civicrm_financial_type") %>%
  select(id, name)
new_financial_type = 
  tibble(name = c("In Kind", "Grant")) %>%
  # make new ids
  mutate(id = max(old_financial_type$id) + 1:n())
civicrm_financial_type = bind_rows(old_financial_type, new_financial_type)
dbAppendTable(database, "civicrm_financial_type", new_financial_type)

civicrm_tag =
  tables$category_codes %>%
  rename(
    id = category_id,
    name = description
  )
# replace old tags
dbSendQuery(database, "DELETE FROM civicrm_tag")
dbAppendTable(database, "civicrm_tag", civicrm_tag)

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
  # TODO: add note
  select(-brochure, -check_back, -coa, -d_100, 
         -d_50, -date_enter, -deleted, -fall_d_mail, -fall_newsletter, 
         -group, -idcode, -interested, -lastattend, -lastgave, 
         -prospect, -spring_d_mail, -spring_newsletter, -type) %>%
  rename(
    birth_date = birthday,
    country_name = country,
    do_not_phone = no_call,
    first_name = first,
    formal_title = title,
    id = individual_id,
    employer_organization_id = organization_id,
    last_name = last,
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
    do_not_phone = as.logical(do_not_phone),
    do_not_trade = !as.logical(trade)
  ) %>%
  select(-trade)

membership_status_type = 
  tables$cpe_member_status_codes %>%
  rename(
    membership_status_type_id = status_code
  ) %>%
  left_join(
    tibble(
      # these descriptions combine two separate ideas: membership type and status
      # split out into two separate columns
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

contact_info =
  bind_rows(
    # add a new contact for partners
    individuals %>%
      filter(!is.na(partner)) %>%
      select(partner, last_name, partner_id = id) %>%
      # split first and last names
      separate(partner, c("first_name", "other_last_name"), extra = "merge", fill = "right") %>%
      mutate(
        contact_type = "Individual",
        # use partner's last name if the same
        last_name = my_coalesce(other_last_name, last_name)
      ) %>%
      select(-other_last_name), 
    tables$organizations %>%
      select(-coa, -code, -date_enter, -deleted, -group) %>%
      rename(
        country_name = country,
        organization_name = name,
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
        contact_type = "Organization"
      ), 
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
      ),
    # add employers of cpe members: cpe members are all already in `individuals``
    cpe_members_employers %>%
      select(employee_id = contact_id, organization_name, phone,
             street_address, city, postal_code, state_province_abbreviation) %>%
      # remove missing employers
      filter(!(is.na(phone) & is.na(street_address) & 
                 is.na(city) & is.na(postal_code) & is.na(state_province_abbreviation))) %>%
      mutate(contact_type = "Organization")
  ) %>%
  mutate(
    # create a new id for the new types of contacts
    id = max(individuals$id) + 1:n()
  ) %>%
  # add to individuals
  bind_rows(
    individuals %>%
      select(-partner)
  )

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
    -cell, -fax, -phone, -phone_ext, -wphone, 
    # relationships
    -employee_id, -employer_organization_id, -partner_id, 
    # tags
    -tag_id_1, -tag_id_2, -tag_id_3, 
    # website 
    -url
  ) %>%
  dbAppendTable(database, "civicrm_contact", .)

civicrm_loc_block = 
  civicrm_event_address %>%
  select(street_address) %>%
  filter(!is.na(street_address)) %>%
  distinct %>%
  mutate(
    # create new location block ids
    id = 1:n(),
    # with identical addreess ids
    address_id = id
  )

bind_rows(
  civicrm_loc_block %>%
    select(-id) %>%
    rename(id = address_id),
  contact_info %>%
    select(
      contact_id = id, street_address, supplemental_address_1, 
      city, postal_code, state_province_abbreviation, country_name
    )
) %>%
  mutate(id = coalesce(id, 1:n())) %>%
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
  left_join(
    dbReadTable(database, "civicrm_country") %>%
      select(
        country_name = name,
        country_id = id
      )
  ) %>%
  select(-country_name) %>%
  # replace state abbreviations with ids
  left_join(
    dbReadTable(database, "civicrm_state_province") %>%
      select(
        country_id,
        state_province_abbreviation = abbreviation,
        state_province_id = id
      )
  ) %>%
  select(-state_province_abbreviation) %>%
  dbAppendTable(database, "civicrm_address", .)

dbAppendTable(
  database, "civicrm_loc_block", 
  civicrm_loc_block %>% select(-street_address)
)

contact_info %>%
  select(
    contact_id = id, email
  ) %>%
  filter(!is.na(email)) %>%
  dbAppendTable(database, "civicrm_email", .)

civicrm_event_address %>%
  left_join(
    civicrm_loc_block %>% 
      select(loc_block_id = id, street_address)
  ) %>%
  select(-street_address) %>%
  dbAppendTable(database, "civicrm_event", .)

# pull out tags from contact info in long form
contact_info %>%
  select(entity_id = id, tag_id_1, tag_id_2, tag_id_3) %>%
  gather("rank", "tag_id", tag_id_1, tag_id_2, tag_id_3, na.rm = TRUE) %>%
  select(-rank) %>%
  mutate(entity_table = "civicrm_contact") %>%
  # if there is no matching tag, just forget it
  semi_join(civicrm_tag %>% select(tag_id = id)) %>%
  dbAppendTable(database, "civicrm_entity_tag", .)

civicrm_membership_type = 
  membership_status_type %>%
  select(name = membership_type_name) %>%
  distinct %>%
  mutate(
    # new membership type ids for non-committees
    id = max(committees$id) + 1:n()
  ) %>%
  # committee membership is just a kind of membership
  bind_rows(committees, .) %>%
  # replace domain name with id
  mutate(domain_name = "Default Domain Name") %>%
  left_join(
    dbReadTable(database, "civicrm_domain") %>%
      select(domain_id = id, domain_name = name)
  ) %>%
  select(-domain_name) %>%
  # replace organization name with id
  mutate(organization_name = "Default Organization") %>%
  left_join(
    dbReadTable(database, "civicrm_contact") %>%
      select(member_of_contact_id = id, organization_name)
  ) %>%
  select(-organization_name) %>%
  # replace financial type name with id
  mutate(financial_type_name = "Donation") %>%
  left_join(
    civicrm_financial_type %>%
      select(financial_type_name = name, financial_type_id = id)
  ) %>%
  select(-financial_type_name)
dbAppendTable(database, "civicrm_membership_type", civicrm_membership_type)

contact_info %>%
  select(contact_id = id, contact, narrative) %>%
  gather("note_type", "note", -contact_id, na.rm = TRUE) %>%
  select(-note_type) %>%
  mutate(
    entity_id = contact_id,
    entity_table = "civicrm_contact"
  ) %>%
  dbAppendTable(database, "civicrm_note", .)

# put together attendees and speakers
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
    )
) %>%
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
  semi_join(civicrm_event %>% select(event_id = id)) %>%
  dbAppendTable(database, "civicrm_participant", .)

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
  select(-phone_type_location_type_name) %>%
  dbAppendTable(database, "civicrm_phone", .)

contact_info %>%
  select(
    contact_id = id, url
  ) %>%
  filter(!is.na(url)) %>%
  dbAppendTable(database, "civicrm_website", .)

# use to replace old organization ids with new contact ids
organization_ids = 
  contact_info %>%
  select(id, organization_id) %>%
  filter(!is.na(organization_id))

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
  semi_join(civicrm_membership_type %>% select(membership_type_id = id)) %>%
  dbAppendTable(database, "civicrm_membership", .)

contact_relationship = 
  contact_info %>%
  select(contact_id = id, employer_organization_id, employee_id, partner_id) %>%
  # replace employer organization ids with contact ids
  left_join(
    organization_ids %>%
      rename(employer_id = id, employer_organization_id = organization_id)
  ) %>%
  select(-employer_organization_id)

contribution_donors = 
  tables$donations %>%
  select(-letter, -pledge_id, -pledge, -request) %>%
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
    financial_type_name = 
      ifelse(as.logical(grant), "Grant", 
             ifelse(as.logical(inkind), "In Kind", "Donation")
      ),
    receive_date = mdy_hms(receive_date),
    # fill in 0 if the amount is missing
    total_amount = my_coalesce(total_amount, 0)
  ) %>%
  select(-grant, -inkind) %>%
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

contact_relationship %>%
  left_join(
    # replace employer ids with employee ids
    contact_relationship %>%
      filter(!is.na(employer_id)) %>%
      select(contact_id = employer_id, new_employee_id = contact_id)
  ) %>%
  select(-employer_id) %>%
  # fill in old employer ids
  mutate(employee_id = my_coalesce(new_employee_id, employee_id)) %>%
  # put relationships in long form
  select(-new_employee_id) %>%
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
  semi_join(contact_info %>% select(contact_id_b = id)) %>%
  dbAppendTable(database, "civicrm_relationship", .)

# put contact ids for donations in long form
contribution = 
  contribution_donors %>%
  select(
    donation_id, 
    contact_id_1, contact_id_2, contact_id_3, contact_id_4
  ) %>%
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

contribution_anonymous = 
  contribution %>%
  # fill in an anonymous donor if the contact id doesn't exist
  anti_join(contact_info %>% select(contact_id = id)) %>%
  mutate(
    contact_id = 
      contact_info %>%
      filter(first_name == "Anonymous") %>%
      .$id %>%
      first
  ) %>%
  bind_rows(
    contribution %>%
      semi_join(contact_info %>% select(contact_id = id))
  )

contribution_anonymous %>%
  # fill in NA if the campaign doesn't eist
  anti_join(civicrm_campaign %>% select(campaign_id = id)) %>%
  mutate(campaign_id = NA) %>%
  bind_rows(
    contribution_anonymous %>%
      semi_join(civicrm_campaign %>% select(campaign_id = id))
  ) %>%
  dbAppendTable(database, "civicrm_contribution", .)
