event_address =
  tables$institutes_workshops_talks %>%
  rename(
    description = event_description,
    id = event_id,
    street_address = location,
    title = event_name
  ) %>%
  mutate(
    # inexact addresses for events
    street_address = paste_item_NA("Near", street_address)
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

civicrm_event = 
  event_address %>%
  # replace addresses with location block ids
  strict_join(
    location_block %>% 
      select(loc_block_id = id, street_address),
    "event_location"
  ) %>%
  select(-street_address) %>%
  mutate(
    event_type_name = "Workshop"
  ) %>%
  code_option(option_value, "event_type")

attendance =
  tables$attendance %>%
  select(-student_class_id) %>%
  rename(
    contact_id = attendee_id
  ) %>%
  mutate(
    participant_role_name = "Attendee",
    source = "Attendance"
  )

teachers = 
  tables$teachers %>%
  rename(
    contact_id = teacher_id
  ) %>%
  mutate(
    participant_role_name = "Speaker",
    source = "Teachers"
  )

new_participants = 
  bind_rows(
    participants_2017 %>%
      select(contact_id, event_title, participant_role_name, source),
    participants_2019 %>%
      select(contact_id, event_title, participant_role_name, source),
    participants_2019 %>%
      select(contact_id, event_title, participant_role_name, register_date, source)
  ) %>%
  # replace event titles with ids
  strict_join(
    civicrm_event %>% 
      select(event_title = title, event_id = id),
    "event"
  ) %>%
  select(-event_title)

# put together attendees and speakers
civicrm_participant = 
  bind_rows(attendance, teachers, new_participants) %>%
  mutate(
    is_test = FALSE,
    status_name = "Attended"
  ) %>%
  code(dbReadTable(database, "civicrm_participant_status_type"), "status") %>%
  code_option(option_value, "participant_role") %>%
  rename(role_id = participant_role_id) %>%
  # discard if the contact doesn't exist
  strict_join(contact_info %>% select(contact_id = id), "contact", drop = TRUE) %>%
  # discard if the event doesn't exist
  strict_join(civicrm_event %>% select(event_id = id), "event", drop = TRUE)
