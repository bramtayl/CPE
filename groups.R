group_contact =
  bind_rows(
    # make a group for the old mailing lists, committees, and general members
    mailchimp %>%
      select(contact_id, group_name), 
    rise_up_community %>%
      select(contact_id, group_name), 
    rise_up_members %>%
      select(contact_id, group_name), 
    rise_up_local %>%
      select(contact_id, group_name),
    tables$committee_members %>%
      select(-committee_member_id, -join_date, -quit_date) %>%
      rename(
        group_id = committee_id,
        contact_id = member_id
      ) %>%
      # replace old committee ids
      strict_join(
        tables$committees %>%
          rename(
            group_id = committee_id,
            group_name = committee_name
          ),
        "committee",
        drop = TRUE
      ) %>%
      select(-group_id),
    cpe_members %>%
      select(contact_id, group_name, status_code) %>%
      # replace old member status codes with values
      strict_join(
        tables$cpe_member_status_codes %>%
          mutate(
            status = 
              ifelse(
                description == "Inactive" | description == "Resigned",
                "Removed",
                "Added"
              )
          ) %>%
          select(-description),
        "member status"
      ) %>%
      select(-status_code)
  ) %>%
  filter(!is.na(group_name))

civicrm_group = 
  group_contact %>%
  select(name = group_name) %>%
  distinct %>%
  mutate(
    group_type_name = "Mailing List",
    id = 1:n() + 1, # 1 existing group (administrators) already
    is_active = TRUE,
    title = name
  ) %>%
  code_option(option_value, "group_type") %>%
  rename(group_type = group_type_id)

civicrm_group_contact = 
  group_contact %>%
  code(civicrm_group, "group") %>%
  # discard if the contacts don't exist
  strict_join(contact_info %>% select(contact_id = id), "contact", drop = TRUE) %>%
  mutate(
    status = coalesce(status, "Added")
  )
