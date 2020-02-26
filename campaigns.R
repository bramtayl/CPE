campaign = 
  tables$development_campaigns %>%
  rename(
    id = development_project_id,
    start_date = begin_date
  )

civicrm_campaign = 
  bind_rows(
    campaign,
    # create new campaigns for those referenced in the new donations table
    new_donations_info %>%
      select(name = campaign_name) %>%
      distinct %>%
      filter(!is.na(name)) %>%
      anti_join(campaign) %>%
      # create a new id for the new campaigns
      mutate(id = max(campaign$id) + 1:n())
  ) %>%
  mutate(
    campaign_status_name = "Completed",
    is_active = FALSE,
    title = name
  ) %>%
  code_option(option_value, "campaign_status") %>%
  rename(status_id = campaign_status_id)