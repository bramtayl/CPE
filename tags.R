civicrm_tag =
  tables$category_codes %>%
  rename(
    id = category_id,
    name = description
  ) %>%
  mutate(
    used_for = "civicrm_contact"
  )

# pull out tags from contact info in long form
civicrm_entity_tag = 
  contact_info %>%
  select(entity_id = id, tag_id_1, tag_id_2, tag_id_3) %>%
  gather("rank", "tag_id", tag_id_1, tag_id_2, tag_id_3, na.rm = TRUE) %>%
  # I assume the order doesn't mean anything?
  select(-rank) %>%
  mutate(entity_table = "civicrm_contact") %>%
  # 0 means missing?
  mutate(tag_id = ifelse(tag_id == 0, NA, tag_id)) %>%
  code(civicrm_tag, "tag", drop = TRUE)
