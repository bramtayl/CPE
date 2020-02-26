# use to replace old organization ids with new contact ids
organization_ids = 
  organizations %>%
  select(contact_id, organization_id)

civicrm_relationship = 
  contact_info %>%
  select(contact_id = id, employer_organization_id, partner_id) %>%
  # replace employer organization ids with contact ids
  strict_join(
    organization_ids %>%
      select(employer_id = contact_id, employer_organization_id = organization_id),
    "organization_id"
  ) %>%
  select(-employer_organization_id) %>%
  # reverse the relationship: we want employers, not employees
  strict_join(
    new_employers_employees %>%
      strict_join(new_employers) %>%
      select(new_employer_id = contact_id, contact_id = employee_id),
    "employer"
  ) %>%
  mutate(
    employer_id = coalesce(employer_id, new_employer_id)
  ) %>%
  select(-new_employer_id) %>%
  # put relationships in long form
  rename(
    contact_id_a = contact_id, 
    `Employee of` = employer_id,
    `Partner of` = partner_id
  ) %>%
  gather("relationship_type_name_a_b", "contact_id_b", -contact_id_a, na.rm = TRUE) %>%
  mutate(
    description = relationship_type_name_a_b,
    is_active = TRUE,
    # so you can see who you're in a relationship with
    is_permission_a_b = TRUE,
    is_permission_b_a = TRUE
  ) %>%
  # replace relationship type names with ids
  strict_join(
    dbReadTable(database, "civicrm_relationship_type") %>%
      select(
        relationship_type_id = id,
        relationship_type_name_a_b = name_a_b
      ),
    "relationship_type"
  ) %>%
  select(-relationship_type_name_a_b)