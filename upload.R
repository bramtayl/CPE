dbAppendTable(database, "civicrm_campaign", civicrm_campaign)

# replace old tags
dbSendQuery(database, "DELETE FROM civicrm_tag")
dbAppendTable(database, "civicrm_tag", civicrm_tag)

# remove source because it prevents automatic deduplication
dbAppendTable(database, "civicrm_contact", civicrm_contact %>% select(-source))
dbAppendTable(database, "civicrm_address", civicrm_address)
dbAppendTable(database, "civicrm_loc_block", civicrm_loc_block)
dbAppendTable(database, "civicrm_email", civicrm_email)
dbAppendTable(database, "civicrm_event", civicrm_event)

dbAppendTable(database, "civicrm_entity_tag", civicrm_entity_tag)
dbAppendTable(database, "civicrm_group", civicrm_group)
dbAppendTable(database, "civicrm_group_contact", civicrm_group_contact)
dbAppendTable(database, "civicrm_note", civicrm_note)
dbAppendTable(database, "civicrm_participant", civicrm_participant)
dbAppendTable(database, "civicrm_phone", civicrm_phone)
dbAppendTable(database, "civicrm_website", civicrm_website)
dbAppendTable(database, "civicrm_relationship", civicrm_relationship)
dbAppendTable(database, "civicrm_pledge", civicrm_pledge)
dbSendQuery(database, "DELETE FROM civicrm_contribution")
dbAppendTable(database, "civicrm_contribution", civicrm_contribution)
