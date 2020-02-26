# add new financial types if they dont exist
tibble(
  name = c("In Kind", "Grant")
) %>%
  mutate(
    description = name,
    is_active = TRUE,
    is_reserved = FALSE
  ) %>%
  anti_join(dbReadTable(database, "civicrm_financial_type") %>% select(name)) %>%
  dbAppendTable(database, "civicrm_financial_type", .)

civicrm_financial_type = dbReadTable(database, "civicrm_financial_type", .)

donations = 
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
    # use current date for thank you date
    # ifelse drops POSIXct grr
    thankyou_date = as.POSIXct(ifelse(letter, Sys.time(), NA), origin = "1970-01-01"),
    # fill in 0 if the amount is missing
    total_amount = coalesce(total_amount, 0)
  ) %>%
  select(-grant, -inkind, -letter)

grants = 
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
    donation_id = 1:n() + max(donations$donation_id),
    financial_type_name = "Grant",
    total_amount = coalesce(total_amount, 0)
  )

new_donations = 
  new_donations_info %>%
  # only used marked new downations
  filter(!is.na(new_donation)) %>%
  select(contact_id_1 = contact_id, receive_date, source, total_amount, campaign_name) %>%
  mutate(
    donation_id = 1:n() + max(grants$donation_id),
    financial_type_name = "Donation",
    # convert date to datetime
    receive_date = mdy(receive_date)
  ) %>%
  # replace campaign names with ids
  strict_join(
    civicrm_campaign %>%
      select(
        campaign_name = name,
        campaign_id = id
      ),
    "campaign"
  ) %>%
  select(-campaign_name)

contribution_donors = 
  bind_rows(donations, grants, new_donations) %>%
  code(civicrm_financial_type, "financial_type") %>%
  # replace organization ids with contact ids
  strict_join(
    organization_ids %>%
      rename(contact_id_4 = contact_id),
    "organization"
  ) %>%
  select(-organization_id)

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
  ungroup %>%
  strict_join(contribution_donors, ., "donor") %>%
  mutate(
    contribution_status_name = "Completed",
    currency = "USD",
    fee_amount = 0,
    is_pay_later = FALSE,
    is_test = FALSE,
    net_amount = total_amount
  ) %>%
  select(-donation_id, -contact_id_1, -contact_id_2, -contact_id_3, -contact_id_4) %>%
  code_option(option_value, "contribution_status")

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
      strict_join(contact_info %>% select(contact_id = id), "contact", drop = TRUE)
  )

civicrm_contribution = 
  bind_rows(
    contribution_anonymous %>%
      # fill in NA if the campaign doesn't exist
      anti_join(civicrm_campaign %>% select(campaign_id = id)) %>%
      mutate(campaign_id = NA),
    contribution_anonymous %>%
      strict_join(
        civicrm_campaign %>% 
          select(campaign_id = id), 
        "campaign", drop = TRUE
      )
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
    cancel = as.logical(cancel),
    done = as.logical(done),
    currency = "USD",
    is_test = FALSE,
    modified_date = create_date,
    original_installment_amount = 0,
    pledge_status_name = 
      # can't be cancelled and completed
      as.character(ifelse(done, "Completed", ifelse(cancel, "Cancelled", "Pending"))),
    # can't be null; fill in an old time
    start_date = na.fill(as.POSIXct(start_date), as.POSIXct("2000-01-01"))
  ) %>%
  select(-done, -cancel) %>%
  # replace pledge status names with ids
  code_option(option_value, "pledge_status") %>%
  rename(status_id = pledge_status_id) %>%
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
