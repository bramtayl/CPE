# this just splits names at the last space: good enough?
name_separator = " (?=[^\\s]+$)"

# fill in vector 2 where vector 1 is missing
coalesce = function(vector_1, vector_2)
  ifelse(is.na(vector_1), vector_2, vector_1)

# if y is missing, return missing
paste_item_NA = function(x, y)
  ifelse(is.na(y), NA, paste(x, y))

# warn when there is the potential for data loss
# drop: when there is no matching value, drop instead of return missing
strict_join = function(table_1, table_2, variable, drop = FALSE) {
  # this finds all values in table_1 without a match
  # round-about way to exclude NA keys from anti_join
  # we only care about the loss of non-missing data
  missings = semi_join(
    anti_join(table_1, table_2), 
    table_1[intersect(names(table_1), names(table_2))], 
    na_matches = "never"
  )
  if (nrow(missings) > 0) warning(variable)
  if (drop) inner_join(table_1, table_2) else left_join(table_1, table_2)
}

# convenience function: recode a variable in data with ids instead of names
# replace `variable_name` with `variable_id` in data
# lookup table must have two columns: name and id
code = function(data, lookup_table, variable, drop = FALSE) {
  name_name = paste0(variable, "_name")
  id_name = paste0(variable, "_id")
  selected_lookup = lookup_table[, c("name", "id")]
  names(selected_lookup) = c(name_name, id_name)
  result = strict_join(data, selected_lookup, variable, drop = drop)
  result[[name_name]] = NULL
  result
}

# in addition to the code steps above, will first subset big_lookup_table for
# when `option_group_name`` == variable. this is intend for use with the
# `option_value` table
code_option = function(data, big_lookup_table, variable, drop = FALSE)
  code(
    data, 
    filter(big_lookup_table, option_group_name == variable), 
    variable, drop = drop
  )

