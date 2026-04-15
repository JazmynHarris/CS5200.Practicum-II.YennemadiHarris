# ============================================================
# Generate Synthetic Transaction CSVs
# Mimics: one CSV per employee per bank per month
# Output: intake/LastName, FirstName (Bank) MONTH YEAR.csv
# Author : Jazmyn Harris, Preethi Rajesh Yennemadi
# ============================================================

# TRANSACTION CSV FORMAT ------------------------------------------------------
#
# Each CSV represents one employee's transactions for one credit card for one month.
# Filename format: LastName, FirstName (Bank) MONTH YEAR.csv
# e.g. Smith, John (American Express) JAN 2026.csv
#
# Columns:
#   Date                      : Date of transaction (YYYY-MM-DD)
#   Vendor                    : Merchant name e.g. HUDSON NEWS
#   Amount                    : Negative if expense, Positive if credit/refund
#   CreditCardMerchant        : Card used e.g. American Express, Citi Platinum
#   ExpenseAllocationCategory : e.g. Travel, Lodging, Meals
#   Subcategory               : e.g. Airfare, Hotel, Restaurant
#   Currency                  : e.g. USD, EUR, BRL
#   Billable                  : Y if billable to a project, N otherwise
#
# Note: Employee name, client, and project are NOT in the CSV.
#   - Employee is derived during ingestion from the filename.
#   - Client and project are added later via expense reports.
# -----------------------------------------------------------------------------


set.seed(123)

# Defining Employee List ------------------------------------------------------
employees <- list(
  list(lastName = "Smith", firstName = "John", cards = c("American Express", "Chase Sapphire")),
  list(lastName = "Doe", firstName = "Jane", cards = c("Citi Platinum", "Chase Sapphire")),
  list(lastName = "Garcia", firstName = "Mary", cards = c("American Express", "Citi Platinum")),
  list(lastName = "Hudson", firstName = "Jason", cards = c("American Express", "Chase Sapphire")),
  list(lastName = "Cavaldi", firstName = "Hannah", cards = c("Citi Platinum", "Chase Sapphire")),
  list(lastName = "Johnson", firstName = "Dave", cards = c("American Express", "Citi Platinum")),
  list(lastName = "Wilson", firstName = "Anna", cards = c("American Express", "Chase Sapphire")),
  list(lastName = "Wang", firstName = "Jacob", cards = c("Citi Platinum", "Chase Sapphire")),
  list(lastName = "Lee", firstName = "Megan", cards = c("American Express", "Citi Platinum")),
  list(lastName = "Key", firstName = "Logan", cards = c("American Express", "Chase Sapphire"))
)

# Defining Month List ------------------------------------------------------
months <- list(
  list(label = "JUL 2025", start = "2025-07-01", end = "2025-07-31"),
  list(label = "AUG 2025", start = "2025-08-01", end = "2025-08-31"),
  list(label = "SEP 2025", start = "2025-09-01", end = "2025-09-30"),
  list(label = "OCT 2025", start = "2025-10-01", end = "2025-10-31"),
  list(label = "NOV 2025", start = "2025-11-01", end = "2025-11-30"),
  list(label = "DEC 2025", start = "2025-12-01", end = "2025-12-31"),
  list(label = "JAN 2026", start = "2026-01-01", end = "2026-01-31"),
  list(label = "FEB 2026", start = "2026-02-01", end = "2026-02-28"),
  list(label = "MAR 2026", start = "2026-03-01", end = "2026-03-31")
)


# Vendors mapped to category/subcategory ------------------------------------------------------
vendor_map <- data.frame(
  Vendor = c(
    "DELTA AIR LINES", "UNITED AIRLINES", "MARRIOTT", "HILTON",
    "UBER", "LYFT", "SHELL", "EXXON",
    "HUDSON NEWS", "STARBUCKS", "MCDONALDS",
    "HERTZ", "AVIS", "EXPEDIA", "BOOKING.COM"
  ),
  Category = c(
    "Travel", "Travel", "Lodging", "Lodging",
    "Travel", "Travel", "Travel", "Travel",
    "Meals", "Meals", "Meals",
    "Travel", "Travel", "Travel", "Travel"
  ),
  Subcategory = c(
    "Airfare", "Airfare", "Hotel", "Hotel",
    "Ground Transportation", "Ground Transportation", "Fuel", "Fuel",
    "Snacks", "Coffee", "Restaurant",
    "Car Rental", "Car Rental", "Booking Fees", "Booking Fees"
  ),
  stringsAsFactors = FALSE
)

# Currencies and Billable Flag------------------------------------------------------
currencies <- c("USD", "EUR", "GBP", "BRL", "CAD", "AUD")

billable_flag <- c("Y", "N")

# Generate Transactions Function ------------------------------------------------------
generate_transactions <- function(n, start_date, end_date, card_name) {
  
  vendors_sample <- vendor_map[sample(1:nrow(vendor_map), n, replace = TRUE), ]
  
  random_dates <- sample(seq(as.Date(start_date), as.Date(end_date), by = "day"), n, replace = TRUE)
  
  
  # generate amounts (mostly expenses, some refunds)
  amounts <- round(rnorm(n, mean = -150, sd = 120), 2)
  
  # randomly flip some to credits/refunds
  refund_idx <- sample(1:n, size = floor(0.08 * n))
  amounts[refund_idx] <- abs(amounts[refund_idx])
  
  # build dataframe
  synthetic_data <- data.frame(
    Date = random_dates,
    Vendor = vendors_sample$Vendor,
    Amount = amounts,
    CreditCardMerchant = card_name,
    ExpenseAllocationCategory = vendors_sample$Category,
    Subcategory = vendors_sample$Subcategory,
    Currency = sample(currencies, n, replace = TRUE),
    Billable = sample(billable_flag, n, replace = TRUE, prob = c(0.8, 0.2)),
    stringsAsFactors = FALSE
  )
  
  return(synthetic_data)
}


# Check folder exits ------------------------------------------------------
if (!dir.exists("intake")) {
  dir.create("intake")
}

# Main loop to load into the files as per Use Case------------------------------------------------------
for (emp in employees) {
  for (card in emp$cards) {
    for (month in months) {
      synthetic_data <- generate_transactions(30, month$start, month$end, card)
      filename <- paste0(emp$lastName, ", ", emp$firstName, " (", card, ") ", month$label, ".csv")
      
      write.csv(synthetic_data, paste0("intake/",filename), row.names = FALSE)
    }
  }
}
