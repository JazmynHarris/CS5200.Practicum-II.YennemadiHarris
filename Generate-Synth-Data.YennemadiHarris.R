
# TRANSACTION CSV FORMAT ------------------------------------------------------
# 
# Description: Vendor or Merchant e.g. HUDSON NEWS
# Amount: Negative if an Expense, Positive if Credit/Refund
# Credit Card Merchant: e.g. American Express or Citi Platinum
# Expense Allocation Category: e.g. Travel or Lodging
# Subcategory: e.g. Restaurants
# Currency: e.g. USD, BRL, EURO
# Billable Flag: Y/N --> T/F
#

set.seed(123)

n <- 500   # number of rows to generate

# Vendors mapped to category/subcategory
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

credit_cards <- c(
  "American Express", "Citi Platinum",
  "Chase Sapphire", "Capital One Venture"
)

currencies <- c("USD", "EUR", "GBP", "BRL", "CAD", "AUD")

billable_flag <- c("Y", "N")

# sample vendors
vendors_sample <- vendor_map[sample(1:nrow(vendor_map), n, replace = TRUE), ]

# generate amounts (mostly expenses, some refunds)
amounts <- round(rnorm(n, mean = -150, sd = 120), 2)

# randomly flip some to credits/refunds
refund_idx <- sample(1:n, size = floor(0.08 * n))
amounts[refund_idx] <- abs(amounts[refund_idx])

# build dataframe
synthetic_data <- data.frame(
  Vendor = vendors_sample$Vendor,
  Amount = amounts,
  CreditCardMerchant = sample(credit_cards, n, replace = TRUE),
  ExpenseAllocationCategory = vendors_sample$Category,
  Subcategory = vendors_sample$Subcategory,
  Currency = sample(currencies, n, replace = TRUE),
  Billable = sample(billable_flag, n, replace = TRUE, prob = c(0.8, 0.2)),
  stringsAsFactors = FALSE
)

# write to CSV
write.csv(synthetic_data, "data/synthetic_expenses.csv", row.names = FALSE)

head(synthetic_data)