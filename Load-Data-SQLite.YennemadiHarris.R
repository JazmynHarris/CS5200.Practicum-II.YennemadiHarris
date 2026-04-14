# ============================================================
# Reads CSVs from intake/ folder and loads into SQLite DB
# Schema : expense-tracker-schema.png
# Author : Preethi Rajesh Yennemadi, Jazmyn Harris
# ============================================================
library(DBI)
library(RSQLite)

# Connect to SQLite ----------------
sqliteDb <- dbConnect(RSQLite::SQLite(), "expenseTracker.sqlite")


# Parsing file name to get details -------------
# extracts LastName, FirstName, CardName, Month, Year
# from "LastName, FirstName (Bank) MONTH YEAR.csv"
parse_filename <- function(filename) {
  pattern <- "^([^,]+), ([^(]+) \\(([^)]+)\\) (\\w+) (\\d{4})\\.csv$"
  m <- regmatches(filename, regexec(pattern, filename))[[1]]
  list(
    lastName  = trimws(m[2]),
    firstName = trimws(m[3]),
    cardName  = m[4],
    month     = m[5],
    year      = m[6]
  )
}

# Lookup or Insert Helper -----------------------------------------------
# Looks up an ID by name. If not found, inserts new record and returns new ID.
get_or_insert_id <- function(db, table, id_col, name_col, name_val, extra_cols = list()) {
  result <- dbGetQuery(db, paste0(
    "SELECT ", id_col, " FROM ", table,
    " WHERE ", name_col, " = '", gsub("'", "''", name_val), "'"
  ))[1, 1]
  
  if (is.na(result)) {
    all_cols <- c(name_col, names(extra_cols))
    all_vals <- c(
      paste0("'", gsub("'", "''", name_val), "'"),
      sapply(extra_cols, function(v) {
        if (is.character(v)) paste0("'", gsub("'", "''", v), "'")
        else as.character(v)
      })
    )
    dbExecute(db, paste0(
      "INSERT INTO ", table,
      " (", paste(all_cols, collapse = ", "), ") VALUES (",
      paste(all_vals, collapse = ", "), ")"
    ))
    cat("  [Safety Net] New record added to", table, ":", name_val, "\n")
    result <- dbGetQuery(db, paste0(
      "SELECT ", id_col, " FROM ", table,
      " WHERE ", name_col, " = '", gsub("'", "''", name_val), "'"
    ))[1, 1]
  }
  
  return(result)
}


# Insert from CSV into SQLite db -------------

# Reads all CSVs from intake folder and loads into Transactions table
load_transactions_from_intake <- function(sqliteDb, intake_folder, doc_folder) {
  
  files <- list.files(intake_folder, pattern = "\\.csv$", full.names = TRUE)
  
  if(!dir.exists(doc_folder)) {
    dir.create(doc_folder)
  }
  
  for (file in files) {
    
    info <- parse_filename(basename(file))
    
    empID <- dbGetQuery(sqliteDb, paste0(
      "SELECT EmployeeID FROM Employee WHERE FirstName = '", info$firstName,
      "' AND lastName = '", info$lastName, "'"
    ))[1,1]
    
    # Employee must exist - skip file if not found
    if (is.na(empID)) {
      cat("WARNING: Employee '", info$firstName, info$lastName,
          "' not found - skipping file:", basename(file), "\n")
      next
    }
    
    df <- read.csv(file, stringsAsFactors = FALSE)
    
    values <- c()
    
    for (i in 1:nrow(df)) {
      row <- df[i, ]
      
      # extracting id from lookup tables, auto-inserting if not found
      vendorID   <- get_or_insert_id(sqliteDb, "Vendor", "VendorID",
                                     "VendorName", row$Vendor)
      
      cardID     <- get_or_insert_id(sqliteDb, "CreditCardMerchant", "CreditCardMerchantID",
                                     "CreditCardMerchantName", row$CreditCardMerchant)
      
      currencyID <- get_or_insert_id(sqliteDb, "Currency", "CurrencyID",
                                     "CurrencyName", row$Currency,
                                     extra_cols = list(USExchangeRate = 1.0))

      catID    <- get_or_insert_id(sqliteDb, "ExpenseAllocationCategory", "CategoryID",
                                   "CategoryName", row$ExpenseAllocationCategory)
      subCatID <- get_or_insert_id(sqliteDb, "SubCategories", "SubCategoryID",
                                   "SubCategoryName", row$Subcategory,
                                   extra_cols = list(CategoryID = catID))
      
      billable <- ifelse(row$Billable == "Y", 1, 0)
      
      row_str <- paste0("('", row$Date, "', ", row$Amount, ", ", billable, ", ",
                        vendorID, ", ", cardID, ", ", currencyID, ", ",
                        empID, ", ", subCatID, ")")
      
      values <- c(values, row_str)
    }
    
    sql <- paste0(
      "INSERT INTO Transactions (Date, Amount, Billable, VendorID, CreditCardMerchantID, CurrencyID, EmployeeID, SubCategoryID) VALUES ",
      paste(values, collapse = ", ")
    )
    
    # Wraps each file INSERT IGNORE INTO single transaction
    dbBegin(sqliteDb)
    tryCatch({
      dbExecute(sqliteDb, sql)
      dbCommit(sqliteDb)
      cat("Loaded", nrow(df), "transactions from", basename(file), "\n")
      file.rename(from = file, to = file.path(doc_folder, basename(file)))
    }, error = function(e) {
      dbRollback(sqliteDb)
      cat("Attempt at loading from", basename(file), "failed", "\n")
    })
  }
}

load_transactions_from_intake(sqliteDb, "intake/", "processed")


# Disconnect SQLite ---------
dbDisconnect(sqliteDb)