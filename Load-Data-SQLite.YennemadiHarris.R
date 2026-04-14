# ============================================================
# Reads CSVs from intake/ folder and loads into SQLite DB
# Schema : expense-tracker-schema.png
# Author : Preethi Rajesh Yennemadi, Jazmyn Harris
# ============================================================

if (!require(RSQLite)) install.packages("RSQLite")
library(RSQLite)

# Connect to SQLite ----------------
sqliteDb <- dbConnect(RSQLite::SQLite(), "expenseTracker.sqlite")

# Drop all tables ----------------
tables <- dbListTables(sqliteDb)
dbExecute(sqliteDb, "PRAGMA foreign_keys = off")
for (table in tables) {
  dbExecute(sqliteDb, paste("DROP TABLE IF EXISTS", table))
}
dbExecute(sqliteDb, "PRAGMA foreign_keys = on")


# Creating Schema ----------------
# Vendor - Lookup Table
dbExecute(sqliteDb, "
  CREATE TABLE IF NOT EXISTS Vendor (
    VendorID INTEGER PRIMARY KEY,
    VendorName VARCHAR(255) NOT NULL
  )
")

# CreditCardMerchant - Lookup Table
dbExecute(sqliteDb, "
  CREATE TABLE IF NOT EXISTS CreditCardMerchant (
    CreditCardMerchantID INTEGER PRIMARY KEY,
    CreditCardMerchantName TEXT NOT NULL
  )
")

# Currency - Lookup Table
dbExecute(sqliteDb, "
  CREATE TABLE IF NOT EXISTS Currency (
   CurrencyID INTEGER PRIMARY KEY,
   CurrencyName VARCHAR(50) NOT NULL,
   USExchangeRate DECIMAL(15,2) NOT NULL
  )
")

# Category - Lookup Table
dbExecute(sqliteDb, "
  CREATE TABLE IF NOT EXISTS ExpenseAllocationCategory (
   CategoryID INTEGER PRIMARY KEY,
   CategoryName VARCHAR(100) NOT NULL
  )
")

# SubCategories - Lookup Table
dbExecute(sqliteDb, "
  CREATE TABLE IF NOT EXISTS SubCategories (
   SubCategoryID INTEGER PRIMARY KEY,
   SubCategoryName VARCHAR(100) NOT NULL,
   CategoryID INTEGER NOT NULL,
   FOREIGN KEY (CategoryID) REFERENCES ExpenseAllocationCategory(CategoryID)
  )
")


# Employee
dbExecute(sqliteDb, "
  CREATE TABLE IF NOT EXISTS Employee (
   EmployeeID INTEGER PRIMARY KEY,
   FirstName TEXT NOT NULL,
   lastName TEXT NOT NULL
  )
")

# Client
dbExecute(sqliteDb, "
  CREATE TABLE IF NOT EXISTS Client (
   ClientID INTEGER PRIMARY KEY,
   Name TEXT NOT NULL
  )
")

# Project 
dbExecute(sqliteDb, "
  CREATE TABLE IF NOT EXISTS Project (
   ProjectID INTEGER PRIMARY KEY,
   ProjectName VARCHAR(255) NOT NULL,
   ProjectBudget DOUBLE NOT NULL,
   ClientID INTEGER NOT NULL,
   FOREIGN KEY (ClientID) REFERENCES Client(ClientID)
  )
")


# EmployeeProject - Join Table
dbExecute(sqliteDb, "
  CREATE TABLE IF NOT EXISTS EmployeeProject (
   EmployeeID INTEGER NOT NULL,
   ProjectID INTEGER NOT NULL,
   PRIMARY KEY (EmployeeID, ProjectID),
   FOREIGN KEY (EmployeeID) REFERENCES Employee(EmployeeID),
   FOREIGN KEY (ProjectID) REFERENCES Project(ProjectID)
  )
")



# Transactions
dbExecute(sqliteDb, "
  CREATE TABLE IF NOT EXISTS Transactions (
   TransactionID INTEGER PRIMARY KEY,
   Date DATE NOT NULL,
   Amount DOUBLE NOT NULL, 
   Billable BOOLEAN NOT NULL,
   VendorID INTEGER NOT NULL,
   CreditCardMerchantID INTEGER NOT NULL,
   CurrencyID INTEGER NOT NULL,
   EmployeeID INTEGER NOT NULL,
   SubCategoryID INTEGER NOT NULL,
   FOREIGN KEY (VendorID) REFERENCES Vendor(VendorID),
   FOREIGN KEY (CreditCardMerchantID) REFERENCES  CreditCardMerchant(CreditCardMerchantID),
   FOREIGN KEY (CurrencyID) REFERENCES Currency(CurrencyID),
   FOREIGN KEY (EmployeeID) REFERENCES Employee(EmployeeID),
   FOREIGN KEY (SubCategoryID) REFERENCES SubCategories(SubCategoryID)
  )
")




# Pre Populating Lookup Tables ---------------

# Vendor - Lookup Table
dbExecute(sqliteDb, "INSERT IGNORE INTO Vendor (VendorID, VendorName) VALUES
  (1 , 'DELTA AIR LINES'),
  (2 , 'UNITED AIRLINES'),
  (3 , 'MARRIOTT'),
  (4 , 'HILTON'),
  (5 , 'UBER'), 
  (6 , 'LYFT'), 
  (7 , 'SHELL'), 
  (8 , 'EXXON'),
  (9 , 'HUDSON NEWS'), 
  (10 , 'STARBUCKS'), 
  (11 , 'MCDONALDS'),
  (12 , 'HERTZ'), 
  (13 , 'AVIS'), 
  (14 , 'EXPEDIA'), 
  (15 , 'BOOKING.COM')
")


# CreditCardMerchant - Lookup Table
dbExecute(sqliteDb, "INSERT IGNORE INTO CreditCardMerchant (CreditCardMerchantID, CreditCardMerchantName) VALUES
  (1 , 'American Express'),
  (2 , 'Chase Sapphire'),
  (3 , 'Citi Platinum')
")

# Currency - Lookup Table
dbExecute(sqliteDb, "INSERT IGNORE INTO Currency (CurrencyID, CurrencyName, USExchangeRate) VALUES
  (1 , 'USD', 1.00),
  (2 , 'EUR', 1.17),
  (3 , 'GBP', 1.34),
  (4 , 'BRL', 0.20),
  (5 , 'CAD', 0.72),
  (6 , 'AUD', 0.70)
")


# Category - Lookup Table
dbExecute(sqliteDb, "INSERT IGNORE INTO ExpenseAllocationCategory (CategoryID, CategoryName) VALUES
  (1 , 'Travel'),
  (2 , 'Lodging'),
  (3 , 'Meals')
")

# SubCategories - Lookup Table
dbExecute(sqliteDb, "INSERT IGNORE INTO SubCategories ( SubCategoryID, SubCategoryName , CategoryID) VALUES
  (1 , 'Airfare', 1),
  (2 , 'Hotel', 2),
  (3 , 'Ground Transportation', 1),
  (4 , 'Fuel', 1),
  (5 , 'Snacks', 3),
  (6 , 'Coffee', 3),
  (7 , 'Restaurant', 3),
  (8 , 'Car Rental', 1),
  (9 , 'Booking Fees', 1)
")



# Pre Populating Employee, Client, Project and Employee Project Tables ---------------

# Employee
dbExecute(sqliteDb, "INSERT IGNORE INTO Employee (EmployeeID, FirstName, lastName ) VALUES
  (1 , 'John' , 'Smith'),
  (2 , 'Jane' , 'Doe'),
  (3 , 'Mary' , 'Garcia'),
  (4 , 'Jason' , 'Hudson'),
  (5 , 'Hannah' , 'Cavaldi'),
  (6 , 'Dave' , 'Johnson'),
  (7 , 'Anna' , 'Wilson'), 
  (8 , 'Jacob' , 'Wang'),
  (9 , 'Megan' , 'Lee'),
  (10, 'Logan' , 'Key')
")

# Client
dbExecute(sqliteDb, "INSERT IGNORE INTO Client (ClientID, Name) VALUES
  (1, 'BHP Group'),
  (2, 'Rio Tinto'),
  (3, 'Southern Copper Corporation'),
  (4, 'Vale SA'),
  (5, 'Glencore')
")


# Project 
dbExecute(sqliteDb, "INSERT IGNORE INTO Project (ProjectID, ProjectName, ProjectBudget, ClientID) VALUES
  (1, 'BHP Site Assessment', 500000, 1),
  (2, 'BHP Compliance Review', 750000, 1),
  (3, 'Rio Tinto Site Assessment', 300000, 2),
  (4, 'Rio Tinto Compliance Review', 450000, 2),
  (5, 'Southern Copper Site Assessment', 600000, 3),
  (6, 'Southern Copper Compliance Review', 350000, 3),
  (7, 'Vale Site Assessment', 400000, 4),
  (8, 'Vale Compliance Review', 550000, 4),
  (9, 'Glencore Site Assessment', 480000, 5),
  (10, 'Glencore Compliance Review', 700000, 5)
")



# EmployeeProject - Join Table
dbExecute(sqliteDb, "INSERT IGNORE INTO EmployeeProject (EmployeeID, ProjectID) VALUES
  (1, 1), (1, 3),
  (2, 2), (2, 5),
  (3, 4), (3, 7),
  (4, 6), (4, 9),
  (5, 8), (5, 10),
  (6, 1), (6, 6),
  (7, 2), (7, 8),
  (8, 3), (8, 9),
  (9, 4), (9, 10),
  (10, 5), (10, 7)
")


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
      "INSERT IGNORE INTO ", table,
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
load_transactions_from_intake <- function(sqliteDb, intake_folder) {
  
  files <- list.files(intake_folder, pattern = "\\.csv$", full.names = TRUE)
  
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
      "INSERT IGNORE INTO Transactions (Date, Amount, Billable, VendorID, CreditCardMerchantID, CurrencyID, EmployeeID, SubCategoryID) VALUES ",
      paste(values, collapse = ", ")
    )
    
    # Wraps each file INSERT IGNORE INTO single transaction
    dbBegin(sqliteDb)
    tryCatch({
      dbExecute(sqliteDb, sql)
      dbCommit(sqliteDb)
      cat("Loaded", nrow(df), "transactions from", basename(file), "\n")
    }, error = function(e) {
      dbRollback(sqliteDb)
      cat("Attempt at loading from", basename(file), "failed", "\n")
    })
    
    
    
    
  }
}

load_transactions_from_intake(sqliteDb, "intake/")


# Disconnect SQLite ---------
dbDisconnect(sqliteDb)