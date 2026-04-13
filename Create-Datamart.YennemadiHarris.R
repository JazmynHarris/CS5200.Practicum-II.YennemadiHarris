# ============================================================
# Merge Transaction Data into MySQL
# Author : Jazmyn Harris, Preethi Rajesh Yennemadi
# ============================================================

library(RSQLite)
library(RMySQL)
library(DBI)


# Expense Facts ------------------------------------------------------
#
#                                     Date 
#                                       |
#                                       |
#
#       ClientProject    ---     Expense Facts    ---     Category
# 
#                               |             |
#                               |             |
#                                 Currency
# Analytical Queries: 
#     Expense Total and Average per Category (rolled up from all subcategories)
#       per client per project and per month and per quarter and per year
# -----------------------------------------------------------------------------


# Connect to MySQL --------------------------------------------------------
DB_CERT <- <CERTIFICATE>

mart <- dbConnect(MySQL(),
                  user = "avnadmin",
                  password = <password>,
                  dbname = "defaultdb",
                  host = "practicum-i-cs5200-harrisj-jazmynrh12-9958.b.aivencloud.com",
                  port = 19929,
                  sslmode = "require",
                  sslcert = DB_CERT)

sqliteDb <- dbConnect(RSQLite::SQLite(), "expenseTracker.sqlite")


# Reset MySQL Database ----------------------------------------------------
tables <- dbListTables(mart)
dbExecute(mart, "SET FOREIGN_KEY_CHECKS = 0;")
for (table in tables) {
  dbExecute(mart, paste("DROP TABLE IF EXISTS", table))
}
dbExecute(mart, "SET FOREIGN_KEY_CHECKS = 1;")

# Create Dimension Tables -------------------------------------------------

# Date Dimension Table
dbExecute(mart, "
          CREATE TABLE IF NOT EXISTS DateDim (
          DateDimID INTEGER AUTO_INCREMENT PRIMARY KEY,
          Date DATE UNIQUE NOT NULL,
          Month INT NOT NULL,
          Quarter INT NOT NULL,
          Year INT NOT NULL)
          ")

# DateDim Indexes
dbExecute(mart, "CREATE INDEX MonthYearIDX ON DateDim(Month, Year)")
dbExecute(mart, "CREATE INDEX QuarterYearIDX ON DateDim(Quarter, Year)")

# ClientProject Dimension Table
dbExecute(mart, "
          CREATE TABLE IF NOT EXISTS ClientProjectDim (
          ClientProjectDimID INTEGER AUTO_INCREMENT PRIMARY KEY,
          ProjectID INTEGER NOT NULL,
          ProjectName VARCHAR(255) NOT NULL,
          ProjectBudget DOUBLE NOT NULL,
          ClientID INTEGER NOT NULL,
          ClientName VARCHAR(255) NOT NULL,
          CONSTRAINT UniqueProjClient UNIQUE(ProjectID, ClientID)
          )")

# Category Dimension Table
dbExecute(mart, "
          CREATE TABLE IF NOT EXISTS CategoryDim (
          CategoryDimID INTEGER AUTO_INCREMENT PRIMARY KEY,
          CategoryName VARCHAR(100) NOT NULL,
          SubCategoryName VARCHAR(100) NOT NULL,
          CONSTRAINT UniqueCategories UNIQUE(CategoryName, SubCategoryName))")

# Currency Dimension Table
dbExecute(mart, "
          CREATE TABLE IF NOT EXISTS CurrencyDim (
          CurrencyDimID INTEGER AUTO_INCREMENT PRIMARY KEY,
          CurrencyName VARCHAR(50) UNIQUE NOT NULL,
          USExchangeRate DECIMAL(15,2) NOT NULL)")


# Create Expense Fact Table -----------------------------------------------

dbExecute(mart, "
          CREATE TABLE IF NOT EXISTS ExpenseFacts (
          ExpenseFactsID INTEGER AUTO_INCREMENT PRIMARY KEY,
          DateDimID INTEGER NOT NULL,
          ClientProjectDimID INTEGER NOT NULL,
          CategoryDimID INTEGER NOT NULL,
          CurrencyDimID INTEGER NOT NULL,
          TotalAmount DECIMAL(15,2) NOT NULL,
          AverageAmount DECIMAL(15,2) NOT NULL,
          FOREIGN KEY (DateDimID) REFERENCES DateDim(DateDimID),
          FOREIGN KEY (ClientProjectDimID) REFERENCES ClientProjectDim(ClientProjectDimID),
          FOREIGN KEY (CategoryDimID) REFERENCES CategoryDim(CategoryDimID),
          FOREIGN KEY (CurrencyDimID) REFERENCES CurrencyDim(CurrencyDimID)
          )")

# ExpenseFacts Indexes
dbExecute(mart, "CREATE INDEX idx_fact_date ON ExpenseFacts(DateDimID)")
dbExecute(mart, "CREATE INDEX idx_fact_client_project ON ExpenseFacts(ClientProjectDimID)")
dbExecute(mart, "CREATE INDEX idx_fact_category ON ExpenseFacts(CategoryDimID)")
dbExecute(mart, "CREATE INDEX idx_fact_currency ON ExpenseFacts(CurrencyDimID)")

# Helper Function ---------------------------------------------------------
sql_format_value <- function(x) {
  
  if (is.na(x)) {
    return("NULL")
    
  } else if (inherits(x, "Date")) {
    return(paste0("'", format(x, "%Y-%m-%d"), "'"))
    
  } else if (is.character(x)) {
    return(paste0("'", gsub("'", "''", x), "'"))
    
  } else if (is.numeric(x) || is.integer(x)) {
    return(as.character(x))
    
  } else {
    return(paste0("'", gsub("'", "''", as.character(x)), "'"))
  }
}

# Insert Data Into A Table
insert_into_table <- function(martDb, df, tableName) {
  cat("Attempting Load into", tableName, "\n")
  dbBegin(martDb)
  print(paste("Names:", names(df)))
  tryCatch({
    batch_size <- 1000
    n <- nrow(df)
    
    for (start in seq(1, n, by = batch_size)) {
      cat("Inserting rows", start, "\n")
      
      batch <- df[start:min(start + batch_size - 1, n), ]
      
      values <- apply(batch, 1, function(row) {
        row <- sapply(row, sql_format_value)
        paste0("(", paste(row, collapse = ", "), ")")
      })
      values <- paste(values, collapse = ", ")
    
      query <- paste0(
        "INSERT IGNORE INTO ", tableName,
        " (", paste(names(batch), collapse = ", "), ") ",
        "VALUES\n", paste(values, collapse = ", ")
      )
    
      dbExecute(martDb, query)
    }
    dbCommit(martDb)
    cat("Loaded Data into", tableName, "\n")
  }, error = function(e) {
    dbRollback(martDb)
    cat("Attempt at loading into", tableName, "failed", "\n")
  })
}

# Inserts SQLite data into MySQL datamart
insert_datamart_tables <- function(martDb, df, tableName) {
  # Get Column List from Table Name
  colNames <- dbGetQuery(martDb, paste0("
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = '", tableName, "'
      AND COLUMN_KEY <> 'PRI'
    ORDER BY ORDINAL_POSITION
  "))$COLUMN_NAME
  
  # get columns that match in df
  colMatches <- df[, intersect(colNames, names(df)), drop = FALSE]
  
  insert_into_table(martDb, colMatches, tableName)
  
  return(dbReadTable(martDb, tableName))
}



# Merge From SQLite -------------------------------------------------------

merge_from_sqlite <- function(sqliteDb, martDb) {
  raw_data <- dbGetQuery(sqliteDb, "
                         SELECT t.Date, 
                         c.ClientID, c.Name AS ClientName, 
                         p.ProjectID, p.ProjectName, p.ProjectBudget,
                         ct.CategoryName, sc.SubCategoryName,
                         cu.CurrencyName, cu.USExchangeRate,
                         SUM(t.Amount) AS TotalAmount,
                         AVG(t.Amount) AS AverageAmount
                         FROM Transactions t
                         JOIN Employee e ON t.EmployeeID = e.EmployeeID
                         JOIN EmployeeProject ep ON e.EmployeeID = ep.EmployeeID
                         JOIN Project p ON p.ProjectID = ep.ProjectID
                         JOIN Client c ON c.ClientID = p.ClientID
                         JOIN Currency cu ON t.CurrencyID = cu.CurrencyID
                         JOIN SubCategories sc ON t.SubCategoryID = sc.SubCategoryID
                         JOIN ExpenseAllocationCategory ct ON sc.CategoryID = ct.CategoryID
                         GROUP BY 1,2,3,4,5,6,7,8,9,10")
  
  # Convert date to fit DateDim Table
  d <- as.Date(raw_data$Date)
  raw_data$Month   <- as.integer(format(d, "%m"))
  raw_data$Quarter <- (as.integer(format(d, "%m")) - 1) %/% 3 + 1
  raw_data$Year    <- as.integer(format(d, "%Y"))
  
  print(paste("Showing raw_data", head(raw_data), "\n"))
  cat("Num rows", nrow(raw_data), "\n")
  
  # Insert Data Into Dim Tables
  dateDim <- insert_datamart_tables(martDb, raw_data, "DateDim")
  clientProjectDim <- insert_datamart_tables(martDb, raw_data, "ClientProjectDim")
  categoryDim <- insert_datamart_tables(martDb, raw_data, "CategoryDim")
  currencyDim <- insert_datamart_tables(martDb, raw_data, "CurrencyDim")

  # Get Expense Facts
  fact_data <- merge(raw_data, dateDim, by = "Date")
  fact_data <- merge(
    fact_data,
    clientProjectDim,
    by = c("ClientID", "ProjectID")
  )
  fact_data <- merge(
    fact_data,
    categoryDim,
    by = c("CategoryName", "SubCategoryName")
  )
  fact_data <- merge(
    fact_data,
    currencyDim,
    by = "CurrencyName"
  )
  fact_data <- fact_data[, c(
    "DateDimID",
    "ClientProjectDimID",
    "CategoryDimID",
    "CurrencyDimID",
    "TotalAmount",
    "AverageAmount"
  )]

  print(paste("Showing Fact_Data", head(fact_data), "\n"))

  insert_into_table(martDb, fact_data, "ExpenseFacts")
}

merge_from_sqlite(sqliteDb, mart)


# Disconnect From Database ------------------------------------------------

dbDisconnect(mart)
dbDisconnect(sqliteDb)
