# ============================================================
# Merge Transaction Data into MySQL
# Author : Jazmyn Harris, Preethi Rajesh Yennemadi
# ============================================================

if (!require(RSQLite))   install.packages("RSQLite")
if (!require(RMySQL))    install.packages("RMySQL")
if (!require(DBI))       install.packages("DBI")
if (!require(mongolite)) install.packages("mongolite")
library(RSQLite)
library(RMySQL)
library(DBI)
library(mongolite)


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


# Connect to MySQL, SQLite and MongoDB --------------------------------------------------------
  DB_CERT <- < CERTIFICATE >
  
  mart <- dbConnect(
    MySQL(),
    user = < user > ,
    password = < password > ,
    dbname = "defaultdb",
    host = "practicum-i-cs5200-harrisj-jazmynrh12-9958.b.aivencloud.com",
    port = 19929,
    sslmode = "require",
    sslcert = DB_CERT
  )

  sqliteDb <- dbConnect(RSQLite::SQLite(), "expenseTracker.sqlite")

  mongoDb <- mongo(
      collection = "expenseReports",
      db         = "expenseTracker",
      url        = "mongodb+srv://admin:<password>@expensetracker.5qwqidw.mongodb.net/?appName=ExpenseTracker"
    )
  
  # Reset MySQL Database ----------------------------------------------------
  tables <- dbListTables(mart)
  dbExecute(mart, "SET FOREIGN_KEY_CHECKS = 0;")
  for (table in tables) {
    dbExecute(mart, paste("DROP TABLE IF EXISTS", table))
  }
  dbExecute(mart, "SET FOREIGN_KEY_CHECKS = 1;")
  
  # Create Dimension Tables -------------------------------------------------
  
  # Date Dimension Table
  # DateDim aggregates at Month/Quarter/Year level not individual dates
  dbExecute(
    mart,
    "
    CREATE TABLE IF NOT EXISTS DateDim (
      DateDimID INTEGER AUTO_INCREMENT PRIMARY KEY,
      Month INT NOT NULL,
      Quarter INT NOT NULL,
      Year INT NOT NULL,
      CONSTRAINT UniqueMonthQuarterYear UNIQUE (Month, Quarter, Year)
    )
    "
  )

  # DateDim Indexes
  dbExecute(mart, "CREATE INDEX MonthYearIDX ON DateDim(Month, Year)")
  dbExecute(mart, "CREATE INDEX QuarterYearIDX ON DateDim(Quarter, Year)")

  # ClientProject Dimension Table
  dbExecute(
    mart,
    "
          CREATE TABLE IF NOT EXISTS ClientProjectDim (
          ClientProjectDimID INTEGER AUTO_INCREMENT PRIMARY KEY,
          ProjectID INTEGER NOT NULL,
          ProjectName VARCHAR(255) NOT NULL,
          ProjectBudget DOUBLE NOT NULL,
          ClientID INTEGER NOT NULL,
          ClientName VARCHAR(255) NOT NULL,
          CONSTRAINT UniqueProjClient UNIQUE(ProjectID, ClientID)
          )"
  )

  # Category Dimension Table
  dbExecute(
    mart,
    "
          CREATE TABLE IF NOT EXISTS CategoryDim (
          CategoryDimID INTEGER AUTO_INCREMENT PRIMARY KEY,
          CategoryName VARCHAR(100) NOT NULL,
          SubCategoryName VARCHAR(100) NOT NULL,
          CONSTRAINT UniqueCategories UNIQUE(CategoryName, SubCategoryName))"
  )

  # Currency Dimension Table
  dbExecute(
    mart,
    "
          CREATE TABLE IF NOT EXISTS CurrencyDim (
          CurrencyDimID INTEGER AUTO_INCREMENT PRIMARY KEY,
          CurrencyName VARCHAR(50) UNIQUE NOT NULL,
          USExchangeRate DECIMAL(15,2) NOT NULL)"
  )

  # EmployeeDim - needed for BillableFacts
  dbExecute(
    mart,
    "
    CREATE TABLE IF NOT EXISTS EmployeeDim (
      EmployeeDimID INTEGER AUTO_INCREMENT PRIMARY KEY,
      EmployeeID INTEGER NOT NULL,
      EmployeeName VARCHAR(255) NOT NULL,
      CONSTRAINT UniqueEmployee UNIQUE (EmployeeID)
    )
    "
  )
  dbExecute(mart, "CREATE INDEX idx_emp_id ON EmployeeDim(EmployeeID)")

  # Create Expense Fact Table -----------------------------------------------

  dbExecute(
    mart,
    "
    CREATE TABLE IF NOT EXISTS ExpenseFacts (
      ExpenseFactsID INTEGER AUTO_INCREMENT,
      DateDimID INTEGER NOT NULL,
      ClientProjectDimID INTEGER NOT NULL,
      CategoryDimID INTEGER NOT NULL,
      CurrencyDimID INTEGER NOT NULL,
      Year INTEGER NOT NULL,
      Quarter INTEGER NOT NULL,
      TotalAmount DECIMAL(15,2) NOT NULL,
      AverageAmount DECIMAL(15,2) NOT NULL,
      PRIMARY KEY (ExpenseFactsID, Year)
    )
    PARTITION BY RANGE (Year) (
      PARTITION p2024 VALUES LESS THAN (2025),
      PARTITION p2025 VALUES LESS THAN (2026),
      PARTITION p2026 VALUES LESS THAN (2027),
      PARTITION pMax  VALUES LESS THAN MAXVALUE
    )
    "
  )

  # ExpenseFacts Indexes
  dbExecute(mart, "CREATE INDEX idx_fact_date ON ExpenseFacts(DateDimID)")
  dbExecute(mart, "CREATE INDEX idx_fact_client_proj ON ExpenseFacts(ClientProjectDimID)")
  dbExecute(mart, "CREATE INDEX idx_fact_category ON ExpenseFacts(CategoryDimID)")
  dbExecute(mart, "CREATE INDEX idx_fact_currency ON ExpenseFacts(CurrencyDimID)")
  dbExecute(mart, "CREATE INDEX idx_fact_year_quarter ON ExpenseFacts(Year, Quarter)")


  # BillableFacts Fact Table ------------------------------------------------

  dbExecute(
    mart,
    "
    CREATE TABLE IF NOT EXISTS BillableFacts (
      BillableFactsID INTEGER AUTO_INCREMENT,
      DateDimID INTEGER NOT NULL,
      ClientProjectDimID INTEGER NOT NULL,
      EmployeeDimID INTEGER NOT NULL,
      Year INT NOT NULL,
      Quarter INT NOT NULL,
      BillableAmountUSD DECIMAL(15,2) NOT NULL,
      NonBillableAmountUSD DECIMAL(15,2) NOT NULL,
      BillableCount INT NOT NULL,
      NonBillableCount INT NOT NULL,
      TotalAmountUSD DECIMAL(15,2) NOT NULL,
      PRIMARY KEY (BillableFactsID, Year)
    )
    PARTITION BY RANGE (Year) (
      PARTITION p2024 VALUES LESS THAN (2025),
      PARTITION p2025 VALUES LESS THAN (2026),
      PARTITION p2026 VALUES LESS THAN (2027),
      PARTITION pMax  VALUES LESS THAN MAXVALUE
    )
    "
  )
  dbExecute(mart, "CREATE INDEX idx_bill_date ON BillableFacts(DateDimID)")
  dbExecute(mart, "CREATE INDEX idx_bill_proj ON BillableFacts(ClientProjectDimID)")
  dbExecute(mart, "CREATE INDEX idx_bill_emp ON BillableFacts(EmployeeDimID)")
  dbExecute(mart, "CREATE INDEX idx_bill_yr_qtr ON BillableFacts(Year, Quarter)")



  # BudgetVsActualFacts Fact Table ------------------------------------------
  dbExecute(
    mart,
    "
    CREATE TABLE IF NOT EXISTS BudgetVsActualFacts (
      BudgetFactsID INTEGER AUTO_INCREMENT,
      DateDimID INTEGER NOT NULL,
      ClientProjectDimID INTEGER NOT NULL,
      Year INT NOT NULL,
      Quarter INT NOT NULL,
      ProjectBudget DECIMAL(15,2) NOT NULL,
      ActualSpendUSD DECIMAL(15,2) NOT NULL,
      Variance DECIMAL(15,2) NOT NULL,
      PRIMARY KEY (BudgetFactsID, Year)
    )
    PARTITION BY RANGE (Year) (
      PARTITION p2024 VALUES LESS THAN (2025),
      PARTITION p2025 VALUES LESS THAN (2026),
      PARTITION p2026 VALUES LESS THAN (2027),
      PARTITION pMax  VALUES LESS THAN MAXVALUE
    )
    "
  )
  dbExecute(mart, "CREATE INDEX idx_budget_date ON BudgetVsActualFacts(DateDimID)")
  dbExecute(mart, "CREATE INDEX idx_budget_proj ON BudgetVsActualFacts(ClientProjectDimID)")
  dbExecute(mart, "CREATE INDEX idx_budget_yr_qtr ON BudgetVsActualFacts(Year, Quarter)")
  
  
  
  
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
          "INSERT IGNORE INTO ",
          tableName,
          " (",
          paste(names(batch), collapse = ", "),
          ") ",
          "VALUES\n",
          paste(values, collapse = ", ")
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
    colNames <- dbGetQuery(
      martDb,
      paste0(
        "
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = '",
        tableName,
        "'
      AND COLUMN_KEY <> 'PRI'
    ORDER BY ORDINAL_POSITION
  "
      )
    )$COLUMN_NAME
    
    # get columns that match in df
    colMatches <- df[, intersect(colNames, names(df)), drop = FALSE]
    
    insert_into_table(martDb, colMatches, tableName)
    
    return(dbReadTable(martDb, tableName))
  }
  
  
  
  # Merge From SQLite -------------------------------------------------------
  
  merge_from_sqlite <- function(sqliteDb, martDb) {
    raw_data <- dbGetQuery(
      sqliteDb,
      "
      SELECT
        CAST(strftime('%m', t.Date) AS INTEGER) AS Month,
        ((CAST(strftime('%m', t.Date) AS INTEGER) - 1) / 3) + 1 AS Quarter,
        CAST(strftime('%Y', t.Date) AS INTEGER) AS Year,
        c.ClientID, c.Name AS ClientName,
        p.ProjectID, p.ProjectName, p.ProjectBudget,
        ct.CategoryName, sc.SubCategoryName,
        cu.CurrencyName, cu.USExchangeRate,
        SUM(ABS(t.Amount) * cu.USExchangeRate) AS TotalAmount,
        AVG(ABS(t.Amount) * cu.USExchangeRate) AS AverageAmount
      FROM Transactions t
      JOIN Employee e ON t.EmployeeID = e.EmployeeID
      JOIN EmployeeProject ep ON e.EmployeeID = ep.EmployeeID
      JOIN Project p ON p.ProjectID = ep.ProjectID
      JOIN Client c ON c.ClientID = p.ClientID
      JOIN Currency cu ON t.CurrencyID = cu.CurrencyID
      JOIN SubCategories sc ON t.SubCategoryID = sc.SubCategoryID
      JOIN ExpenseAllocationCategory ct ON sc.CategoryID = ct.CategoryID
      GROUP BY
        Month, Quarter, Year,
        c.ClientID, ClientName,
        p.ProjectID, p.ProjectName, p.ProjectBudget,
        ct.CategoryName, sc.SubCategoryName,
        cu.CurrencyName, cu.USExchangeRate
      "
    )
    cat("Rows fetched from SQLite:", nrow(raw_data), "\n")
    
    # Insert Data Into Dim Tables
    dateDim <- insert_datamart_tables(martDb, raw_data, "DateDim")
    clientProjectDim <- insert_datamart_tables(martDb, raw_data, "ClientProjectDim")
    categoryDim <- insert_datamart_tables(martDb, raw_data, "CategoryDim")
    currencyDim <- insert_datamart_tables(martDb, raw_data, "CurrencyDim")
    
    # Get Expense Facts
    fact_data <- merge(raw_data,
                       dateDim[, c("Month", "Quarter", "Year", "DateDimID")],
                       by = c("Month", "Quarter", "Year"))
    fact_data <- merge(fact_data,
                       clientProjectDim[, c("ClientID", "ProjectID", "ClientProjectDimID")],
                       by = c("ClientID", "ProjectID"))
    fact_data <- merge(fact_data,
                       categoryDim[, c("CategoryName", "SubCategoryName", "CategoryDimID")],
                       by = c("CategoryName", "SubCategoryName"))
    fact_data <- merge(fact_data,
                       currencyDim[, c("CurrencyName", "CurrencyDimID")],
                       by = "CurrencyName")
    
    fact_data <- fact_data[, c(
      "DateDimID",
      "ClientProjectDimID",
      "CategoryDimID",
      "CurrencyDimID",
      "Year",
      "Quarter",
      "TotalAmount",
      "AverageAmount"
    )]
    
    insert_into_table(martDb, fact_data, "ExpenseFacts")
    
    # Load EmployeeDim --------------------------------------------------------
    employees_raw <- dbGetQuery(sqliteDb,"SELECT EmployeeID,FirstName || ' ' || lastName AS EmployeeName
       FROM Employee" )
    employeeDim <- insert_datamart_tables(martDb, employees_raw, "EmployeeDim")
    
    # Load BillableFacts ------------------------------------------------------
    billable_raw <- dbGetQuery(sqliteDb,
      "
      SELECT
        CAST(strftime('%m', t.Date) AS INTEGER) AS Month,
        ((CAST(strftime('%m', t.Date) AS INTEGER) - 1) / 3) + 1 AS Quarter,
        CAST(strftime('%Y', t.Date) AS INTEGER)  AS Year,
        e.EmployeeID,
        p.ProjectID,
        c.ClientID,
        SUM(CASE WHEN t.Billable = 1 THEN ABS(t.Amount) * cu.USExchangeRate ELSE 0 END) AS BillableAmountUSD,
        SUM(CASE WHEN t.Billable = 0 THEN ABS(t.Amount) * cu.USExchangeRate ELSE 0 END) AS NonBillableAmountUSD,
        SUM(CASE WHEN t.Billable = 1 THEN 1 ELSE 0 END) AS BillableCount,
        SUM(CASE WHEN t.Billable = 0 THEN 1 ELSE 0 END) AS NonBillableCount,
        SUM(ABS(t.Amount) * cu.USExchangeRate) AS TotalAmountUSD
      FROM Transactions t
      JOIN Employee e ON t.EmployeeID = e.EmployeeID
      JOIN EmployeeProject ep ON e.EmployeeID  = ep.EmployeeID
      JOIN Project p ON p.ProjectID = ep.ProjectID
      JOIN Client c ON c.ClientID = p.ClientID
      JOIN Currency cu ON t.CurrencyID = cu.CurrencyID
      GROUP BY Month, Quarter, Year, e.EmployeeID, p.ProjectID, c.ClientID
      "
    )
    
    
    billable_data <- merge(billable_raw,
                           dateDim[, c("Month", "Quarter", "Year", "DateDimID")],
                           by = c("Month", "Quarter", "Year"))
    billable_data <- merge(billable_data,
                           clientProjectDim[, c("ClientID", "ProjectID", "ClientProjectDimID")],
                           by = c("ClientID", "ProjectID"))
    billable_data <- merge(billable_data,
                           employeeDim[, c("EmployeeID", "EmployeeDimID")],
                           by = "EmployeeID")
    
    billable_data <- billable_data[, c(
      "DateDimID", "ClientProjectDimID", "EmployeeDimID",
      "Year", "Quarter",
      "BillableAmountUSD", "NonBillableAmountUSD",
      "BillableCount", "NonBillableCount", "TotalAmountUSD"
    )]
    insert_into_table(martDb, billable_data, "BillableFacts")
    
    # Load BudgetVsActualFacts ------------------------------------------------
    budget_raw <- dbGetQuery(sqliteDb,
      "
      SELECT
        CAST(strftime('%m', t.Date) AS INTEGER) AS Month,
        ((CAST(strftime('%m', t.Date) AS INTEGER) - 1) / 3) + 1 AS Quarter,
        CAST(strftime('%Y', t.Date) AS INTEGER) AS Year,
        p.ProjectID,
        c.ClientID,
        p.ProjectBudget,
        SUM(ABS(t.Amount) * cu.USExchangeRate) AS ActualSpendUSD
      FROM Transactions t
      JOIN Employee e ON t.EmployeeID = e.EmployeeID
      JOIN EmployeeProject ep ON e.EmployeeID = ep.EmployeeID
      JOIN Project p ON p.ProjectID = ep.ProjectID
      JOIN Client c ON c.ClientID = p.ClientID
      JOIN Currency cu ON t.CurrencyID = cu.CurrencyID
      GROUP BY Month, Quarter, Year, p.ProjectID, c.ClientID, p.ProjectBudget
      "
    )
    
    # Compute variance = budget minus actual spend
    budget_raw$Variance <- budget_raw$ProjectBudget - budget_raw$ActualSpendUSD
    
    budget_data <- merge(budget_raw, dateDim[, c("Month", "Quarter", "Year", "DateDimID")],
                         by = c("Month", "Quarter", "Year"))
    budget_data <- merge(budget_data,
                         clientProjectDim[, c("ClientID", "ProjectID", "ClientProjectDimID")],
                         by = c("ClientID", "ProjectID"))
    budget_data <- budget_data[, c(
      "DateDimID", "ClientProjectDimID",
      "Year", "Quarter",
      "ProjectBudget", "ActualSpendUSD", "Variance"
    )]
    insert_into_table(martDb, budget_data, "BudgetVsActualFacts")
  }
  
  # Merge From MongoDB -------------------------------------------------------
  # Pulls expense report data from MongoDB and merges into MySQL fact tables
  
  merge_from_mongodb <- function(martDb) {
    cat("MongoDB documents found:", mongoDb$count(), "\n")

    if (mongoDb$count() == 0) {
      cat("No MongoDB documents found - skipping MongoDB merge\n")
      mongoDb$disconnect()
      return()
    }
    
    # Pull all expense reports from MongoDB
    reports <- mongoDb$find()
    
    # Read existing dimension tables from MySQL for FK resolution
    dateDim          <- dbReadTable(martDb, "DateDim")
    clientProjectDim <- dbReadTable(martDb, "ClientProjectDim")
    employeeDim      <- dbReadTable(martDb, "EmployeeDim")
    
    # Flatten projectSplits for ExpenseFacts ----------------------------------
    # Each report has multiple projectSplits - one row per split per report
    expense_rows <- list()
    billable_rows <- list()
    
    for (i in 1:nrow(reports)) {
      report <- reports[i, ]
      
      month   <- as.integer(report$month)
      quarter <- ((month - 1) %/% 3) + 1
      year    <- as.integer(report$year)
      emp_id  <- as.integer(report$employeeID)
      
      # Resolve DateDimID
      date_match <- dateDim[dateDim$Month == month &
                              dateDim$Quarter == quarter &
                              dateDim$Year == year, ]
      if (nrow(date_match) == 0) next
      date_dim_id <- date_match$DateDimID[1]
      
      # Resolve EmployeeDimID
      emp_match <- employeeDim[employeeDim$EmployeeID == emp_id, ]
      if (nrow(emp_match) == 0) next
      emp_dim_id <- emp_match$EmployeeDimID[1]
      
      # Flatten projectSplits
      splits <- report$projectSplits[[1]]
      if (is.null(splits) || length(splits) == 0) next
      
      for (j in 1:length(splits)) {
        split <- splits[[j]]
        
        proj_id   <- as.integer(split$projectID)
        
        # Resolve ClientProjectDimID
        cp_match <- clientProjectDim[clientProjectDim$ProjectID == proj_id, ]
        if (nrow(cp_match) == 0) next
        cp_dim_id <- cp_match$ClientProjectDimID[1]
        
        subtotal <- as.numeric(split$subtotal)
        
        # Add to expense rows
        expense_rows[[length(expense_rows) + 1]] <- data.frame(
          DateDimID          = date_dim_id,
          ClientProjectDimID = cp_dim_id,
          CategoryDimID      = 1,
          CurrencyDimID      = 1,
          Year               = year,
          Quarter            = quarter,
          TotalAmount        = subtotal,
          AverageAmount      = subtotal,
          stringsAsFactors   = FALSE
        )
      }
      
      # Flatten transactions for BillableFacts
      txns <- report$transactions[[1]]
      if (is.null(txns) || length(txns) == 0) next
      
      billable_usd     <- 0
      non_billable_usd <- 0
      billable_count   <- 0
      non_billable_count <- 0
      total_usd        <- 0
      
      for (k in 1:length(txns)) {
        txn    <- txns[[k]]
        amt    <- abs(as.numeric(txn$amount))
        
        # Safety check - default to 0 if billable field is missing or NULL
        billable_val <- txn$billable
        if (is.null(billable_val) || length(billable_val) == 0) billable_val <- 0
        is_bill <- as.integer(billable_val)
        
        total_usd <- total_usd + amt
        
        if (is_bill == 1) {
          billable_usd   <- billable_usd + amt
          billable_count <- billable_count + 1
        } else {
          non_billable_usd   <- non_billable_usd + amt
          non_billable_count <- non_billable_count + 1
        }
      }
      
      # One billable row per employee per project split per month
      for (j in 1:length(splits)) {
        split     <- splits[[j]]
        proj_id   <- as.integer(split$projectID)
        cp_match  <- clientProjectDim[clientProjectDim$ProjectID == proj_id, ]
        if (nrow(cp_match) == 0) next
        cp_dim_id <- cp_match$ClientProjectDimID[1]
        frac      <- as.numeric(split$fraction)
        
        billable_rows[[length(billable_rows) + 1]] <- data.frame(
          DateDimID            = date_dim_id,
          ClientProjectDimID   = cp_dim_id,
          EmployeeDimID        = emp_dim_id,
          Year                 = year,
          Quarter              = quarter,
          BillableAmountUSD    = round(billable_usd * frac, 2),
          NonBillableAmountUSD = round(non_billable_usd * frac, 2),
          BillableCount        = round(billable_count * frac),
          NonBillableCount     = round(non_billable_count * frac),
          TotalAmountUSD       = round(total_usd * frac, 2),
          stringsAsFactors     = FALSE
        )
      }
    }
    
    # Insert into ExpenseFacts
    if (length(expense_rows) > 0) {
      expense_df <- do.call(rbind, expense_rows)
      cat("MongoDB ExpenseFacts rows to insert:", nrow(expense_df), "\n")
      insert_into_table(martDb, expense_df, "ExpenseFacts")
    }
    
    # Insert into BillableFacts
    if (length(billable_rows) > 0) {
      billable_df <- do.call(rbind, billable_rows)
      cat("MongoDB BillableFacts rows to insert:", nrow(billable_df), "\n")
      insert_into_table(martDb, billable_df, "BillableFacts")
    }
    
    mongoDb$disconnect()
    cat("MongoDB merge complete\n")
  }
  
  merge_from_sqlite(sqliteDb, mart)
  merge_from_mongodb(mart)
  
  
  # Disconnect From Database ------------------------------------------------
  
  dbDisconnect(mart)
  dbDisconnect(sqliteDb)
  