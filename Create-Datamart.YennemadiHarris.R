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

mart <- dbConnect(MySQL(),
                  dbname = "classicmodels", 
                  host = "localhost", 
                  port = 3306, 
                  user = "root", 
                  password = "password")


# Reset MySQL Database ----------------------------------------------------

# TODO: Delete all tables


# Create Dimension Tables -------------------------------------------------

# Date Dimension Table
dbExecute(mart, "
          CREATE TABLE IF NOT EXISTS DateDim (
          DateDimID INTEGER AUTO_INCREMENT PRIMARY KEY,
          Date DATE NOT NULL,
          Month INT NOT NULL,
          Quarter INT NOT NULL,
          Year INT NOT NULL)
          ")

# TODO: INDEX FOR (MONTH, YEAR) AND (QUARTER, YEAR)

# ClientProject Dimension Table
dbExecute(mart, "
          CREATE TABLE IF NOT EXISTS ClientProjectDim (
          ClientProjectDimID INTEGER AUTO_INCREMENT PRIMARY KEY,
          ProjectID INTEGER UNIQUE,
          ProjectName TEXT NOT NULL,
          ProjectBudget DOUBLE NOT NULL,
          ClientID INTEGER NOT NULL,
          ClientName TEXT NOT NULL
          )")

# Category Dimension Table
dbExecute(mart, "
          CREATE TABLE IF NOT EXISTS CategoryDim
          CategoryDimID INTEGER AUTO_INCREMENT PRIMARY KEY,
          CategoryName TEXT NOT NULL,
          SubCategoryName TEXT NOT NULL)")

# TODO: (CATEGORYNAME, SUBCAT NAME UNIQUE)

# Currency Dimension Table
dbExecute(mart, "
          CREATE TABLE IF NOT EXISTS CurrencyDim
          CurrencyDimID INTEGER AUTO_INCREMENT PRIMARY KEY,
          CurrencyName TEXT NOT NULL,
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



# Helper Function ---------------------------------------------------------

# Insert Data Into A Table
insert_into_table <- function(martDb, df, tableName) {
  
  for (i in seq_len(nrow(df))) {
    row <- df[i, , drop = FALSE]
    
    placeholders <- paste(rep("?", ncol(row)), collapse = ", ")
    
    
    query <- paste0(
      "INSERT IGNORE INTO ", tableName,
      " (", paste(names(row), collapse = ", "), ") ",
      "VALUES (", placeholders, ")"
    )
    
    dbBegin(martDb)
    dbExecute(martDb, query, params = as.list(row))
    dbCommit(martDb)
    cat("Loaded Data into", tableName, "\n")
  }
}

# Inserts SQLite data into MySQL datamart
insert_datamart_tables <- function(martDb, df, tableName) {
  # Get Column List from Table Name
  colNames <- dbListFields(martDb, tableName)
  
  # get columns that match in df
  colMatches <- df[, colNames[colNames %in% names(df)], drop = FALSE]
  
  insert_into_table(martDb, colMatches, tableName)
  
  return(dbReadTable(martDb, tableName))
}



# Merge From SQLite -------------------------------------------------------

merge_from_sqlite <- function(sqliteDb, martDb) {
  raw_data <- dbGetQuery(sqliteDb, "
                         SELECT t.Date, 
                         c.ClientID, c.Name, 
                         p.ProjectID, p.ProjectName, p.ProjectBudget,
                         ct.CategoryName, sc.SubCategoryName,
                         cu.CurrencyName, cu.USExchangeRate,
                         SUM(t.Amount) AS TotalAmount,
                         AVG(t.Amount) AS AverageAmount
                         FROM Transactions t
                         JOIN Employee e ON t.EmployeeID = e.EmployeeID
                         JOIN EmployeeProject ep ON e.EmployeeID = ep.EmployeeProjectID,
                         JOIN Project p ON p.ProjectID = ep.ProjectID
                         JOIN Currency cu ON t.CurrencyID = cu.CurrencyID
                         JOIN SubCategory sc ON t.SubCategoryID = sc.SubCategoryID
                         JOIN Category ct ON sc.CategoryID = ct.CategoryID
                         GROUP BY 1,2,3,4,5,6,7,8,9,10")
  
  # Convert date to fit DateDim Table 
  df$Month   <- as.integer(format(df$Date, "%m"))
  df$Quarter <- (as.integer(format(df$Date, "%m")) - 1) %/% 3 + 1
  df$Year    <- as.integer(format(df$Date, "%Y"))
  
  # Insert Data Into Dim Tables
  tableNames <- c("DateDim", "ClientProjectDim", "CategoryDim", "CurrencyDim")
  dateDim <- insert_datamart_tables(martDb, raw_data, "DateDim")
  clientProjectDim <- insert_datamart_tables(martDb, raw_data, "ClientProjectDim")
  CategoryDim <- insert_datamart_tables(martDb, raw_data, "CategoryDim")
  CurrencyDim <- insert_datamart_tables(martDb, raw_data, "CurrencyDim")
  
  # Get Expense Facts
  fact_data <- merge(raw_data, date_dim, by = "Date")
  fact_data <- merge(
    fact_data,
    client_project_dim,
    by = c("ClientID", "ProjectID")
  )
  fact_data <- merge(
    fact_data,
    category_dim,
    by = c("CategoryName", "SubCategoryName")
  )
  fact_data <- merge(
    fact_data,
    currency_dim,
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
  
  cat("Showing Fact_Data")
  head(fact_data)
  
  insert_into_table(martDb, fact_data, "ExpenseFacts")
}


# Disconnect From Database ------------------------------------------------

dbDisconnect(mart)

