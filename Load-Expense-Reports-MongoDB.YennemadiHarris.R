# ============================================================
# Generates synthetic expense reports and loads into MongoDB
# Author: Preethi Rajesh Yennemadi, Jazmyn Harris
# ============================================================


library(RSQLite)
library(mongolite)

# Connect to SQLite ------------------
sqliteDb <- dbConnect(RSQLite::SQLite(), "expenseTracker.sqlite")

# Connect to MongoDB -----------------
mongoDb <- mongo(
  collection = "expenseReports",
  db         = "expenseTracker",
  url        = "mongodb+srv://admin:<password>@expensetracker.5qwqidw.mongodb.net/?appName=ExpenseTracker"
)

# Drop collection to avoid duplicates on re-run
mongoDb$drop()

cat("MongoDB documents before insert:", mongoDb$count(), "\n")


# defining approvers since SQLlite does not have it ----------
approvers <- list(
  list(approverID = 1, name = "Sarah Connor"),
  list(approverID = 2, name = "Michael Scott")
)

# Building expense report ----------
build_expense_report <- function(employee, transactions, approver, month, year) {
  
  # pull employee's projects from SQLite
  projects <- dbGetQuery(sqliteDb, paste0(
    "SELECT p.ProjectID, p.ProjectName, c.Name as ClientName
     FROM EmployeeProject ep
     JOIN Project p ON ep.ProjectID = p.ProjectID
     JOIN Client c ON p.ClientID = c.ClientID
     WHERE ep.EmployeeID = ", employee$EmployeeID
  ))
  
  totalAmount <- sum(transactions$Amount)
  n_projects  <- nrow(projects)
  fraction    <- round(1 / n_projects, 4)
  
  # build project splits
  projectSplits <- lapply(1:n_projects, function(i) {
    list(
      projectID   = projects$ProjectID[i],
      projectName = projects$ProjectName[i],
      clientName  = projects$ClientName[i],
      fraction    = fraction,
      subtotal    = round(totalAmount * fraction, 2)
    )
  })
  
  # build transaction list
  txn_list <- lapply(1:nrow(transactions), function(i) {
    list(
      transactionID = transactions$TransactionID[i],
      date          = transactions$Date[i],
      amount        = transactions$Amount[i],
      billable      = transactions$Billable[i]
    )
  })
  
  report <- list(
    employeeID    = employee$EmployeeID,
    employeeName  = paste(employee$FirstName, employee$lastName),
    approverID    = approver$approverID,
    approverName  = approver$name,
    month         = month,
    year          = year,
    totalAmount   = totalAmount,
    transactions  = txn_list,
    projectSplits = projectSplits
  )
  
  return(report)
}


# Building and in serting into Mongo DB --------
# In order : "JAN", "FEB", "MAR" 
months <- c("01", "02", "03")

years  <- c(2026)

employees <- dbGetQuery(sqliteDb, "SELECT * FROM Employee")

for (i in 1:nrow(employees)) {
  employee <- employees[i, ]
  
  for (month in months) {
    for (year in years) {
      
      transactions <- dbGetQuery(sqliteDb, paste0(
        "SELECT * FROM Transactions
         WHERE EmployeeID = ", employee$EmployeeID, "
         AND strftime('%m', Date) = '", month, "'
         AND strftime('%Y', Date) = '", year, "'"
      ))
      
      
      if (nrow(transactions) == 0) next
      
      approver <- approvers[[sample(1:length(approvers), 1)]]
      
      report <- build_expense_report(employee, transactions, approver, month, year)
      
      mongoDb$insert(report)
    }
  }
}


# Checking Insert -------------
cat("Expense reports in MongoDB:", mongoDb$count(), "\n")


# Disconnecting ---------
dbDisconnect(sqliteDb)
mongoDb$disconnect()
