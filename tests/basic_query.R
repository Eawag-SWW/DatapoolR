library(DatapoolR)

init.db.connection()

data = query.database("Select * from quality;", showQhery=TRUE)


print(data)