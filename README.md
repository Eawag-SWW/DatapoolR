# DatapoolR

A package to simplify accessing the Urban Water Observatory datapool with R.

## Installation

To install the package from Gitlab we need `devtools`. If you have problems try to run R as admin for the installation.

1. `install.packages(c("devtools", "RPostgreSQL", "lubridate", "magrittr"))`
2. install `DatapoolR` from the Eawag Gitlab server:
```R
library(devtools)
install_git("https://gitlab.switch.ch/andy.disch/DatapoolR.git")
```

## Setting a default connection

This step has to be completed once only. The default connection will be saved for later use.

```R
library(DatapoolR)

host <- "eaw-sdwh1.eawag.wroot.emp-eaw.ch"
port <- 5432
database <- "datapool"
user <- "datapool"
password <- "UWO_database"

setDefaults(host,port,database,user,password)

```

## Usage

Most commands should be self-explanatory. For Details see the package documentation.

Some examples
```R
library(DatapoolR)

# connect to the datapool
init.db.connection()


## you can send arbitray SQL queries for maximal flexibility
query.database("SELECT * FROM source_type")

## or use a convinient wrapper function
list.sources()

list.sites()

list.quality.methods()

list.quality.flags()

signals.at.site(site="F04-2", from="2016-06-22 12", to="2016.06.22 12:10:00")

signals.of.source(source="Nivus-POA3-bahnhofstr", from="2016-06-22 12", to="2016.06.22 12:10:00")

signals.of.source(source.name ="bf_f04_23_bahnhofstr", from="2018-02-01 11:50:00", to="2018-02-01 12:10:00", parameter.name = "flow rate")

signals.of.sourcetype(source="Nivus-POA4", from="2016-06-22 12", to="2016.06.22 12:10:00")

newest.signals(5)

last.signals()

add.quality.flags(source ="bf_f04_23_bahnhofstr", parameter = "flow rate", method = "rangeCheck", flags = c("green", "green", "orange", "green", "red", "green"), from="2018-02-01 12:00:00", to="2018-02-01 12:25:00", author = "dischand")

add.quality.method(method = "rangeCheck", flags = c("green", "orange", "red"))

get.special.values(source.type = unlist(get.source.type("bf_f04_23_bahnhofstr")))

get.source.type(source.name = "bf_f04_23_bahnhofstr")

```
