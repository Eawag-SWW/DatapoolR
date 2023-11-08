## -------------------------------------------------------
##
## File: datapoolr.r
##
## April  7, 2017 -- Andreas Scheidegger, andreas.scheidegger@eawag.ch
## Juli 10, 2018 -- Andy Disch, andy.disch@eawag.ch
## 
## -------------------------------------------------------

##' @import RPostgreSQL
NULL

##' @import lubridate
NULL

##' @import utils
NULL

##' @import tidyr
NULL

##' @import ini
NULL

##' DatapoolR - access the datapool of the urban water obervatory
##'
##' This little package simplifies accessing data collected at the urban water
##' observatory. It also provides functions to add comments.
##' @name DatapoolR
##' @author Andreas Scheidegger
##' @docType package
NULL

## define enviroment to store PostgreSQL driver
.pkgglobalenv <- new.env(parent=emptyenv())

## -----------
## load package and method connection

.onAttach <- function(libname, pkgname){
  assign("drv", DBI::dbDriver("PostgreSQL"), envir=.pkgglobalenv)
  ## save old timzone and set to UTC
  assign("oldTZ", Sys.getenv("TZ"), envir=.pkgglobalenv)
  Sys.setenv(TZ="UTC")
  if(.pkgglobalenv$oldTZ != "UTC") packageStartupMessage("Note: DatapoolR has set the timezone for this session to UTC!")
}

.onDetach <- function(libpath) {
  ## set timezone back 
  Sys.setenv(TZ=.pkgglobalenv$oldTZ)
}

##' This function established a connection to the datapool.
##'
##' You must run this function after importing the DatapoolR library to connect
##' to the datapool database.
##' @title Connect to Datapool
##' @param host String, specifying the database server's address 
##' @param port Integer, specifying the port on which the database runs
##' @param database String, stating the database name
##' @param user String, specifying the database user
##' @param password String, specifying the database user's password
##' @return NULL
##' @examples
##' \dontrun{
##' # if defaults have been set with the \code{link[DatapoolR]{setDefaults}} function.
##' init.db.connection()
##' 
##' # connecting without default
##' 
##' host <- "<host-address>"
##' port <- <host-port>
##' database <- "<database-name>"
##' user <- "<database-user-name>"
##' password <- "<database-user-password>"
##' 
##' setDefaults(host,port,database,user,password)
##' 
##' }
##' @author Christian Foerster
##' @export
init.db.connection = function(host=NULL,port=NULL,database=NULL,user=NULL,password=NULL, section=NULL){
  
  if (!is.null(host) & !is.null(port) & !is.null(database) & !is.null(user) & !is.null(password)) {
    
    assign("host", host, envir=.pkgglobalenv)
    assign("port", port, envir=.pkgglobalenv)
    assign("database", database, envir=.pkgglobalenv)
    assign("user", user, envir=.pkgglobalenv)
    assign("password", password, envir=.pkgglobalenv)
    
  }else if(!is.null(host) | !is.null(port) | !is.null(database) | !is.null(user) | !is.null(password)){
    
    stop("Please provide all input arguments or none (if you have set a default connection).")
    
  }else{
    
    .importdefaults(section)
    
  }
  
  print("Congratulations you are now connected to the datapool! :)")
  
}

##' Sends a general SQL query to the datapool
##'
##' Per default only \code{SELECT} queries are allowed (see argument \code{allow.modifications}).
##' All timestamps are converted in \code{POSIXct} objects with time zone \code{UTC}.
##' @title Query datapool
##' @param query a string containing the SQL query
##' @param ... arguments interpolated in the query,
##' see \code{link[DBI]{sqlInterpolate}} for details
##' @param showQuery if \code{TRUE} print SQL query
##' @param allow.modifications FALSE
##' @return a data.frame
##' @examples
##' \dontrun{
##' query.database("select * from source")
##' }
##' @author Andreas Scheidegger
##' @export
query.database <- function(query, ..., showQuery=FALSE, allow.modifications=FALSE){
  on.exit({
    dbDisconnect(con)
  })
  
  
  test <- try({
    con <- DBI::dbConnect(.pkgglobalenv$drv,
                          host = .pkgglobalenv$host, port = .pkgglobalenv$port,
                          user = .pkgglobalenv$user,
                          password = .pkgglobalenv$password,
                          dbname = .pkgglobalenv$database)
  })
  
  if(class(test)=="try-error"){
   stop("Cannot connect to the database. Have you run init.db.connection()? The Database might be down.") 
  }

  ## interpolate query if needed
  qq <- DBI::sqlInterpolate(con, query, ...)   

  if(showQuery) print(qq)

  if(!allow.modifications && !grepl("select", tolower(qq))){ # for queries without SELECT
    stop("Your query is potentially modifying the data base!
If you really want to execute this query run 'query.database' again with 'allow.modifications=TRUE'.")
  }
  
  res <- dbGetQuery(con, qq)
  if(!is.null(res)) res <- force_tz(res, tzone="UTC")            # to ensure UTC time

  return(res)
}

##' Parse time points and guess time format
##' @title Parse time points
##' @param to date string
##' @param from date string
##' @return list of date stings properly parsed
##' @author Andreas Scheidegger
##' @export
parse.dates <- function(from, to) {
  
  tmin <- parse_date_time(from, "ymdHMS", truncated=3, tz = "UTC")
  tmax <- parse_date_time(to, "ymdHMS", truncated=3, tz = "UTC")
  
  if(tmin > tmax) stop("'from' must be smaller than 'to'!")
  
  list(tmin=as.character(tmin), tmax=as.character(tmax))
}

##' Get special values.
##' @title Get special values.
##' @param source.type String with name of source_type
##' @return Integer vector with special values
##' @author adisch
##' @examples
##' \dontrun{
##' source.type = "Hach_Flo-Dar"
##' get.special.values(source.type)
##' }
##' @export
get.special.values <- function(source.type) {
  
  qq <- "SELECT special_value_definition.numerical_value
  FROM source_type
  LEFT JOIN special_value_definition ON source_type.source_type_id = special_value_definition.source_type_id
  WHERE source_type.name=?source_type
  GROUP BY special_value_definition.numerical_value"
  
  res <- unique(query.database(qq, source_type = source.type))
  
  if(nrow(res)==0) stop("No signals found!")
  
  colnames(res) <- c("value")
  
  return(res)
  
}

##' Get source type of source.
##' @title Get source type.
##' @param source.name String with name of source
##' @return Integer vector with name of source type
##' @author adisch
##' @examples
##' \dontrun{
##' source.name = "bf_f04_23_bahnhofstr"
##' get.source.type(source.name)
##' }
##' @export
get.source.type <- function(source.name) {
  
  qq <- "SELECT source_type.name
  FROM source
  LEFT JOIN source_type ON source.source_type_id = source_type.source_type_id
  WHERE source.name=?source_name"
  
  res <- unique(query.database(qq, source_name = source.name))
  
  if(nrow(res)==0) stop("No signals found!")
  
  colnames(res) <- c("value")
  
  return(res)
  
}

##' Get a signal ID.
##'
##' @title Get a signal ID.
##' @param source.name String with name of the source
##' @param parameter.name String with name of the parameter
##' @param from date, ideally in the format \code{1900-01-01 00:00:00}
##' @param to date, ideally in the format \code{2017-01-01 00:00:00}
##' @return data.frame with \code{time} as POSIXct and ID as \code{integer}
##' @author adisch
##' @export
get.signal.ID <- function(source.name,
                        parameter.name,
                        from="1900-01-01 00:00:00",
                        to=as.character(with_tz(now(), "UTC"))) {
  
  times <- parse.dates(from, to)
  tmin <- times$tmin
  tmax <- times$tmax
  
  qq <- "SELECT signal.timestamp, signal_id
  FROM signal 
  INNER JOIN parameter ON signal.parameter_id = parameter.parameter_id
  INNER JOIN source ON signal.source_id = source.source_id
  WHERE parameter.name = ?parameter AND
  source.name = ?source AND
  ?tmin::timestamp <= signal.timestamp AND
  signal.timestamp <= ?tmax::timestamp
  ORDER BY signal.timestamp ASC"
  
  res <- query.database(qq, source = source.name, parameter = parameter.name, tmin=tmin, tmax=tmax)
  if(nrow(res)==0) stop("No signals found!")
  colnames(res) <- c("time", "id")
  
  return(res)
  
}

##' List all types of sources in the datapool
##'
##' @title List all source
##' @return data.frame of all sources in the datapool
##' @author Andreas Scheidegger
##' @examples
##' \dontrun{
##' list.sources()
##' }
##' @export
list.sources <- function(){
  
  qq <- "SELECT srctype.name, src.name, src.serial, srctype.description, src.description
        FROM source_type AS srctype, source AS src
        WHERE src.source_type_id = srctype.source_type_id;"
  
  res <- query.database(qq)
  
  if(nrow(res)==0) stop("No sources found!")
  
  colnames(res) <- c("type", "instance", "serial", "description", "instance.description")
  
  return(res)
  
}

##' List all sites in the datapool.
##'
##' @title List all sites
##' @return data.frame of all sites 
##' @author Andreas Scheidegger
##' ##' \dontrun{
##' list.sites()
##' }
##' @export
list.sites <- function() {
  
  qq <- "SELECT name, coord_x, coord_y, coord_z, street, postcode,
        COUNT(picture.filename), site.description
        FROM site
        LEFT JOIN picture ON site.site_id = picture.site_id
        GROUP BY site.site_id"
  
  res <- query.database(qq)
  
  if(nrow(res)==0) stop("No sites found!")
  
  colnames(res) <- c("name", "coord.x", "coord.y", "coord.z", "street", "postcode",  "n.images", "description")
  
  return(res)
  
}

##' List all unique quality methods in the datapool.
##'
##' @title List all quality methods uniquely
##' @return data.frame of all methods
##' @author Andy Disch
##' @export
list.quality.methods <- function(){
  
  res <- query.database("SELECT DISTINCT method FROM quality")
  
  if(nrow(res) == 0) return(NA)

  colnames(res) <- c("method")
  
  return(res)
  
}

##' List all quality methods with the respective flags in the datapool.
##'
##' @title List all quality methods and flags
##' @return data.frame of all methods and flags
##' @author Andy Disch
##' @export
list.quality.flags <- function(){
  
  res <- query.database("SELECT DISTINCT method, flag FROM quality")
  
  if(nrow(res)==0) stop("No sites found!")
  
  colnames(res) <- c("method", "flag")
  
  return(res)
  
}

##' Get all signals measured at a given site.
##'
##' @title Get signals from a site
##' @param site.name string of the site name
##' @param from date, ideally in the format \code{1900-01-01 00:00:00}
##' @param to date, ideally in the format \code{2017-01-01 00:00:00}
##' @return data.frame containing the signals
##' @author Andreas Scheidegger
##' @examples
##' \dontrun{
##' site = "mysite"
##' from <- "2016-06-22 12"        # zeros are automatically added
##' to <- "2016.06.22 12:10:00"  # parser tries to guess the datetime format
##' signals.at.site(site, from, to)
##' }
##' @export
signals.at.site <- function(site.name,
                            from = "1900-01-01 00:00:00",
                            to = "2019-01-01 00:00:00") {

  times <- DatapoolR:::parse.dates(from, to)
  tmin <- times$tmin
  tmax <- times$tmax

  qq <- "SELECT signal.timestamp, value, unit, parameter.name, source.name, source.serial,
    source_type.name, site.name, count(signals_comments_association.comment_ID), quality.method, quality.flag  
    FROM signal
    INNER JOIN site ON signal.site_id = site.site_id
    INNER JOIN parameter ON signal.parameter_id = parameter.parameter_id
    INNER JOIN source ON signal.source_id = source.source_id
    INNER JOIN source_type ON source.source_type_id = source_type.source_type_id
    LEFT JOIN signals_comments_association ON signal.signal_id = signals_comments_association.signal_id
    LEFT JOIN signals_signal_quality_association ON signals_signal_quality_association.signal_id = signal.signal_id
    LEFT JOIN signal_quality ON signals_signal_quality_association.signal_quality_id = signal_quality.signal_quality_id
    LEFT JOIN quality ON quality.quality_id = signal_quality.quality_id
    WHERE site.name=?site AND
     ?tmin::timestamp <= signal.timestamp AND
     signal.timestamp <= ?tmax::timestamp
    GROUP BY signal.timestamp, signal.value, signal.signal_id, parameter.name, unit, source_type.name, source.name, source.serial, site.name, quality.method, quality.flag 
    ORDER BY signal.timestamp ASC"

  res <- query.database(qq, site=site.name, tmin=tmin, tmax=tmax)

  if(nrow(res)==0) stop("No signals found!")
  
  colnames(res) <- c("time", "value", "unit", "parameter", "source", "serial", "source.type", "site", "n.comments", "quality.flag", "quality.method")
  
  res <- spread(data = res, key = quality.flag, value = quality.method)

  return(res)
  
}

##' Get all or defined signals from a source instance.
##'
##' @title Get all or defined signals from a source instance
##' @param source.name name of source instance
##' @param from date, ideally in the format \code{1900-01-01 00:00:00}
##' @param to date, ideally in the format \code{2017-01-01 00:00:00}
##' @param parameter.name string of parameter name (optional, \code{NULL} by default)
##' @return data.frame containing the signals
##' @author Andreas Scheidegger, Andy Disch
##' @examples
##' \dontrun{
##' source.name = "bf_f04_23_bahnhofstr"
##' from = "2018-07-11 00:00:00"
##' to = "2018-07-11 23:55:00"
##' parameter.name <- "flow rate"
##' signals.of.source(source.name, from, to, parameter.name)
##' }
##' @export
signals.of.source <- function(source.name,
                              from = "1900-01-01 00:00:00",
                              to = "2019-01-01 00:00:00",
                              parameter.name = NULL) {
  
  times <- DatapoolR:::parse.dates(from, to)
  tmin <- times$tmin
  tmax <- times$tmax
  
  if(is.null(parameter.name)) {
    
    qq <- "SELECT signal.timestamp, value, unit, parameter.name, source.name, source.serial,
    source_type.name, site.name, count(signals_comments_association.comment_ID), quality.method, quality.flag  
    FROM signal
    LEFT JOIN site ON signal.site_id = site.site_id
    INNER JOIN parameter ON signal.parameter_id = parameter.parameter_id
    INNER JOIN source ON signal.source_id = source.source_id
    INNER JOIN source_type ON source.source_type_id = source_type.source_type_id
    LEFT JOIN signals_comments_association ON signal.signal_id = signals_comments_association.signal_id
    LEFT JOIN signals_signal_quality_association ON signals_signal_quality_association.signal_id = signal.signal_id
    LEFT JOIN signal_quality ON signals_signal_quality_association.signal_quality_id = signal_quality.signal_quality_id
    LEFT JOIN quality ON quality.quality_id = signal_quality.quality_id
    WHERE source.name=?source_name AND
    ?tmin::timestamp <= signal.timestamp AND
    signal.timestamp <= ?tmax::timestamp
    GROUP BY signal.timestamp, signal.value, signal.signal_id, parameter.name, unit, source_type.name, source.name, source.serial, site.name, quality.method, quality.flag 
    ORDER BY signal.timestamp ASC"

    res <- query.database(qq, source_name = source.name, tmin = tmin, tmax = tmax)

  } else {

    qq <- "SELECT signal.timestamp, value, unit, parameter.name, source.name, source.serial,
    source_type.name, site.name, count(signals_comments_association.comment_ID), quality.method, quality.flag  
    FROM signal
    LEFT JOIN site ON signal.site_id = site.site_id
    INNER JOIN parameter ON signal.parameter_id = parameter.parameter_id
    INNER JOIN source ON signal.source_id = source.source_id
    INNER JOIN source_type ON source.source_type_id = source_type.source_type_id
    LEFT JOIN signals_comments_association ON signal.signal_id = signals_comments_association.signal_id
    LEFT JOIN signals_signal_quality_association ON signals_signal_quality_association.signal_id = signal.signal_id
    LEFT JOIN signal_quality ON signals_signal_quality_association.signal_quality_id = signal_quality.signal_quality_id
    LEFT JOIN quality ON quality.quality_id = signal_quality.quality_id
    WHERE source.name=?source_name AND
      ?tmin::timestamp <= signal.timestamp AND
      signal.timestamp <= ?tmax::timestamp AND
      parameter.name = ?parameter_name
    GROUP BY signal.timestamp, signal.value, signal.signal_id, parameter.name, unit, source_type.name, source.name, source.serial, site.name, quality.method, quality.flag
    ORDER BY signal.timestamp ASC"

    res <- query.database(qq, source_name = source.name, tmin = tmin, tmax = tmax, parameter_name = parameter.name)
    
  }

  if(nrow(res)==0) stop("No signals found!")
  
  colnames(res) <- c("time", "value", "unit", "parameter", "source", "serial", "source.type", "site", "n.comments", "quality.flag", "quality.method")
  
  res <- spread(data = res, key = quality.flag, value = quality.method)
  
  if('<NA>' %in% names(res)) res <- res[ , !(names(res) %in% '<NA>')]

  return(res)
  
}

##' Get all signals from a source type.
##'
##' @title Get all signals from a source type
##' @param source.type name of source type
##' @param from date, ideally in the format \code{1900-01-01 00:00:00}
##' @param to date, ideally in the format \code{2017-01-01 00:00:00}
##' @return data.frame containing the signals
##' @author Andreas Scheidegger
##' @examples
##' \dontrun{
##' source.type = "Hach_Flo-Dar"
##' signals.of.sourcetype(source.name)
##' }
##' @export
signals.of.sourcetype <- function(source.type,
                                  from="1900-01-01 00:00:00",
                                  to = "2019-01-01 00:00:00") {

  times <- DatapoolR:::parse.dates(from, to)
  tmin <- times$tmin
  tmax <- times$tmax

  qq <- "SELECT signal.timestamp, value, unit, parameter.name, source.name, source.serial,
    source_type.name, site.name, count(signals_comments_association.comment_ID), quality.method, quality.flag  
    FROM signal
    LEFT JOIN site ON signal.site_id = site.site_id
    INNER JOIN parameter ON signal.parameter_id = parameter.parameter_id
    INNER JOIN source ON signal.source_id = source.source_id
    INNER JOIN source_type ON source.source_type_id = source_type.source_type_id
    LEFT JOIN signals_comments_association ON signal.signal_id = signals_comments_association.signal_id
    LEFT JOIN signals_signal_quality_association ON signals_signal_quality_association.signal_id = signal.signal_id
    LEFT JOIN signal_quality ON signals_signal_quality_association.signal_quality_id = signal_quality.signal_quality_id
    LEFT JOIN quality ON quality.quality_id = signal_quality.quality_id
    WHERE source_type.name=?source_type AND
      ?tmin::timestamp <= signal.timestamp AND
      signal.timestamp <= ?tmax::timestamp
    GROUP BY signal.timestamp, signal.value, signal.signal_id, parameter.name, unit, source_type.name, source.name, source.serial, site.name, quality.method, quality.flag
    ORDER BY signal.timestamp ASC"
  
  res <- query.database(qq, source_type = source.type, tmin = tmin, tmax = tmax)

  if(nrow(res)==0) stop("No signals found!")
  
  colnames(res) <- c("time", "value", "unit", "parameter", "source", "serial", "source.type", "site", "n.comments", "quality.flag", "quality.method")
  
  res <- spread(data = res, key = quality.flag, value = quality.method)
  
  return(res)
  
}

##' Get the most recent signals
##'
##' @title Get the last signals 
##' @param n number of signals
##' @param recent.on.top if \code{TRUE} the most recent signal is on
##' top of data.frame, if \code{FALSE} at the end.
##' @return data.frame containing the signals
##' @author Andreas Scheidegger
##' @examples
##' \dontrun{
##' newest.signals(4)
##'
##' newest.signals(4, FALSE)
##'}
##' @export
newest.signals <- function(n, recent.on.top=TRUE){

  qq <- "SELECT signal.timestamp, value, unit, parameter.name, source.name, source.serial,
                source_type.name, count(signals_comments_association.comment_ID)  
         FROM signal 
            INNER JOIN site ON signal.site_id = site.site_id
            INNER JOIN parameter ON signal.parameter_id = parameter.parameter_id
            INNER JOIN source ON signal.source_id = source.source_id
            INNER JOIN source_type ON source.source_type_id = source_type.source_type_id
            LEFT JOIN signals_comments_association ON signal.signal_id = signals_comments_association.signal_id
         GROUP BY signal.timestamp, signal.value, signal.signal_id, parameter.name, unit, source_type.name, source.name, source.serial
         ORDER BY signal.timestamp DESC
         LIMIT ?n"
  res <- query.database(qq, n=n)

  if(nrow(res)==0) stop("No signals found!")
  
  if(!recent.on.top){
    res <- res[rev(row.names(res)),]
  }
  colnames(res) <- c("time", "value", "unit", "parameter", "source", "serial",
                     "source.type", "n.comments")
  
  return(res)
}

##' Get the latest signal from each source.
##'
##' @title Get the latest signal from each source
##' @param recent.on.top if \code{TRUE} the most recent signal is on
##' top of data.frame, if \code{FALSE} at the end.
##' @return data.frame containing the signals
##' @author Lukas Keller
##' @examples
##' \dontrun{
##' last.signals()
##' }
##' @export
last.signals <- function(recent.on.top=TRUE){
  
  qq <- "SELECT source.name, source.serial, MAX(signal.timestamp)
    FROM signal 
  INNER JOIN source ON signal.source_id = source.source_id
  GROUP BY source.name, source.serial
  ORDER BY MAX(signal.timestamp) ASC"
  
  res <- query.database(qq)
  
  if(nrow(res)==0) stop("No signals found!")

  colnames(res) <- c("source", "serial", "time")
  
  return(res)
}

##' Add quality for new method.
##'
##' @title Add quality for new method.
##' @param method string with name of applied method
##' @param flags vector of strings with flags (\code{green} madatory)
##' @param promt logical \code{TRUE} to promt confirmation, \code{FALSE} to avoid confirmation prompt 
##' @return NULL
##' @author adisch
##' @examples
##' \dontrun{
##' method = "rangeCheck"
##' flags = c("green", "orange", "red")
##' add.quality.method(method, flags)
##' }
##' @export
add.quality.method <- function(method, flags, promt = TRUE) {
  
  if(!any(flags == "green")) stop("global default flag \"green\" missing!")
  if(any(duplicated(flags))) stop("set unique quality flags!")

  maxID <- query.database("SELECT MAX(quality_id) FROM quality", showQuery=F)
  maxID[is.na(maxID)] <- 0
  
  quality.df <- data.frame(quality_id = (as.numeric(maxID) + 1):(as.numeric(maxID) + length(flags)),
                           flag = flags,
                           method = method)
  
  if(promt){
    ans <- readline(paste0("Add quality '", flags, "' to ", method, "?\n yes/no \n"))
  }
  if(!promt || (substring(tolower(ans),1,1) == "y")) {
    on.exit({
      dbDisconnect(con)
    })
    
    con <- dbConnect(.pkgglobalenv$drv,
                     host = .pkgglobalenv$host, port = .pkgglobalenv$port,
                     user = .pkgglobalenv$user, 
                     password = .pkgglobalenv$password, 
                     dbname = .pkgglobalenv$database)
    
    dbWriteTable(con, "quality", quality.df, append=TRUE, row.names=FALSE)
    print("quality added")
  } else {
    print("aborted")
  }
  
}

##' Add signal quality to signal.
##'
##' @title Add signal quality to signal.
##' @param source string with name of the source
##' @param parameter string with name of the parameter
##' @param method string with name of method
##' @param flags string vector with flagging
##' @param from date, ideally in the format \code{1900-01-01 00:00:00}
##' @param to date, ideally in the format \code{2017-01-01 00:00:00}
##' @param author string with responsible person
##' @param promt logical \code{TRUE} to promt confirmation, \code{FALSE} to avoid confirmation prompt 
##' @return NULL
##' @author adisch
##' @examples
##' \dontrun{
##' source.name = "bf_f04_23_bahnhofstr"
##' parameter.name = "flow rate"
##' method = "rangeCheck"
##' flags = c("green", "green", "orange", "green", "red", "green")
##' from = "2018-02-01 12:00:00"
##' to = "2018-02-01 12:25:00"
##' author = "adisch"
##' add.quality.flags(source.name, parameter.name, method, flags, tmin, tmax, author, promt = TRUE)
##' }
##' @export
add.quality.flags <- function(source,
                             parameter,
                             method,
                             flags,
                             from,
                             to,
                             author,
                             promt = TRUE){
  
  qq <- "SELECT quality_id, flag
  FROM quality 
  WHERE method = ?method"
  
  quality.id <- query.database(qq, method = method)
  
  if(length(quality.id$quality_id)==0) stop("No matching quality found!")
  if(!any(flags %in% quality.id$flag)) stop("Uninitialized flag found! Associate flag with method by applying the add.quality.method() function.")

  # add signal_quality
  qq <- "SELECT signal_quality_id
         FROM signal_quality
         INNER JOIN quality ON signal_quality.quality_id = quality.quality_id
         WHERE timestamp = ?timestamp AND
         author = ?author AND
         method = ?method"
  
  res <- query.database(qq, timestamp = as.character(round(Sys.time(), units = "days")), author = author, method = method)
  
  if(length(res$signal_quality_id) == 0) {
  
    maxID <- query.database("SELECT MAX(signal_quality_id) FROM signal_quality", showQuery = FALSE)
    maxID[is.na(maxID)] <- 0
  
    signal.quality.df <- data.frame(signal_quality_id = seq(as.numeric(maxID) + 1, as.numeric(maxID) + length(quality.id$flag)),
                                    quality_id = quality.id$quality_id,
                                    timestamp = rep(round(Sys.time(), units = "days"), length(quality.id$flag)),
                                    author = rep(author, length(quality.id$flag)))
    
  } else {
    
    qq <- "SELECT signal_quality_id, signal_quality.quality_id, timestamp, author
         FROM signal_quality
    INNER JOIN quality ON signal_quality.quality_id = quality.quality_id
    WHERE timestamp = ?timestamp AND
    author = ?author AND
    method = ?method"
    
    signal.quality.df <- query.database(qq, timestamp = as.character(round(Sys.time(), units = "days")), author = author, method = method)
    
  }

  # add quality 
  match.qualit.id.to.flag <- rep(NA, length(flags))
  
  for(i in 1:length(quality.id$flag)) {
    tmp <- quality.id$flag[i]
    match.qualit.id.to.flag[flags == tmp] <- signal.quality.df$signal_quality_id[quality.id$flag == tmp]
  }

  # add signals_signal_quality_association
  signal.df <- get.signal.ID(source, parameter, from, to)
  signals.signal.quality.association <- as.data.frame(cbind(signal_id = signal.df$id, signal_quality_id = as.numeric(match.qualit.id.to.flag)))
  
  if(promt){
    ans <- readline(paste0(" Add signal quality? \n Add flag quality? \n yes/no \n"))
  }
  if(!promt || (substring(tolower(ans),1,1) == "y")) {
    on.exit({
      dbDisconnect(con)
    })
    con <- dbConnect(.pkgglobalenv$drv,
                     host = .pkgglobalenv$host, port = .pkgglobalenv$port,
                     user = .pkgglobalenv$user, 
                     password = .pkgglobalenv$password, 
                     dbname = .pkgglobalenv$database)
    
    if(length(res$signal_quality_id) == 0) {
      dbWriteTable(con, "signal_quality", signal.quality.df, append=TRUE, row.names=FALSE)
      print("signal quality added")
    }
    
    dbWriteTable(con, "signals_signal_quality_association", signals.signal.quality.association, append=TRUE, row.names=FALSE)
    print("flag added")
    
  } else {
    print("aborted")
  }
  
}

##' Add new comment.
##'
##' @title Add new comment to comment table.
##' @param comment string with comment text
##' @param author string with name of the author
##' @param promt logical \code{TRUE} to promt confirmation, \code{FALSE} to avoid confirmation prompt 
##' @return NULL
##' @author adisch
##' @examples
##' \dontrun{
##' add.new.comment("offset correction to 1450mm", "adisch")
##' }
##' @export
add.new.comment <- function(comment, author, promt = TRUE) {
  
  maxID <- query.database("SELECT MAX(comment_id) FROM comment", showQuery = FALSE)
  maxID[is.na(maxID)] <- 0
  
  comment.df <- data.frame(
    comment_id = as.numeric(maxID) + 1,
    text = comment,
    timestamp = round(Sys.time(), units = "days"),
    author = author,
    stringsAsFactors = FALSE
  )
  
  if(promt){
    ans <- readline(paste0("Add comment '", comment, "?\n yes/no \n"))
  }
  if(!promt || (substring(tolower(ans),1,1) == "y")) {
    on.exit({
      dbDisconnect(con)
    })
    
    con <- dbConnect(.pkgglobalenv$drv,
                     host = .pkgglobalenv$host, port = .pkgglobalenv$port,
                     user = .pkgglobalenv$user, 
                     password = .pkgglobalenv$password, 
                     dbname = .pkgglobalenv$database)
    
    dbWriteTable(con, "comment", comment.df, append = TRUE, row.names = FALSE)
    print("comment added")
  } else {
    print("aborted")
  }
  
}

##' Add comment.
##'
##' @title Associate comment to signal
##' @param comment_id numeric with comment id
##' @param signal_id numeric with signal id
##' @param promt logical \code{TRUE} to promt confirmation, \code{FALSE} to avoid confirmation prompt 
##' @return NULL
##' @author adisch
##' @examples
##' \dontrun{
##' add.comment()
##' }
##' @export
add.comment <- function(comment_id, signal_id, promt = TRUE) {
  
  comment.df <- data.frame(
    comment_id = comment_id,
    signal_id = signal_id,
    stringsAsFactors = FALSE
  )
  
  if(promt){
    ans <- readline(paste0("Add comment '", comment_id, "to ", signal_id,"?\n yes/no \n"))
  }
  if(!promt || (substring(tolower(ans),1,1) == "y")) {
    on.exit({
      dbDisconnect(con)
    })
    
    con <- dbConnect(.pkgglobalenv$drv,
                     host = .pkgglobalenv$host, port = .pkgglobalenv$port,
                     user = .pkgglobalenv$user, 
                     password = .pkgglobalenv$password, 
                     dbname = .pkgglobalenv$database)
    
    dbWriteTable(con, "signals_comments_association", comment.df, append = TRUE, row.names = FALSE)
    print("comment_id and signal_id linked")
  } else {
    print("aborted")
  }
  
}