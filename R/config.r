##' @import RPostgreSQL
NULL

# place where connection details will be saved
connfile = paste0(path.expand("~"),"/.datapoolaccess")
usrconnfile = paste0(path.expand("~"),"/R/.datapoolaccess")

# check whether the users answer is valid
.check_validity <- function(option){
  option = tolower(option)
  do <- FALSE
  if(option == "y" | option == "yes"){
    do <- TRUE
  }else if(option == "n" | option == "no"){
    do <- FALSE
  }else{
    print("Invalid option -> y,n,yes,no are valid options.")
  }
  return(do)
}

# reading defaults from file setting them as global variables
.importdefaults <- function(section){
  
  if (is.null(section)){
    section = "DEFAULT"
  }
  
  if (!file.exists(connfile)){
    stop("Please provide all input arguments <host, port, database, user, password> or use the setDefaults function to set a default connection.")
  }else{
    
    readconn <- try({
      # read conn details
      connectiondetails <- read.ini(connfile)
    })
    
    if(class(readconn)=="try-error"){
      
      connectiondetails <- read.ini(usrconnfile)
      
    }
    
    # create varaibles
    .conn_details = connectiondetails[[section]]
    .names = names(.conn_details)
    
    for (i in 1:length(.names)){
      assign(.names[[i]], .conn_details[[i]], envir=.pkgglobalenv)
    }
  }
}


##' This function is able to set default connection parameters.
##' @title Set Defaults
##' @param host String, specifying the database server's address 
##' @param port Integer, specifying the port on which the database runs
##' @param database String, stating the database name
##' @param user String, specifying the database user
##' @param password String, specifying the database user's password
##' @return String, stating if setting was possible.
##' @author Christian Foerster
##' @export
setDefaults <- function(host,port,database,user,password,instance=NULL,overwrite=FALSE){
  
  set_defaults = "yes"
  if (is.null(instance)){
    instance="DEFAULT"
  }
  
  if (file.exists(connfile)){
    if(overwrite){
        
    }else{
        set_defaults = readline(prompt="Some defaults have already been set, are you sure you want to replace them? [y/n] ")
    }
  }
  do = .check_validity(set_defaults)
  
  if (do){
    
    
    test <- try({
      con <- DBI::dbConnect(DBI::dbDriver("PostgreSQL"),
                             host = host, port = port,
                             user = user,
                             password = password,
                             dbname = database)
       DBI::dbDisconnect(con)
      })

    if(class(test)=="try-error"){
      
      print(paste("\nNo connection to the datapool could be established. Please check your default values. Other reasons might me a missing internet connection/vpn connection. Are you in the right network? Have you passed the arguments in the right order? [host, port, database, user, password]"))
      return("defaults not set")
      
    } else {
      
      connstring = list()
      connstring[[instance]] = list(
        host = host,
        port = port,
        database = database, 
        user = user,
        password = password
      )
      
      writeconn <- try({
        write.ini(connstring, connfile)
      })
      
      if(class(writeconn)=="try-error"){
        
        if (!dir.exists("/R")){dir.create(paste0(path.expand("~"),"/R"))}
        write.ini(connstring, usrconnfile)
      }
      
      print(paste0("Access file has been created at ",connfile))
      return("defaults set")
      
    }
    
    
  }else{
    return("defaults not set")
  }
  
}

##' This function is able to remove previously set default connection parameters.
##' @title Remove Defaults
##' @return String, stating if removing was possible.
##' @author Christian Foerster
##' @export
removeDefaults <- function(){
  
  if(file.exists(connfile)){
    
    remove.defaults <- readline(prompt="Are you sure you want to remove current defaults? [y/n] ")
  
  }else{
  
    return(paste0("No defaults found at: ",connfile))
      
  }

  do <- .check_validity(remove.defaults)
  
  if (do){
    
    file.remove(connfile)
    return("defaults removed")
    
  }
}


