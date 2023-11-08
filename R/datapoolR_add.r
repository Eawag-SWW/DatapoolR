##' @import dygraphs
NULL

##' @import xts
NULL

##' idea: generic plotting function to visualize DatapoolR outputs
##' 
##' @title DatapoolR plots
##' @param signal data.frame containing the signals, \code{DatapoolR} structure
##' @param show.red logical if red values are shown \code{TRUE} (as default) or not \code{FALSE}
##' @return list.of.graphs a list of dygraph objects
##' @author adisch
##' @examples
##' \dontrun{
##' PlotDatapoolR(signal)
##' }
##' @export
PlotDatapoolR <- function(signal, show.red = TRUE){
  
  list.of.dygraphs <- list()
  
  if(ncol(signal) > 9) {
    
    methods <- names(signal[ ,10:ncol(signal), drop = FALSE])
    
    for(i in 1:length(methods)) {
      
      data <- xts(signal$value, order.by = signal$time)
      
      signal.red <- signal[ ,9+i]
      
      if(!any(!is.na(signal.red))) next
      
      for(iii in 1:length(signal$time)) {
        if(!is.na(signal[iii,9+i])) {
          if(signal[iii,9+i] == "red") {
            signal.red[iii] <- signal$value[iii] 
          } else {
            signal.red[iii] <- NA
          }
        } else {
          signal.red[iii] <- NA
        }
      }
        
      data <- merge(data, signal.red)
      
      if(show.red) {

        graph <- dygraph(data) %>%
          dyOptions(useDataTimezone = TRUE) %>% 
          dyRangeSelector() %>%
          dyLegend(width = 400) %>%
          dyOptions(axisLineWidth = 1.5) %>% 
          dySeries("signal.red",
                   label = methods[i],
                   color = "red",
                   drawPoints = TRUE,
                   strokeWidth = 0,
                   pointSize = 5) %>% 
          dySeries("data", color = "black")
        
        for(ii in 1:length(signal$time)) {
          if(!is.na(signal[ii,9+i])) {
          
            if(signal[ii,9+i] == "red") graph <- graph %>% dyAnnotation(signal$time[ii], text = methods[i], tooltip = methods[i])
          
          }
          
        }
        
      } else {
        
        for(iiii in 1:length(signal$time)) {
          
          if(!is.na(signal[iiii,9+i])) {
            if(signal[iiii,9+i] == "red") {
              signal$value[iiii] <- NA 
            }
            
          }
          
        }
        
        data <- xts(signal$value, order.by = signal$time)
        
        graph <- dygraph(data) %>%
          dyOptions(useDataTimezone = TRUE) %>% 
          dyRangeSelector() %>%
          dyLegend(width = 400) %>%
          dyOptions(axisLineWidth = 1.5) %>% 
          dySeries("V1", color = "black")
        
      }

      list.of.dygraphs[[i]] <- graph
      
    }
    
  } else {
    
    list.signal <- split(signal, signal$parameter)
    
    for(i in 1:length(list.signal)) {
      
      tmp <- list.signal[[i]]
      
      list.of.dygraphs[[i]] <- dygraph(xts(tmp$value, order.by = tmp$time), main = paste0(tmp$source[1],": ",tmp$parameter[1])) %>%
        dyOptions(useDataTimezone = TRUE) %>% 
        dyRangeSelector() %>%
        dyLegend(width = 400) %>%
        dyOptions(axisLineWidth = 1.5) %>% 
        dySeries("V1", color = "black")

    }
    
  }
  
  return(list.of.dygraphs)
  
}