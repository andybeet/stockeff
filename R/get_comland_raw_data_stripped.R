#' Extracts commercial data from Database
#'
#' Connects to cfdbs and pulls fields from WOLANDS, WODETS, CFDETS
#'
#'@param channel an Object inherited from \link[DBI]{DBIConnection-class}. This object is used to connect
#' to communicate with the database engine. (see \code{\link[dbutils]{connect_to_database}})
#'@param endyear Numeric Scalar. Final year of query.
#'@param landed Character String. Use landed weight ("y" - meatwt) for scallops and clams instead of live weight ("n" - livewt).
#'@param out.dir path to directory where final output will be saved
#'
#'
#'@return Data frame (data.table) (n x 10)
#'Each row of the data.table represents a species record for a given tow/trip
#'
#'\item{YEAR}{Year of trip/tow}
#'\item{MONTH}{Month of trip/tow}
#'\item{NEGEAR}{Fishing gear used on trip/tow}
#'\item{TONCL1}{Tonnage class of the fishing vessel}
#'\item{NESPP3}{Species code (3 charachters)}
#'\item{NESPP4}{Species code and market code (4 characters)}
#'\item{AREA}{Statistical area in which species was reportly caught}
#'\item{UTILCD}{Utilization code}
#'\item{SPPLIVLB}{live weight (landed = "n") or landed weight (landed="y") in lbs}
#'\item{SPPVALUE}{The value of landed catch to the nearest dollar (U.S.), paid to fisherman by dealer, for a given species.}
#'
#'@section File Creation:
#'
#'A file containing the data.table above will also be saved to the users machine in the directory provided
#'
#'@export

get_comland_raw_data <- function(channel, filterByYear = 1994){
  
  #If not specifying a year default to 1964 - 2019
  filterByArea <- NA
  
  message(paste0("Pulling landings data from ",
                 filterByYear[1], " to ", filterByYear[length(filterByYear)],
                 ". This could take a while (> 1 hour) ... "))
  



  tables <- as.numeric(c(substr(filterByYear[which(filterByYear <= 1993)], 3, 4),
                         filterByYear[which(filterByYear >  1993)]))
   tables[which(tables > 1993 & tables <= 2019)] <- 
    paste0('CFDETS', tables[which(tables > 1993 & tables <= 2019)], 'AA')

  #output objects
comland <- c()
  for(itab in 1:length(filterByYear)){

      #only post 1993
     #Data query

      trip.table <- paste0('CFDETT',  filterByYear[itab], 'AA')

      landings.qry <- paste("select a.year, a.month, a.negear, a.toncl2, a.nespp3, 
                            a.area, a.spplivlb, a.spplndlb, a.sppvalue, 
                           a.utilcd    from", tables[itab], "a,", trip.table, "b
                           where a.link = b.link")

      comland.yr <- data.table::as.data.table(DBI::dbGetQuery(channel, landings.qry))

  #Sum landings and value
    data.table::setkey(comland.yr,
                       YEAR,
                       MONTH,
                       NEGEAR,
                       TONCL2,
                       NESPP3,
                       AREA,
                       UTILCD)
    #landings
    comland.yr[, V1 := sum(SPPLIVLB, na.rm = T), by = c("YEAR","MONTH","NEGEAR",
                                                        "TONCL2","NESPP3","AREA","UTILCD")]
    #value
    comland.yr[, V2 := sum(SPPVALUE, na.rm = T), by =c("YEAR","MONTH","NEGEAR",
                                                       "TONCL2","NESPP3","AREA","UTILCD")]
    
    #Remove extra rows/columns
    comland.yr <- unique(comland.yr, by = c("YEAR","MONTH","NEGEAR",
                                            "TONCL2","NESPP3","AREA","UTILCD"))
    comland.yr[, c('SPPLIVLB', 'SPPLNDLB', 'SPPVALUE') := NULL]
    
    #Rename summed columns
    data.table::setnames(comland.yr, c('V1', 'V2'), c('SPPLIVLB', 'SPPVALUE'))
    
    comland <- data.table::rbindlist(list(comland, comland.yr))
    
    message("Pulled data from ",tables[itab]," ...")
    
  }
  
  #Convert number fields from chr to num
  numberCols <- c('YEAR', 'MONTH', 'NEGEAR', 'TONCL2', 'NESPP3', 'UTILCD', 'AREA'      )
  
  comland[, (numberCols):= lapply(.SD, as.numeric), .SDcols = numberCols][]
  
  #Adjust pounds to metric tons
  comland[, SPPLIVMT := SPPLIVLB * 0.00045359237]
  comland[, SPPLIVLB := NULL]
  
  #standardize YEAR field
  comland[YEAR < 100, YEAR := YEAR + 1900L]
  
  return(comland = comland[])
}