#' Extracts commercial data from Database (Stockeff)
#'
#' Replicates code in get_comland_raw_data but pulls from stockeff instead of woland/wodet/cfdet
#' 
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

get_stockeff_raw_data_stripped<- function(channel,filterByYear=1994){
  
  #If not specifying a year default to 1964 - 2019
  filterbyArea <- NA
  
  message(paste0("Pulling landings data from ",
                 filterByYear[1], " to ", filterByYear[length(filterByYear)],
                 ". This could take a while (> 1 hour) ... "))

  #output objects
  comland <- c()
  for (iyear in head(filterByYear,1):tail(filterByYear,1)) {
    message("Pulling data from year = ",iyear," ...")
    landings.qry <- paste("select year, month, negear, toncl2, nespp3, area,
                           spplivlb, spplndlb, sppvalue, utilcd
                           from stockeff.mv_cf_landings where YEAR = ",iyear)
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
                                                        "TONCL2","NESPP3","AREA",
                                                        "UTILCD")]
    #value
    comland.yr[, V2 := sum(SPPVALUE, na.rm = T), by = c("YEAR","MONTH","NEGEAR",
                                                        "TONCL2","NESPP3","AREA",
                                                        "UTILCD")]
    
    #Remove extra rows/columns
    comland.yr <- unique(comland.yr, by = data.table::key(comland.yr))
    comland.yr[, c('SPPLIVLB', 'SPPLNDLB', 'SPPVALUE') := NULL]
    
    #Rename summed columns
    data.table::setnames(comland.yr, c('V1', 'V2'), c('SPPLIVLB', 'SPPVALUE'))
    
    comland <- data.table::rbindlist(list(comland, comland.yr))
    
  }

  
  #Convert number fields from chr to num
  numberCols <- c('YEAR', 'MONTH', 'NEGEAR', 'TONCL2', 'NESPP3', 'UTILCD', 'AREA'  )
  comland[, (numberCols):= lapply(.SD, as.numeric), .SDcols = numberCols][]

  #Adjust pounds to metric tons
  comland[, SPPLIVMT := SPPLIVLB * 0.00045359237]
  comland[, SPPLIVLB := NULL]

  return(comland = comland[])
}