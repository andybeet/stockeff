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

get_comland_raw_data <- function(channelSole, channelNova, filterByYear = NA, 
                                 filterByArea = NA, useLanded = T, removeParts = T){
  
  #If not specifying a year default to 1964 - 2019
  if(is.na(filterByYear[1])) filterByYear <- 1964:2019
  
  message(paste0("Pulling landings data from ",
                 filterByYear[1], " to ", filterByYear[length(filterByYear)],
                 ". This could take a while (> 1 hour) ... "))
  
  #Generate vector of tables to loop through
  if(any(filterByYear < 1964)) stop("Landings data start in 1964")
  
  #CAMS tables are on NEFSC_USER while the rest are on sole
  channels <- rep('sole', length(filterByYear))
  channels[which(filterByYear > 2019)] <- 'nova'
  
  #Name tables
  tables <- as.numeric(c(substr(filterByYear[which(filterByYear <= 1993)], 3, 4),
                         filterByYear[which(filterByYear >  1993)]))
  tables[which(tables > 2019)] <- "CAMS_GARFO.CAMS_LAND"
    #paste0('CAMS_GARFO.CAMS_LAND', tables[which(tables > 2019)], 'AA')
  tables[which(tables > 1993 & tables <= 2019)] <- 
    paste0('CFDETS', tables[which(tables > 1993 & tables <= 2019)], 'AA')
  tables[which(tables > 63 & tables <= 81)] <- paste0('WOLANDS', tables[which(tables > 63 & tables <= 81)])
  tables[which(tables > 81 & tables <= 93)] <- paste0('WODETS',  tables[which(tables > 81 & tables <= 93)])
  
  #output objects
  comland <- c()
  sql <- c()
  
  for(itab in 1:length(tables)){
    #Use correct connection
    if(channels[itab] == 'sole') channel <- channelSole else channel <- channelNova
    
    #Data query
    #Need to add mesh data post 1981
    if(substr(tables[itab], 1, 3) == 'WOL'){
      landings.qry <- paste("select year, month, negear, toncl2, nespp3, nespp4, area,
                           spplivlb, spplndlb, sppvalue, utilcd
                           from", tables[itab])
      if(!is.na(filterByArea[1])){
        landings.qry <- paste0(landings.qry, " where area in (", survdat:::sqltext(filterByArea), ")
                               order by area")
      }
      comland.yr <- data.table::as.data.table(DBI::dbGetQuery(channel, landings.qry))
      comland.yr[, MESH := 5] #Identify all as large mesh
    } else {
      if(filterByYear[itab] > 2019){
        trip.table <- "CAMS_GARFO.CAMS_LAND"
      }
      if(filterByYear[itab] > 1993 & filterByYear[itab] <= 2019){
        trip.table <- paste0('CFDETT',  filterByYear[itab], 'AA')
      } 
      if(filterByYear[itab] > 1981 & filterByYear[itab] <= 1993){
        trip.table <- paste0('WODETT',  substr(filterByYear[itab], 3, 4))
      }
      landings.qry <- paste("select a.year, a.month, a.negear, a.toncl2, a.nespp3, 
                           a.nespp4, a.area, a.spplivlb, a.spplndlb, a.sppvalue, 
                           a.utilcd, b.mesh
                           from", tables[itab], "a,", trip.table, "b
                           where a.link = b.link")
      if(!is.na(filterByArea[1])){
        landings.qry <- paste0(landings.qry, " and a.area in (", survdat:::sqltext(filterByArea), ")
                               order by area")
      }
      comland.yr <- data.table::as.data.table(DBI::dbGetQuery(channel, landings.qry))
    }
    sql <- c(sql, landings.qry)
    
    #Identify small/large mesh fisheries
    comland.yr[MESH <= 3, MESHCAT := 'SM']
    comland.yr[MESH >  3, MESHCAT := 'LG']
    comland.yr[, MESH := NULL]
    
    # Use landed weight instead of live weight for shellfish
    if(useLanded) {comland.yr[NESPP3 %in% 743:800, SPPLIVLB := SPPLNDLB]}
    
    # Remove fish parts so live weight is not double counted
    if(removeParts){
      comland.yr <- comland.yr[!NESPP4 %in% c('0119', '0123', '0125', '0127', 
                                              '0812', '0819', '0828', '0829', 
                                              '1731', '2351', '2690', '2699', 
                                              '3472', paste0(348:359, 8), 
                                              '3868', paste0(469:471, 4),
                                              paste0(480:499, 8), '5018', 
                                              '5039', '5261', '5265'), ]
    }
    
    #Sum landings and value
    data.table::setkey(comland.yr,
                       YEAR,
                       MONTH,
                       NEGEAR,
                       MESHCAT,
                       TONCL2,
                       NESPP3,
                       AREA,
                       UTILCD)
    #landings
    comland.yr[, V1 := sum(SPPLIVLB, na.rm = T), by = c("YEAR","MONTH","NEGEAR","MESHCAT",
                                                        "TONCL2","NESPP3","AREA","UTILCD")]
    #value
    comland.yr[, V2 := sum(SPPVALUE, na.rm = T), by =c("YEAR","MONTH","NEGEAR","MESHCAT",
                                                       "TONCL2","NESPP3","AREA","UTILCD")]
    
    #Create market category
    comland.yr[, MKTCAT := substr(NESPP4, 4, 4)]
    
    #Remove extra rows/columns
    comland.yr <- unique(comland.yr, by = c("YEAR","MONTH","NEGEAR","MESHCAT",
                                            "TONCL2","NESPP3","AREA","UTILCD"))
    comland.yr[, c('SPPLIVLB', 'SPPLNDLB', 'SPPVALUE', 'NESPP4') := NULL]
    
    #Rename summed columns
    data.table::setnames(comland.yr, c('V1', 'V2'), c('SPPLIVLB', 'SPPVALUE'))
    
    comland <- data.table::rbindlist(list(comland, comland.yr))
    
    message("Pulled data from ",tables[itab]," ...")
    
  }
  
  #Convert number fields from chr to num
  numberCols <- c('YEAR', 'MONTH', 'NEGEAR', 'TONCL2', 'NESPP3', 'UTILCD', 'AREA',
                  'MKTCAT')
  comland[, (numberCols):= lapply(.SD, as.numeric), .SDcols = numberCols][]
  
  #Adjust pounds to metric tons
  comland[, SPPLIVMT := SPPLIVLB * 0.00045359237]
  comland[, SPPLIVLB := NULL]
  
  #standardize YEAR field
  comland[YEAR < 100, YEAR := YEAR + 1900L]
  
  #Add Nationality Flag
  comland[, US := T]
  
  return(list(comland = comland[], 
              sql     = sql))
}