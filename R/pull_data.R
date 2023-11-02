#' Pull stockeff data and comlandr data
#'
#'

channel <- dbutils::connect_to_database("sole","abeet")
channelNova <- dbutils::connect_to_database("nova","abeet",ROracle = F)

sqlStatement <- "select * from cfdbs.cfspp"
species <- DBI::dbGetQuery(channel,sqlStatement)   %>% 
  dplyr::mutate(NESPP3 = as.numeric(NESPP3)) %>%
  dplyr::select(SPPNM,NESPP3) %>% 
  dplyr::distinct()

library(magrittr)
comlandData <- comlandr::get_comland_raw_data(channel,channelNova,useLanded = F,removeParts = F)

sqlStatement <- "select year, month, day, negear, negear2, toncl2, nespp3, nespp4, area, spplndlb, spplivlb, sppvalue, utilcd, mesh, species_itis, market_code,  ntrips, df, da ,  trplndlb,trplivlb,GIS_LAT,GIS_LON 
                    from stockeff.mv_cf_landings where YEAR >=1964 and YEAR <1970 "

se <- DBI::dbGetQuery(channel,sqlStatement)
saveRDS(se,here::here("data/stockeff1964_1969.rds"))
rm(se)

sqlStatement <- "select year, month, day, negear, negear2, toncl2, nespp3, nespp4, area, spplndlb, spplivlb, sppvalue, utilcd, mesh, species_itis, market_code,  ntrips, df, da ,  trplndlb,trplivlb,GIS_LAT,GIS_LON 
                    from stockeff.mv_cf_landings where YEAR >=1970 and YEAR <1995 "
se <- DBI::dbGetQuery(channel,sqlStatement)
saveRDS(se,here::here("data/stockeff1970_1994.rds"))
rm(se)

sqlStatement <- "select year, month, day, negear, negear2, toncl2, nespp3, nespp4, area, spplndlb, spplivlb, sppvalue, utilcd, mesh, species_itis, market_code,  ntrips, df, da ,  trplndlb,trplivlb,GIS_LAT,GIS_LON 
                    from stockeff.mv_cf_landings where YEAR >=1995 and YEAR <2010 "
se <- DBI::dbGetQuery(channel,sqlStatement)
saveRDS(here::here("data/stockeff1995_2009.rds"))
rm(se)

sqlStatement <- "select year, month, day, negear, negear2, toncl2, nespp3, nespp4, area, spplndlb, spplivlb, sppvalue, utilcd, mesh, species_itis, market_code,  ntrips, df, da ,  trplndlb,trplivlb,GIS_LAT,GIS_LON 
                    from stockeff.mv_cf_landings where YEAR >=2010 and YEAR <2023 "
se <- DBI::dbGetQuery(channel,sqlStatement)
saveRDS(here::here("data/stockeff2010_2023.rds"))
rm(se)

