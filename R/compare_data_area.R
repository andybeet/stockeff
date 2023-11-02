#' compare Stockeff and comlandr
#' plot 20 most abundantly caught species over time'
#'
#'

library(magrittr)

sqlStatement <- "select * from cfdbs.cfspp"
species <- DBI::dbGetQuery(channel,sqlStatement)   %>% 
  dplyr::mutate(NESPP3 = as.numeric(NESPP3)) %>%
  dplyr::select(SPPNM,NESPP3) %>% 
  dplyr::distinct()


comlandData <- readRDS(here::here("data/comlandData.rds"))$comland  %>%
  dplyr::group_by(AREA,YEAR) %>%
  dplyr::summarise(landmt = sum(SPPLIVMT,na.rm = T),
                   value = sum(SPPVALUE,na.rm = T),
                   .groups = "drop") %>%
  dplyr::mutate(source = "comlandr") 



se1 <- readRDS(here::here("data/stockeff1964_1969.rds"))
se <- NULL
setemp <- se1 %>%
  dplyr::group_by(AREA,YEAR) %>%
  dplyr::summarise(landmt = sum(SPPLIVLB*0.00045359237),
                   value = sum(SPPVALUE,na.rm = T),
                   .groups = "drop")
rm(se1)

se <- rbind(se,setemp)

se2 <- readRDS(here::here("data/stockeff1970_1994.rds"))

setemp <- se2 %>%
  dplyr::group_by(AREA,YEAR) %>%
  dplyr::summarise(landmt = sum(SPPLIVLB*0.00045359237),
                   value = sum(SPPVALUE,na.rm = T),
                   .groups = "drop")
se <- rbind(se,setemp)
rm(se2)

se3 <- readRDS(here::here("data/stockeff1995_2009.rds"))
setemp <- se3 %>%
  dplyr::group_by(AREA,YEAR) %>%
  dplyr::summarise(landmt = sum(SPPLIVLB*0.00045359237),
                   value = sum(SPPVALUE,na.rm = T),
                   .groups = "drop")
se <- rbind(se,setemp)
rm(se3)

se4 <- readRDS(here::here("data/stockeff2010_2023.rds"))
setemp <- se4 %>%
  dplyr::group_by(AREA,YEAR) %>%
  dplyr::summarise(landmt = sum(SPPLIVLB*0.00045359237),
                   value = sum(SPPVALUE,na.rm = T),
                   .groups = "drop")
se <- rbind(se,setemp)

rm(se4)

se <- se %>% 
  dplyr::mutate(source = "stockeff") %>% 
  dplyr::mutate(AREA = as.numeric(AREA),
                YEAR = as.numeric(YEAR))
# combine comlandr with stockeff
d <- rbind(comlandData,se) %>% 
  dplyr::mutate(source = as.factor(source))

top20 <- d %>% 
  dplyr::filter(source == "comlandr") %>%
  dplyr::group_by(AREA) %>% 
  dplyr::summarise(total = sum(landmt)) %>%
  dplyr::arrange(desc(total)) %>% 
  head(28) %>%
  dplyr::pull(AREA)


d %>% 
  dplyr::filter(AREA %in% top20) %>%
  ggplot2::ggplot(.) +
  ggplot2::geom_line(ggplot2::aes(x=YEAR,y = landmt,color=source))+
  ggplot2::facet_wrap(ggplot2::vars(AREA),scales="free_y")

ggplot2::ggsave(here::here("plots/top20byarea.png"),width=10,height=8)
