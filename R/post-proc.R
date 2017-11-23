#' Hlavni funkce pro post processing po nove kalibraci
#'
#' @param indicators (logical) Spocitat indikatory?
#'
#' @return NULL Funkce se pouziva pro sve vedlejsi efekty - priprava/prepocet ruznych datasetu - meni data v .datadir
#' @export post_process
#'
#' @examples
post_process = function(aggregate = TRUE, indicators = TRUE){
  
  if (aggregate) bilan_agg()
  
  setwd(file.path(.datadir, 'bilan'))
  d = dir()
  
  if (indicators) indicators()    
    
 
}


#' Agreguje data z used_data/bilan na mesicni a tydenni krok, ulozi vysledek do used_data/postproc
#'
#' @return
#' @export
#'
#' @examples
bilan_agg = function(){
  
  setwd(file.path(.datadir, 'bilan'))
  d = dir()
  #pb = txtProgressBar(min = 1, max = length(d), initial = 1, style = 3)
  
  # i = d[1]
  #for (i in d){
  registerDoMC(cores = 4)
  M = foreach(i = d) %dopar% {
    
      #setTxtProgressBar(pb, length(M)+1)
      upov = gsub('\\.rds', '', i)
      r = readRDS(i)
      kde = grepl(upov, names(r))
      if (any(kde)) {
        setnames(r, names(r)[kde], gsub(paste0(upov, '\\.'), '', names(r)[kde]))
      }
      mr = melt(r, id.vars = 'DTM')
      m1 = mr[variable!='T', .(value = sum(value)), by = .(year(DTM), month(DTM), variable)]
      m2 = mr[variable=='T', .(value = mean(value)), by = .(year(DTM), month(DTM), variable)]
      m = rbind(m1,m2)
      m[, DTM:=as.Date(paste(year, month, 1, sep = '-'))]
      return(m)
    
      # w1 = mr[variable!='T', .(value = sum(value), DTM = DTM[1]), by = .(year(DTM), week(DTM), variable)]
      # w2 = mr[variable=='T', .(value = mean(value), DTM = DTM[1]), by = .(year(DTM), week(DTM), variable)]
      # w = rbind(w1,w2)
      # #w[, DTM:=as.Date(paste(year, month, 1, sep = '-'))]
      # W[[length(M)+1]] = w
    
  }
  
  names(M) = gsub('\\.rds', '', d)
  BM = rbindlist(M, idcol = 'UPOV_ID')
  
  # names(W) = gsub('\\.rds', '', dir())
  # BW = rbindlist(W, idcol = 'UPOV_ID')
   
  setwd(file.path(.datadir, 'postproc'))
  saveRDS(BM, 'bilan_month.rds')
  #saveRDS(BW, 'bilan_week.rds')
  rm(BM)
  #rm(BW)
  gc()
}



#' Vypocet indikatoru
#'
#' @param SPI_vars promenne pro vypocet SPI
#' @param SPEI_vars promenne pro vypocer SPEI
#' @param tscale meritko pro vypocet indikatoru
#' @param DV_vars promenne pro vypocet nedostatkovych objemu
#' @param DV_standardize maji se data nejdrive standardizovat?
#' @param DV_thr prah pro vypocet nedostatkovych objemu
#'
#' @return data.table s casovymi radami indikatoru
#' @export indicators
#'
#' @examples
indicators = function(SPI_vars = c('P', 'R', 'RM', 'BF'), SPEI_vars = c('PE'), DV_vars = c('P', 'SW', 'R', 'RM'), DV_standardize = TRUE, DV_thr = .2){
  
  message('Pocitam indikatory.')
  setwd(file.path(.datadir, 'postproc_stable'))
  BM = data.table(readRDS('bilan_month.rds'))
  
  # SPI
  
  registerDoMC(cores = 4)
  
  S = foreach(i = c(1, 3, 6, 12)) %dopar% {
    
    return(
      BM[variable %in% SPI_vars & !is.na(value)][, .(DTM, value = c(
        SPEI::spi(
        ts(value, frequency = 12, start = c(year[1], month[1]), end = c(year[.N], month[.N])), 
        scale = i, ref.start = c(1980, 1), ref.end = c(2010, 12))$fitted), 
        scale = i), by = .(UPOV_ID, variable)])
    
  }
  
  names(S) = paste0('SPI_', c(1, 3, 6, 12))
  S = rbindlist(S, idcol = 'IID')
  
  setwd(file.path(.datadir, 'indikatory'))
  saveRDS(S, 'spi.rds')
  
  # SPEI
  
  
  # PDSI
  
  # dV
    
}
