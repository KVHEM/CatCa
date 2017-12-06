#' Hlavni funkce pro post processing po nove kalibraci
#'
#' @param indicators (logical) Spocitat indikatory?
#' @details Vychozim bodem jsou parametry modelu Bilan (?Data(bpars)? ) - dale navazuji:
#' \itemize{
#'  \item generovani dennich dat \code{bilan_gen}
#'  \item agregace do tydenniho a mesicniho kroku \code{bilan_agg}
#'  \item vypocet indikatory \code{indicators}
#'  \item ...
#'  }
#' @return NULL Funkce se pouziva pro sve vedlejsi efekty - priprava/prepocet ruznych datasetu - meni data v .datadir
#' @export post_process
#'
#' @examples
post_process = function(aggregate = TRUE, indicators = TRUE){
  
  if (aggregate) bilan_agg()
  
  setwd(file.path(.datadir, 'bilan'))
  d = dir()
  
  if (indicators) indicators()    
  if (webapp_data) web_app()
    
 
}


#' Generuje denni data na zaklade parametru
#'
#' @return
#' @export bilan_gen
#'
#' @examples
bilan_gen = function(){
  
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
#' @param B_vars promenne P - PET pro vypocet SPEI
#' @param dVc_vars promenne pro vypocet dV
#' @param dVn_vars promenne pro vypocet dV
#' @param tscale meritko pro vypocet indikatoru
#' @param DV_vars promenne pro vypocet nedostatkovych objemu
#' @param DV_standardize maji se data nejdrive standardizovat?
#' @param DV_thr prah pro vypocet nedostatkovych objemu
#'
#' @return data.table s casovymi radami indikatoru
#' @export indicators
#'
#' @examples

catca_spi <- function(SPI_vars = c('P', 'RM', 'BF'), ref = getOption('ref_period')) {
  
  registerDoMC(cores = 4)
  
  message('Pocitam SPI.')
  setwd(file.path(.datadir, 'postproc_stable'))
  BM = data.table(readRDS('bilan_month.rds'))

  S = foreach(i = getOption('ind_scales')) %dopar% {
    
    return(
      BM[variable %in% SPI_vars & !is.na(value)][, .(DTM, value = c(
        SPEI::spi(
          ts(value, frequency = 12, start = c(year[1], month[1]), end = c(year[.N], month[.N])), 
          scale = i, ref.start = c(year(ref[1]), month(ref[1])), ref.end = c(year(ref[2]), month(ref[2])))$fitted), 
        scale = i), by = .(UPOV_ID, variable)])
  }
  
  names(S) <- paste0('SPI_', getOption('ind_scales'))
  S <- rbindlist(S, idcol = 'IID')
  S <- cbind(S, month = month(S$DTM), year = year(S$DTM))
  
  setwd(file.path(.datadir, 'indikatory'))
  saveRDS(S, 'spi.rds')
  
}

catca_spei <- function(B_vars = c('P', 'PET'), ref = getOption('ref_period')) {

  registerDoMC(cores = 4)
    
  message('Pocitam SPEI.')
  setwd(file.path(.datadir, 'postproc_stable'))
  BM = data.table(readRDS('bilan_month.rds'))

  cbm = dcast.data.table(BM[variable %in% B_vars, .(UPOV_ID, DTM, variable, value)], UPOV_ID + DTM ~ variable)
  cbm = cbm[complete.cases(P, PET)]
  cbm[, B := P - PET]
  
  S = foreach(i = getOption('ind_scales')) %dopar% { 
    
    return(
      cbm[variable == 'B' & !is.na(value)][, .(DTM, value = c(
        SPEI::spei(
          ts(value, frequency = 12, start = c(year[1], month[1]), end = c(year[.N], month[.N])), 
          scale = i, ref.start = c(year(ref[1]), month(ref[1])), ref.end = c(year(ref[2]), month(ref[2])))$fitted), 
        scale = i), by = .(UPOV_ID, variable)])
    
  }
  
  names(S) <- paste0('SPEI_', getOption('ind_scales'))
  S <- rbindlist(S, idcol = 'IID')
  S <- cbind(S, month = month(S$DTM), year = year(S$DTM))
  
  setwd(file.path(.datadir, 'indikatory'))
  saveRDS(S, 'spei.rds')
  
}

catca_dv <- function(DV_standardize = TRUE, DV_thr = .2, DV_vars = c('P', 'RM', 'SW', 'GS'), ref = getOption('ref_period')) {
  
  message('Pocitam dV.')
  setwd(file.path(.datadir, 'postproc_stable'))
  BM = data.table(readRDS('bilan_month.rds'))
  
  def_vol_id = function(x, threshold = quantile(x, DV_thr, na.rm = TRUE), mit = 0, min.len = 0) { 
    
    nul = rle(x >= threshold)
    cs = cumsum(nul$len)
    nul$val[(nul$val == TRUE) & (nul$len < mit)] = FALSE
    breaks = c(0, cs)#if (cs[1] == 1) (cs) else (c(1, cs))
    fct = cut(1:length(x), breaks = breaks, inc = TRUE) 
    
    kde = rle(nul$val[as.integer(fct)])
    kde$val[(kde$val == FALSE) & (kde$len < min.len)] = TRUE
    cs = cumsum(kde$len)
    breaks = c(0, cs) #if (cs[1] == 1) (cs) else (c(1, cs))
    fct = as.integer(cut(1:length(x), breaks = breaks, inc = TRUE)) #, labels = 1:length(breaks))	)
    kkde = which(kde$val)
    fct[as.integer(fct) %in% kkde] = NA
    
    fct
  }
  
  def_vol_val = function(dVc_vars = c('RM', 'P', 'BF'), dVn_vars = c('SW', 'GS')) {
    
    q = BM[variable == 'RM'] 
    #def_vol(q$value, quantile(q$value, DV_thr))         
    q[, THR := quantile(value, DV_thr), by = .(UPOV_ID, variable)]
    q[, EID := def_vol_id(value, THR)]
    q[!is.na(EID) & variable %in% dVc_vars, dV := cumsum(THR - value), by = .(UPOV_ID, variable, EID)]  
    q[!is.na(EID) & variable %in% dVn_vars, dV := (THR - value), by = .(UPOV_ID, variable, EID)]
    
    q <- q[, .(UPOV_ID, year, month, EID, dV)]
    #q <- q[!is.na(dV)]
    
    setwd(file.path(.datadir, 'indikatory'))
    saveRDS(q, 'dV.rds')
  } 
  
}

indicators = function() {

  message('Pocitam indikatory.')
  
  # SPI
  catca_spi()
  
  # SPEI
  catca_spei()
  
  # dV
  catca_dv()  
 
  # PDSI
  
}

