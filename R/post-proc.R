#' Hlavni funkce pro post processing po nove kalibraci
#'
#' @param indicators (logical) Spocitat indikatory?
#' @details Vychozim bodem jsou parametry modelu Bilan (?Data(bpars)? ) - dale navazuji:
#' \item generovani dennich dat
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
indicators = function(SPI_vars = c('P', 'RM', 'BF'), DV_standardize = TRUE, DV_thr = .2, DV_vars = c('P', 'RM', 'SW', 'GS')){
  
  message('Pocitam indikatory.')
  setwd(file.path(.datadir, 'postproc_stable'))
  BM = data.table(readRDS('bilan_month.rds'))
  ref = getOption('ref_period')
  
  # !! DO SOUBORU S INDIKATORY PROSIM ZAHRN I SLOUPEC ROK A SLOUPEC MESIC
  
  # SPI
  
  registerDoMC(cores = 4)
  
  S = foreach(i = getOption('ind_scales')) %dopar% {
    
    return(
      BM[variable %in% SPI_vars & !is.na(value)][, .(DTM, value = c(
        SPEI::spi(
        ts(value, frequency = 12, start = c(year[1], month[1]), end = c(year[.N], month[.N])), 
        scale = i, ref.start = c(year(ref[1]), month(ref[1])), ref.end = c(year(ref[2]), month(ref[2])))$fitted), 
        scale = i), by = .(UPOV_ID, variable)])
    
  }
  
  names(S) = paste0('SPI_', getOption('ind_scales'))
  S = rbindlist(S, idcol = 'IID')
  
  setwd(file.path(.datadir, 'indikatory'))

  saveRDS(S, 'spi.rds')

  
  # SPEI
  cbm = dcast.data.table(BM[variable %in% c('P', 'PET'), .(UPOV_ID, DTM, variable, value)], UPOV_ID + DTM ~ variable)
  cbm = cbm[complete.cases(P, PET)]
  cbm[, B := P - PET]
  # ...
  
  
  # PDSI
  
  
  # dV
    
}

# JE POTREBA DODELAT !!!
def.vol.val = function(){
  
  q = BM[variable == 'RM' & UPOV_ID == 'BER_0100']
  def.vol(q$value, quantile(q$value, .2))
  q[, THR := quantile(value, .2), by = .(UPOV_ID, variable)]
  q[, EID := def.vol_id(value, THR)]
  q[!is.na(EID) & variable %in% c('RM', 'P', 'BF'), dV := cumsum(THR - value), by = .(UPOV_ID, variable, EID)]
  q[!is.na(EID) & variable %in% c('SW', 'GS'), dV := (THR - value), by = .(UPOV_ID, variable, EID)]
  
}

def.vol_id = function(x, threshold = quantile(x, .2, na.rm = TRUE), mit = 0, min.len = 0){
  
  nul = rle(x >= threshold)
  cs = cumsum(nul$len)
  nul$val[ (nul$val==TRUE) & (nul$len<mit) ] = FALSE
  breaks = c(0,cs)#if (cs[1]==1) (cs) else (c(1, cs))
  fct = cut(1:length(x), breaks = breaks, inc = TRUE)
  
  kde = rle(nul$val[as.integer(fct)])
  kde$val[ (kde$val == FALSE) & (kde$len<min.len)] = TRUE
  cs = cumsum(kde$len)
  breaks = c(0,cs)#if (cs[1]==1) (cs) else (c(1, cs))
  fct = as.integer(cut(1:length(x), breaks = breaks, inc = TRUE))#, labels = 1:length(breaks))	)
  kkde = which(kde$val)
  fct[as.integer(fct)%in%kkde] = NA
  
  fct
}

