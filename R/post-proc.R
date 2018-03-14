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


#' Generuje denni data na zaklade parametru z used_data/pars, ulozi vysledek do used_data/bilan
#'
#' @param UPOVS jeden nebo vice utvaru
#' @param ivars sada vstupnich parametru modelu bilan
#' @return
#' @export bilan_gen
#'
#' @examples
#'
#' UPOVS <- c('DYJ_0320', 'BER_0050')
#' bilan_gen(UPOVS, P = P, T = T, R = 0) # Stepankova data
#'
#' bilan_gen(UPOVS, P = Rain, Tavg = T, PET = Etr, R = 0) # SoilClim data
#'
bilan_gen <- function(UPOVS, ...) {

  ivars <- expression(list(...))

  stop_cluster <- create_cluster()

  foreach(i = 1:length(UPOVS)) %dopar% {
    UPOV_PARS <- readRDS(file.path(paste0(.datadir, 'pars/pars.rds')))[[UPOVS[i]]]
    UPOV_AMETEO <- as.data.table(readRDS(file.path(.datadir, 'amteo61-16', paste0(UPOVS[i], '.rds'))))
    UPOV_AMETEO <- UPOV_AMETEO[DTM >= '1982-11-01' & DTM <= '2010-10-31', ]

    b <- bil.new(type = "D")
    bil.set.params.curr(model = b, params = UPOV_PARS$pars$current[1:6])
    bil.set.values(     model = b,
                   input_vars = data.frame(UPOV_AMETEO[, eval(ivars)]),
                    init_date = '1982-11-01',
                       append = FALSE)
    if(is.null(eval(ivars)$PET)) {
      bil.pet(b)
    }
    bil.run(b)

    saveRDS(bil.get.values(b), file.path(paste0(.datadir, 'bilan/', UPOVS[i], '.rds')))
  }

  stop_cluster()
}

#' Vygeneruje data.table parametru z used_data/pars
#'
#' @param file nazev souboru - default = pars.rds
#'
#' @return
#' @export bilan_getPars
#'
#' @examples
bilan_getPars = function(file = 'pars.rds'){
  
  setwd(file.path(.datadir, 'pars'))
  pa = readRDS(file)

  p  = lapply(pa, function(x){x$pars[, .(name, current)]})
  
  rbindlist(p, idcol = 'UPOV_ID')
  
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

  stop_cluster <- create_cluster()

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

  stop_cluster()

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

#' Kalibruj SPI
#'
#' @param SPI_vars promenne pro vypocet SPI
#' @param ref casove meritko pro vypocet
#' @param i scale vypoctu indexu
#'
#' @return data.table s koeficienty pro vypocet SPI
#' @export cal_spi
#'
#' @examples
cal_spi <- function(SPI_vars = c('P', 'RM', 'BF'), ref = getOption('ref_period')) {

  stop_cluster <- create_cluster()

  message('Pocitam koeficienty pro SPI.')
  setwd(file.path(.datadir, 'postproc_stable'))
  if(!exists("BM")) {
    BM <- data.table(readRDS('bilan_month.rds'))
  }

  S = foreach(i = getOption('ind_scales')) %dopar% {

    return(
      BM[variable %in% SPI_vars & !is.na(value)][, .(value = list(
        SPEI::spi(
          ts(value, frequency = 12, start = c(year[1], month[1]), end = c(year[.N], month[.N])),
          scale = i, ref.start = c(year(ref[1]), month(ref[1])), ref.end = c(year(ref[2]), month(ref[2])))$coef),
        scale = i), by = .(UPOV_ID, variable)])
  }

  names(S) <- paste0('SPI_', getOption('ind_scales'))
  S <- rbindlist(S, idcol = 'IID')

  setwd(file.path(.datadir, 'indikatory'))
  saveRDS(S, 'spi_coef.rds')

  stop_cluster()
}

#' Vypocet SPI
#'
#' @param SPI_vars promenne pro vypocet SPI
#' @param ref casove meritko pro vypocet
#' @param i scale vypoctu indexu
#'
#' @return data.table s casovymi radami SPI
#' @export catca_spi
#'
#' @examples
catca_spi <- function(SPI_vars = c('P', 'RM', 'BF'), ref = getOption('ref_period')) {

  stop_cluster <- create_cluster()

  message('Pocitam SPI.')
  setwd(file.path(.datadir, 'postproc_stable'))
  if(!exists("BM")) {
    BM <- data.table(readRDS('bilan_month.rds'))
  }

  #pz = BM[, .(pze = sum(value==0)/.N), by = .(UPOV_ID, variable) ]
  pze = 0

  setwd(file.path(.datadir, 'indikatory'))
  Scoef = readRDS('spi_coef.rds')
  id = Scoef[, any(is.na(value[[1]])), by = .(UPOV_ID, scale, variable)][V1==TRUE, unique(UPOV_ID)]

  getCoef = function(upov_id, Scale, var){
    Scoef[UPOV_ID == upov_id & scale == Scale & variable == var, value[[1]]]
  }

  S = foreach(i = getOption('ind_scales')) %dopar% {

    return(
      BM[variable %in% SPI_vars & !is.na(value) & !UPOV_ID%in%id][, .(DTM, value = c(
        SPEI::spi(
          ts(value, frequency = 12, start = c(year[1], month[1]), end = c(year[.N], month[.N])),
          scale = i, ref.start = c(year(ref[1]), month(ref[1])), ref.end = c(year(ref[2]), month(ref[2])), params = getCoef(UPOV_ID[1], i, variable[1]))$fitted),
        scale = i), by = .(UPOV_ID, variable)])
  }

  names(S) <- paste0('SPI_', getOption('ind_scales'))
  S <- rbindlist(S, idcol = 'IID')
  S <- cbind(S, month = month(S$DTM), year = year(S$DTM))

  setwd(file.path(.datadir, 'indikatory'))
  saveRDS(S, 'spi.rds')

  stop_cluster()

}

#' Kalibrace SPEI
#'
#' @param B_vars promenne P - PET pro vypocet SPEI
#' @param ref casove meritko pro vypocet
#' @param i scale vypoctu indexu
#'
#' @return data.table s koeficienty pro SPEI
#' @export cal_spei
#'
#' @examples
cal_spei <- function(ref = getOption('ref_period')) {

  stop_cluster <- create_cluster()

  message('Pocitam koeficienty pro SPEI.')
  setwd(file.path(.datadir, 'postproc_stable'))
  if(!exists("BM")) {
    BM <- data.table(readRDS('bilan_month.rds'))
  }

  cbm = dcast.data.table(BM[variable %in% c("P", "PET"), .(UPOV_ID, DTM, variable, value, year, month)], UPOV_ID + DTM + year + month ~ variable)
  cbm = cbm[complete.cases(P, PET)]
  cbm[, B := P - PET]

  S = foreach(i = getOption('ind_scales')) %dopar% {

    return(
      cbm[ !is.na(B)][, .(value = list(
        SPEI::spei(
          ts(B, frequency = 12, start = c(year[1], month[1]), end = c(year[.N], month[.N])),
          scale = i, ref.start = c(year(ref[1]), month(ref[1])), ref.end = c(year(ref[2]), month(ref[2])))$coef),
        scale = i), by = .(UPOV_ID)])

  }

  names(S) <- paste0('SPEI_', getOption('ind_scales'))
  S <- rbindlist(S, idcol = 'IID')

  setwd(file.path(.datadir, 'indikatory'))
  saveRDS(S, 'spei_coef.rds')

  stop_cluster()

}

#' Vypocet SPEI
#'
#' @param B_vars promenne P - PET pro vypocet SPEI
#' @param ref casove meritko pro vypocet
#' @param i scale vypoctu indexu
#'
#' @return data.table s casovymi radami SPEI
#' @export catca_spei
#'
#' @examples
catca_spei <- function(ref = getOption('ref_period')) {

  stop_cluster <- create_cluster()

  message('Pocitam SPEI.')
  setwd(file.path(.datadir, 'postproc_stable'))
  if(!exists("BM")) {
    BM <- data.table(readRDS('bilan_month.rds'))
  }

  cbm = dcast.data.table(BM[variable %in% c("P", "PET"), .(UPOV_ID, DTM, variable, value, year, month)], UPOV_ID + DTM + year + month ~ variable)
  cbm = cbm[complete.cases(P, PET)]
  cbm[, B := P - PET]

  setwd(file.path(.datadir, 'indikatory'))
  Scoef = readRDS('spei_coef.rds')
  id = Scoef[, any(is.na(value[[1]])), by = .(UPOV_ID, scale)][V1==TRUE, unique(UPOV_ID)]

  getCoef = function(upov_id, Scale){
    Scoef[UPOV_ID == upov_id & scale == Scale, value[[1]]]
  }

  S = foreach(i = getOption('ind_scales')) %dopar% {

    return(
      cbm[!is.na(B) & !UPOV_ID%in%id][, .(DTM, value = c(
        SPEI::spei(
          ts(B, frequency = 12, start = c(year[1], month[1]), end = c(year[.N], month[.N])),
          scale = i, ref.start = c(year(ref[1]), month(ref[1])), ref.end = c(year(ref[2]), month(ref[2])), params = getCoef(UPOV_ID[1], i))$fitted),
        scale = i), by = .(UPOV_ID)])

  }

  names(S) <- paste0('SPEI_', getOption('ind_scales'))
  S <- rbindlist(S, idcol = 'IID')
  S <- cbind(S, month = month(S$DTM), year = year(S$DTM))

  setwd(file.path(.datadir, 'indikatory'))
  saveRDS(S, 'spei.rds')

  stop_cluster()
}

#' Kalibrace deficitnich objemu
#'
#' @param DV_standardize maji se data nejdrive standardizovat?
#' @param DV_thr prah pro vypocet nedostatkovych objemu
#' @param DV_vars promenne pro vypocet nedostatkovych objemu
#' @param ref casove meritko pro vypocet
#'
#' @return data.table s thresholdy vars pro jednotlive UPOVy pro vypocet dV
#' @export cal_dv
#'
#' @examples
cal_dv <- function(DV_standardize = TRUE, DV_thr = .2, dVc_vars = c('RM', 'P', 'BF'), dVn_vars = c('SW', 'GS')) {

  message('Pocitam thresholdy pro dV.')
  setwd(file.path(.datadir, 'postproc_stable'))
  if(!exists("BM")) {
    BM <- data.table(readRDS('bilan_month.rds'))
  }
  
  q = BM[variable %in% c(dVc_vars, dVn_vars)]
  #def_vol(q$value, quantile(q$value, DV_thr))
  q$value <- replace(q$value, is.na(q$value), 0)

  q[, THR := quantile(value, DV_thr), by = .(UPOV_ID, variable)]
  q <- unique(q[, .(UPOV_ID, variable, THR)])
  #q <- q[!is.na(dV)]

  setwd(file.path(.datadir, 'indikatory'))
  saveRDS(q, 'dV_thr.rds')
  
}

#' Vypocet deficitnich objemu
#'
#' @param DV_standardize maji se data nejdrive standardizovat?
#' @param DV_thr prah pro vypocet nedostatkovych objemu
#' @param DV_vars promenne pro vypocet nedostatkovych objemu
#' @param ref casove meritko pro vypocet
#'
#' @return data.table s casovymi radami dV + EID
#' @export catca_dv
#'
#' @examples
catca_dv <- function(DV_standardize = TRUE, ref = getOption('ref_period'),
                     dVc_vars = c('RM', 'P', 'BF'), dVn_vars = c('SW', 'GS')) {

  message('Pocitam dV.')
  setwd(file.path(.datadir, 'postproc_stable'))
  if(!exists("BM")) {
    BM <- data.table(readRDS('bilan_month.rds'))
  }

  def_vol_id = function(x, threshold, mit = 0, min.len = 0) {

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
  
  q = BM[variable %in% c(dVc_vars, dVn_vars)]
  #def_vol(q$value, quantile(q$value, DV_thr))
  q$value <- replace(q$value, is.na(q$value), 0)

  setwd(file.path(.datadir, 'indikatory'))
  q_thr <- readRDS("dV_thr.rds")

  q <- merge(q, q_thr, by = c("UPOV_ID", "variable"))
  q[, EID := def_vol_id(value, THR)]
  q[!is.na(EID) & variable %in% dVc_vars, dV := cumsum(THR - value), by = .(UPOV_ID, variable, EID)]
  q[!is.na(EID) & variable %in% dVn_vars, dV := (THR - value), by = .(UPOV_ID, variable, EID)]

  q <- q[, .(UPOV_ID, year, month, variable, EID, dV)]
  q <- q[!is.na(dV)]

  setwd(file.path(.datadir, 'indikatory'))
  saveRDS(q, 'dV.rds')
  

}

#' Kalibrace indikatoru
#'
#' @param SPI_vars promenne pro vypocet SPI
#' @param B_vars promenne P - PET pro vypocet SPEI
#' @param dVc_vars promenne pro vypocet dV
#' @param dVn_vars promenne pro vypocet dV
#' @param ref casove meritko pro vypocet
#' @param i scale vypoctu indexu (SPI, SPEI)
#' @param DV_vars promenne pro vypocet nedostatkovych objemu
#' @param DV_standardize maji se data nejdrive standardizovat?
#' @param DV_thr prah pro vypocet nedostatkovych objemu
#'
#' @return data.table s kalibracnimi parametry pro vypocet indikatoru
#' @export cal_indicators
#'
#' @examples
cal_indicators = function() {

  message('Kalibruji indikatory.')

  setwd(file.path(.datadir, 'postproc_stable'))
  BM <- data.table(readRDS('bilan_month.rds'))

  # SPI
  cal_spi()

  # SPEI
  cal_spei()

  # dV
  cal_dv()

  # PDSI

}

#' Vypocet indikatoru
#'
#' @param SPI_vars promenne pro vypocet SPI
#' @param B_vars promenne P - PET pro vypocet SPEI
#' @param dVc_vars promenne pro vypocet dV
#' @param dVn_vars promenne pro vypocet dV
#' @param ref casove meritko pro vypocet
#' @param i scale vypoctu indexu (SPI, SPEI)
#' @param DV_vars promenne pro vypocet nedostatkovych objemu
#' @param DV_standardize maji se data nejdrive standardizovat?
#' @param DV_thr prah pro vypocet nedostatkovych objemu
#'
#' @return data.table s casovymi radami indikatoru
#' @export indicators
#'
#' @examples
catca_indicators = function() {

  message('Pocitam indikatory.')

  setwd(file.path(.datadir, 'postproc_stable'))
  BM <- data.table(readRDS('bilan_month.rds'))

  # SPI
  catca_spi()

  # SPEI
  catca_spei()

  # dV
  catca_dv()

  # PDSI

}

