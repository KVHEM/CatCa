
#' Zjisti plochu povodi v km2
#'
#' @md
#' @param id UPOV_ID
#' @param upov_only jen pro UPOV nebo pro cele povodi od pramene?
#'
#' @return Plocha v km2
#' @export A
#'
#' @examples
A = Vectorize(function(id, upov_only = FALSE){
  a = TABA[FROM == id, if(upov_only)(PLOCHA_UPOV)else(PLOCHA_CELKEM)]
  a
})


#' Prevody mezi vyskami a objemy
#'
#' @param q odtok v m3/s
#' @param mm odtok v mm
#' @param u uzivani v tis. m3
#' @param id UPOV_ID
#' @param ... parametry pro A (tj. momentalne upov_only)
#'
#' @return
#' @export q2mm_day
#'
#' @examples
q2mm_day = Vectorize(function(q, id, ...){
  
  area = A(id, ...) * 1000000
  
 unname((1000 * q * 3600 * 24) / (area))
  
})

#' @describeIn q2mm_day 
#' @export mm_day2q
mm_day2q = Vectorize(function(mm, id, ...){
  
  area = A(id, ...) * 1000000
  unname((mm * area) / (1000 * 3600 * 24))
  
})

#' @describeIn q2mm_day
#' @export u2mm_day 
u2mm_day = function(u, id, dtm = NA, ...){

  area = A(id, ...) * 1000000
  
  uu = u / area 
  if (any(is.na(dtm))) return(uu/30.4375)
  
  uu/days_in_month(dtm)
  
}

#' @describeIn q2mm_day 
#' @export u2mm_month
u2mm_month = function(u, id, ...){
  
  area = A(id, ...) * 1000000
  
  uu = u / area 
  
  uu
  
}

#' Ziskej charakteristiky pro UPOV
#'
#' @param id UPOV_ID
#'
#' @return charakteristiky: prumerny odtok, srazky, m-denni vody apod.
#' @export char
#'
#' @examples
char = function(id){
  CHARS[UPOV_ID %in% id]
}

#' Extrahuj z tabulky CHARS
#'
#' @param ... nazev pozadovane veliciny, nemusi byt v uvozovkach
#'
#' @return funkci s parametrem id = UPOV_ID
#' @export ch
#'
#' @examples
#' ch(Qa)
#' 
#' \dontrun{
#' ch(Qa)(id = c('BER_0100', 'BER_0010'))
#' }
ch = function(...){
  n = c(as.character(substitute(list(...))[-1L]))
  function(id){
    e = parse(text = n)
    CHARS[UPOV_ID %in% id, eval(e)]
  }
}

#' Extrahuj m-denni vody z tabulky chars
#'
#' @param m pocet dni
#'
#' @return funkce s parametry id = UPOV_ID a mm_day: TRUE = Qmd je v mm_den jinak m3/s
#' @export qmd
#'
#' @examples
qmd = function(m){
  Vectorize(function(id, mm_day = TRUE){
    ch = char(id)
    e = parse(text = paste0('Q', m, 'd'))
    if (mm_day) ch[,  eval(e)/ 365.25] else ch[, mm_day2q(eval(e), id) / 365.25]
  })
}



#' Vypocti minimalni zustatkovy prutok 
#'
#' @param id UPOV_ID
#' @param mm_day (logical) ma byt vysledek v mm_den nebo m3/s
#'
#' @return
#' @export MZP
#'
#' @examples
MZP = Vectorize(function(id, mm_day = TRUE){
  
  ch = char(id)
  if (nrow(ch)==0) return(NA_real_)
  Q355d = qmd(355)(id, mm_day = FALSE) #ch[, mm_day2q(Q355d, id) / 365.25]
  if (is.na(Q355d)) return(NA_real_)
  
  if (Q355d <= 0.05) return(qmd(330)(id, mm_day = mm_day))
  if (Q355d <= 0.50) return(0.5 * (qmd(330)(id, mm_day = mm_day) + qmd(355)(id, mm_day = mm_day)))
  if (Q355d <= 5.00) return(qmd(355)(id, mm_day = mm_day))
  return(0.5 * (qmd(355)(id, mm_day = mm_day) + qmd(364)(id, mm_day = mm_day)))
})