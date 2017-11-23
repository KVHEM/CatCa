#' Nahraj data
#' 
#' @description Umožňuje přistupovat k datasetům ze synchronizované owncloud složky "used_data". Většina atributů načítá rovnou více datasetů. Stručný popis, viz níže. Pro přidání a dokumentaci datasetu upravte kód na githubu, nasynchronizujte data na owncloudu a znovu sestavte balík. Nebo kontaktujte správce.
#' 
#' @md
#' @param ... jméno/jména datasetu
#' 
#' @section Data(chars):
#' 
#' **CHARS** (data.table): Hydrologické charakteristiky povodi UPOV
#'   * UPOV_ID
#'   * NAZ_UTVAR Název útvaru
#'   
#' @section Data(geo):
#' 
#' **NADRZE** (shapefile): Významné vodní nádrže
#'   * položky atributové tabulky zatím nejasné
#'  
#' **UPOVS** (shapefile): Vrstva povodí    
#'   * UPOV_ID 
#' 
#' @section Data(qd):
#' 
#' **QD** (data.table): Denní průtoky
#' 
#' @section Data(routing): 
#' 
#' **TABA** (data.table): Tabulka následnosti povodí
#'  * FROM: UPOV_ID odkud (tece voda)
#'  * TO: UPOV_ID kam (tece voda), -9999 = mimo republiku
#'  * FR_ORD, TO_ORD: rady toku
#'  * UPOV_AREA: plocha povodi - jen UPOV
#'  * TOTAL_AREA: celkova plocha povodi - az k prameni - v soucasnosti pro povodi Ohre a ostatni nedodana povodi dopocteno jako suma UPOV_AREA - nekde to muze byt uplne blbe 
#'  
#' **TABB** (data.table): Tabulka vsech hornich povodi pro danny UPOV
#'  * FROM:
#'  * TO:
#'    
#' **TABNAD** (data.table): Tabulka charakteristik nadrží
#'  * UPOV_ID - UPOV_ID
#' 
#' **TABREKY** (data.table): Tabulka delek rek pro HYPE
#'  * UPOV_ID - UPOV_ID
#'  * L_HTOK: delka hlavniho toku v metrech
#'  * L_VTOK: delka vedlejsich toku v metrech
#'  
#' @section Data(uziv06):
#' 
#' **UZIV06** (data.table): užívání 2006-2016 v tis.m3/mes 
#'  * ?
#' 
#' **UZIV06mm** (data.table): užívání 2006-2016 v mm/mes
#'  * ?
#'  
#' @section Data(uziv79):
#' 
#' **UZIV79** (data.table): užívání 1979-2016 v tis.m3/mes
#'  * ?
#' 
#' **UZIV79mm** (data.table): užívání 1979-2016 v mm/mes
#'  * ?
#'  
#' @section Data(webapp_data):
#' 
#' **JEZERA**
#' * ?
#' 
#' **POVODI**
#' * ?
#' 
#' **REKY**
#' * ?
#' 
#' **STANICE**
#' * ?
#' 
#' @export Data
Data = function(...){

  E = expression(
    
    switch(name[j],
           
         'chars' = {
           CHARS = readRDS(file.path(.datadir, 'chmu', 'chars_dopl.rds'))
         },
         
         'geo' = {
           NADRZE = rgdal::readOGR(file.path(.datadir,"geo/nadrze.shp"), "nadrze")
           UPOVS = rgdal::readOGR(file.path(.datadir,"geo/UPOV_poly.shp"))
         },
         
         'qd' = {
           QD = data.table(readRDS(file.path(.datadir, 'chmu/QD.rds')))
         },
         
         'routing' = {
           warning('Plochy TOTAL_AREA jsou pro nektera povodi dopoctene - po zfinalizovani dodavky od CHMU bude vymeneno. Rozdily, zejmena na hranicich republiky mohou byt znacne! mh')
           TABA = data.table::data.table(base::readRDS(file.path(.datadir,"routing/TABA.rds")))
           TABB = data.table::data.table(base::readRDS(file.path(.datadir,"routing/TABB.rds")))
           TABNAD = base::readRDS(file.path(.datadir,"routing/nadrze_tab.rds"))
           TABREKY = base::readRDS(file.path(.datadir, "routing/DELKY_REK.rds"))
         },
         
         'uziv06' = {
           UZIV06 = readRDS(file.path(.datadir, 'uzivani/06_16/uzivani_na_nahraz.rds'))
           UZIV06mm = readRDS(file.path(.datadir, 'uzivani/06_16/uzivani_na_nahraz_mm.rds'))
         },
         
         'uziv79' = {
           UZIV79 = readRDS(file.path(.datadir, 'uzivani/79_15/uzivani_upovid.rds'))
           UZIV79mm = readRDS(file.path(.datadir, 'uzivani/79_15/uzivani_upovid_mm.rds'))
         },
         
          'webapp_data' = {
            povodi = rgdal::readOGR(file.path(.datadir, "webapp_data/geo/povodi.shp"))
            reky = rgdal::readOGR(file.path(.datadir, "webapp_data/geo/reky.shp"))
            jezera = rgdal::readOGR(file.path(.datadir, "webapp_data/geo/jezera.shp"))
            nadrze = rgdal::readOGR(file.path(.datadir, 'webapp_data/geo/nadrze.shp'))
            stanice = rgdal::readOGR(file.path(.datadir, "webapp_data/geo/stanice.shp"))
            kraje = rgdal::readOGR(file.path(.datadir, "webapp_data/geo/kraje.shp"))
            okresy = rgdal::readOGR(file.path(.datadir, "webapp_data/geo/okresy.shp"))
            
            popis = read.table(file.path(.datadir, 'webapp_data/E_ISVS$UTV_POV.txt'),encoding = 'UTF-8', header = TRUE, sep=';')
            BM = readRDS(file.path(.datadir, 'webapp_data/mbilan/bilan_month.rds'))
            BM.long = readRDS(file.path(.datadir, 'webapp_data/mbilan/bilan_month_long.rds'))
            pars = readRDS(file.path(.datadir, 'webapp_data/pars/pars.rds'))
            u = readRDS(file.path(.datadir, 'webapp_data/uzivani/06_16/uzivani_na_nahraz.rds'))
            QD = readRDS(file.path(.datadir, 'webapp_data/chmu/QD.rds'))
          }
         
  ) )
  
  name <- c(as.character(substitute(list(...))[-1L]))

  if (length(name) == 0) return(message('Dostupne datasety:\n\n', paste(names(E[[1]])[-(1:2)], collapse = '\n'), '\n'))

  give_paths()

  e = objects()

  for (j in 1:length(name)){
    eval(E)
  }

  e2 = objects()
  n = e2[!e2 %in% c(e, 'e', 'j', 'E')]

  for (i in 1:length(n)){
   assign(n[i], eval(parse(text = n[i])), envir = .GlobalEnv)
  }

  cat('\nLOADED OBJECTS: \n=====================\n')
  cat(n, sep = '\n')

}