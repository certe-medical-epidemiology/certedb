# ===================================================================== #
#  An R package by Certe:                                               #
#  https://github.com/certe-medical-epidemiology                        #
#                                                                       #
#  Licensed as GPL-v2.0.                                                #
#                                                                       #
#  Developed at non-profit organisation Certe Medical Diagnostics &     #
#  Advice, department of Medical Epidemiology.                          #
#                                                                       #
#  This R package is free software; you can freely use and distribute   #
#  it for both personal and commercial purposes under the terms of the  #
#  GNU General Public License version 2.0 (GNU GPL-2), as published by  #
#  the Free Software Foundation.                                        #
#                                                                       #
#  We created this package for both routine data analysis and academic  #
#  research and it was publicly released in the hope that it will be    #
#  useful, but it comes WITHOUT ANY WARRANTY OR LIABILITY.              #
# ===================================================================== #


#' MMB-gegevens ophalen van MySQL-/MariaDB-database
#'
#' @description Gegevens van Certe Medische Microbiologie uit de Certe-database downloaden. De benodigde tabellen worden als \code{LEFT JOIN} automatisch toegevoegd op basis van de input bij \code{where} en \code{select}. Nadien kan met \code{\link{qry}} de query bekeken worden, die als eigenschap bij het object opgeslagen wordt.
#'
#' \code{certedb_getmmb} haalt orders en resultaten op, met uitslagen. \cr
#' \code{certedb_getmmb_tat} haalt van deze orders de doorlooptijden op (TAT = Turn Around Time).
#' @rdname certedb_getmmb
#' @param dates Standaard is dit hele jaar. Geldige opties zijn: leeg (selecteert dit hele jaar) of een vector met 2 datums (bestaande uit oudste en nieuwste datum) of een enkele datum (einddatum wordt laatste dag van dat jaar).
#' @param where Standaard is leeg. Syntax om toe te voegen aan de WHERE-clausule. \cr \cr
#' R-syntax wordt omgezet naar SQL-syntax, zie Examples. Deze syntax wordt geëvalueerd, dus gebruik van variabelen is ook mogelijk, zoals \code{certedb_getmmb(where = where(o.jaar == mijnjaar))}. \cr \cr
#' \strong{NB.} Wanneer tabelvelden in meerdere brontabellen voorkomen, moet de tabelreferentie opgegeven worden. Zie Details voor de tabelreferenties en Examples voor voorbeelden.
#' @param select_preset,preset Standaard is \code{"mmb"} of \code{"tat"} bij doorlooptijden. Variabelen om te selecteren volgens de voorgedefinieerde lijst met variabelen, zie \code{\link{preset.list}}. Kan ook een vector met meerdere presets zijn. Voor het selecteren van de eerste preset uit de huidige map (bestandsnaam moet beginnen met \code{"preset"}), gebruik \code{select_preset = \link{preset.thisfolder}()}.
#' @param select Standaard is leeg. Variabelen om \strong{handmatig} te selecteren. Deze kunnen het best geselecteerd worden met \code{\link{db}} en dit overschrijft \code{select_preset}.
#' @param add_cols Standaard is leeg. Variabelen in een vector of \code{list} om \strong{extra} te selecteren. Deze kunnen het best geselecteerd worden met \code{\link{db}}. Gebruik \code{add_cols = c("naam" = db$t.kolom)} voor \code{"t.kolom AS naam"}.
#' @param limit Standaard is 10.000.000. Het aantal rijen dat maximaal opgehaald moet worden.
#' @param con Standaard is leeg, waarmee de verbinding gemaakt wordt op basis van de omgevingsvariabelen van de huidige gebruiker: \code{"DB_HOST"}, \code{"DB_PORT"}, \code{"DB_USERNAME"} en \code{"DB_PASSWORD"}.
#' @param dbname Standaard is \code{"certemmb"}. Naam van de database die geselecteerd moet worden. Wordt genegeerd als \code{con} al een bestaande verbinding is.
#' @param info Standaard is \code{TRUE}. Printen van voortgang en het uiteindelijke aantal rijen en kolommen dat gedownload is.
#' @param first_isolates Standaard is \code{FALSE}. Bepaling van eerste isolaten toevoegen.
#' @param eucast_rules Standaard is \code{"all"}. EUCAST expert rules toepassen, zie \code{\link{eucast_rules}}.
#' @param MIC Standaard is \code{FALSE}. Toevoegen van MIC's aan alle RSI-kolommen van de de standaard query (i.e. zonder dat \code{select} gebruikt wordt).
#' @param zipcodes Standaard is \code{FALSE}. Toevoegen van postcodes van de patiënt, als \code{p.postcode}. Met name voor gebruik met \code{\link{get_map}} en \code{\link{plot2.map}}.
#' @param ziplength Standaard is \code{4}. De gewenste lengte van de postcode.
#' @param inhabitants Standaard is \code{FALSE}, maar alleen zinvol als \code{zipcodes = TRUE}. Voegt automatisch de data van CBS toe, die opgeslagen liggen in de tabel \code{temporary_certemm_cbs}, op basis van de postcodelengte die opgegeven is met \code{ziplength}.
#' @param tat_hours Standaard is \code{TRUE}. Turn-around-times toevoegen in uren \emph{(alleen voor doorlooptijden)}.
#' @param only_real_patients Standaard is \code{TRUE}. Hiermee worden alleen daadwerkelijke patiënten gedownload (geen rondzendingen en testorders). Dit voegt automatisch \code{AND u.is_echte_patient = TRUE} toe aan de \code{WHERE}.
#' @param only_conducted_tests Standaard is \code{TRUE}. Hiermee worden alleen verrichte testen gedownload (geen testen die niet verricht zijn). Dit voegt automatisch \code{AND u.is_verricht = TRUE} toe aan de \code{WHERE}.
#' @param only_validated  Standaard is \code{FALSE} bij \code{certedb_getmmb()} en \code{TRUE} bij \code{certedb_getmmb_tat()}. Hiermee worden alleen geautoriseerde uitslagen gedownload. Dit voegt automatisch \code{AND u.uitslag_int <> '(in behandeling)'} toe aan de \code{WHERE}.
#' @param only_show_query Standaard is \code{FALSE}. Draait de query niet, maar toont hem alleen.
#' @param review_where Standaard is \code{FALSE} als \code{info = FALSE} of de sessie in niet-interactief (RMarkdown). Bij een interactieve sessie (d.w.z. niet in RMarkdown) wordt de WHERE weergegeven die de gebruiker moet accorderen. Dit kan standaard op \code{FALSE} gezet worden met \code{\link{getOption}("review_where")}.
#' @param unselect_all_same Standaard is \code{FALSE}. Kolommen excluderen die maar 1 unieke waarde bevatten met \code{select(!\link{all_same}())}.
#' @param unselect_all_empty Standaard is \code{FALSE}. Kolommen excluderen die helemaal leeg zijn met \code{select(!\link{all_empty}())}.
#' @param ... Overige parameters die doorgegeven worden aan \code{\link{certedb_query}}, zoals \code{auto_transform} en \code{timezone}.
#' @details Voor gebruik van \code{where} staan hieronder de tabelreferenties. Deze zijn ook beschikbaar via de \emph{list} \code{\link[certetools]{db}}:
#'   \itemize{
#'     \item{\strong{\code{a}}} \cr {\code{temporary_certemm_aanvragers_praktijken}}
#'     \item{\strong{\code{aanvr}}} \cr {\code{temporary_certemm_aanvragers_praktijken}}
#'     \item{\strong{\code{beh}}} \cr {\code{temporary_certemm_aanvragers_praktijken}}
#'     \item{\strong{\code{b}}} \cr {\code{temporary_certemm_bacterienlijst}}
#'     \item{\strong{\code{cbs}}} \cr {\code{temporary_certemm_cbs} (joint aan \code{certemm_pat.postcode})}
#'     \item{\strong{\code{d}}} \cr {\code{temporary_certemm_aanmaakdatums}}
#'     \item{\strong{\code{dlt}} \emph{(alleen voor doorlooptijden)}} \cr {\code{temporary_certemm_doorlooptijden}}
#'     \item{\strong{\code{i}}} \cr {\code{temporary_certemm_isolaten_rsi}}
#'     \item{\strong{\code{i_mic}}} \cr {\code{temporary_certemm_isolaten_mic}}
#'     \item{\strong{\code{l_instelling}}} \cr {\code{temporary_certemm_locaties}}
#'     \item{\strong{\code{l_ontvangst}}} \cr {\code{temporary_certemm_locaties}}
#'     \item{\strong{\code{l_uitvoer}}} \cr {\code{temporary_certemm_locaties}}
#'     \item{\strong{\code{m}}} \cr {\code{temporary_certemm_materiaalgroepen}}
#'     \item{\strong{\code{o}}} \cr {\code{temporary_certemm_orders}}
#'     \item{\strong{\code{p}}} \cr {\code{certemm_pat}}
#'     \item{\strong{\code{pat}}} \cr {\code{temporary_certemm_patienten}}
#'     \item{\strong{\code{t}}} \cr {\code{temporary_certemm_testgroepen}}
#'     \item{\strong{\code{u}}} \cr {\code{temporary_certemm_uitslagen}}
#'   }
#'
#'   Locatie wordt dus op \strong{drie manieren gekoppeld}; als \code{l_instelling} (op \code{o.instelling}, zoals huisarts), als \code{l_ontvangst} (op \code{o.recsitenb}, zoals Assen) en als \code{l_uitvoer} (op \code{u.uitvafd}, zoals Moleculair).
#' @importFrom crayon silver
#' @export
#' @seealso \code{\link{certedb_query}} voor het direct gebruik van query's.
#' @examples
#' 
certedb_getmmb <- function(dates = NULL,
                           where = NULL,
                           select_preset = "mmb",
                           preset = "mmb",
                           select = NULL,
                           add_cols = NULL,
                           limit = 10000000,
                           con = NULL,
                           dbname = 'certemmb',
                           info = TRUE,
                           first_isolates = FALSE,
                           eucast_rules = "all",
                           MIC = FALSE,
                           zipcodes = FALSE,
                           ziplength = 4,
                           inhabitants = FALSE,
                           tat_hours = FALSE,
                           only_real_patients = TRUE,
                           only_conducted_tests = TRUE,
                           only_validated = FALSE,
                           only_show_query = FALSE,
                           review_where = as.logical(min(c(getOption("review_where", TRUE),
                                                           info,
                                                           interactive()))),
                           unselect_all_same = FALSE,
                           unselect_all_empty = FALSE,
                           ...) {
  eucast_rules_setting <- eucast_rules
  if (is.function(eucast_rules_setting)) {
    eucast_rules_setting <- "all"
  }
  
  dates_depsub <- deparse(substitute(dates))
  where_depsub <- deparse(substitute(where))
  if (all(gsub("-", "", where_depsub, fixed = TRUE) %like% "^[0-9]+$")) {
    #stop("Invalid `where`, it contains a date or year. ")
    warning("Using `where` as `dates[2]` (", where_depsub, ")", immediate. = TRUE, call. = FALSE)
    where <- NULL
    if (length(dates) == 1) {
      dates <- c(dates, where_depsub)
    } else if (length(dates) == 2) {
      dates[2] <- where_depsub
    }
  }
  
  dots <- list(...) %>% unlist()
  if (length(dots) != 0) {
    dots.names <- dots %>% names()
    if ('startdate' %in% dots.names | 'enddate' %in% dots.names) {
      dates <- character(2)
    }
    if ('startdate' %in% dots.names) {
      if (!is.null(dots[which(dots.names == 'startdate')])) {
        dates[1] <- dots[which(dots.names == 'startdate')]
        if (dates[1] %unlike% '[12][90][0-9][0-9]-[01][0-9]-[0123][0-9]') {
          dates[1] <- as.character(as.Date(as.integer(dates[1]), origin = "1970-01-01"))
        }
      }
    }
    if ('enddate' %in% dots.names) {
      if (!is.null(dots[which(dots.names == 'enddate')])) {
        dates[2] <- dots[which(dots.names == 'enddate')]
        if (dates[2] %unlike% '[12][90][0-9][0-9]-[01][0-9]-[0123][0-9]') {
          dates[2] <- as.character(as.Date(as.integer(dates[2]), origin = "1970-01-01"))
        }
      }
    }
    if ('EUCAST_rules' %in% dots.names) {
      eucast_rules_setting <- dots[which(dots.names == 'EUCAST_rules')]
    }
  }
  
  # datumbereik
  if (is.null(dates)) {
    # dit hele jaar
    dates <- c(start_of_this_year(), end_of_this_year())
  } else {
    # alleen jaar opgegeven
    if (dates[1] %like% "^[12][90][0-9][0-9]$") {
      dates[1] <- paste0(dates[1], "-01-01")
    }
    if (dates[2] %like% "^[12][90][0-9][0-9]$") {
      dates[2] <- paste0(dates[2], "-12-31")
    }
    
    dates <- as.character(dates)
    dates_int <- suppressWarnings(as.integer(dates))
    
    if (!is.na(dates_int[1])) {
      dates[1] <- as.character(as.Date(as.integer(dates[1]), origin = "1970-01-01"))
    }
    if (!is.na(dates_int[2])) {
      dates[2] <- as.character(as.Date(as.integer(dates[2]), origin = "1970-01-01"))
    }
    
    if (all(dates %unlike% '^[12][90][0-9][0-9]-[01][0-9]-[0123][0-9]$') & all(dates %in% c("", NA))) {
      # bestaat nog niet uit yyyy-mm-dd
      stop("Invalid value(s) for `dates`. Use format yyyy-mm-dd.")
    }
    if (length(dates) == 1 | (identical(dates[2], "") | identical(dates[2], NA_character_))) {
      # datum tot einde van jaar
      dates <- c(dates[1], paste0(year(dates[1]), '-12-31'))
    } else if (length(dates) > 2) {
      stop("`dates` can have a maximum length of 2.")
    }
    
    if (is.na(dates[1])) {
      warning("First `dates` element is NA.", immediate. = TRUE, call. = FALSE)
    }
    if (is.na(dates[2])) {
      warning("Second `dates` element is NA.", immediate. = TRUE, call. = FALSE)
    }
  }
  dates <- paste0("'", gsub('["\']', '', dates), "'")
  
  if (is.null(select)) {
    if (preset != "mmb") {
      select <- preset.read(preset)
    } else {
      select <- preset.read(select_preset)
    }
  }
  select <- select_translate_asterisk(select)
  
  # extra kolommen
  if (!is.null(add_cols)) {
    add_cols <- unlist(add_cols) # wanneer add_cols = list(a = t.kolom) gebruikt wordt
    select <- c(select, select_translate_asterisk(add_cols))
  }
  
  # MIC toevoegen
  if (MIC == TRUE | isTRUE(list(...)$mic)) {
    select <- c(select,
                "i_mic.moxa_mic",
                "i_mic.peni_mic",
                "i_mic.bepe_mic",
                "i_mic.oxac_mic",
                "i_mic.clox_mic",
                "i_mic.meti_mic",
                "i_mic.amox_mic",
                "i_mic.amcl_mic",
                "i_mic.ampi_mic",
                "i_mic.amsu_mic",
                "i_mic.azlo_mic",
                "i_mic.pipe_mic",
                "i_mic.pita_mic",
                "i_mic.tazo_mic",
                "i_mic.tica_mic",
                "i_mic.ticl_mic",
                "i_mic.cfra_mic",
                "i_mic.cfix_mic",
                "i_mic.cfac_mic",
                "i_mic.cfal_mic",
                "i_mic.czol_mic",
                "i_mic.cfep_mic",
                "i_mic.cfur_mic",
                "i_mic.cfam_mic",
                "i_mic.cfox_mic",
                "i_mic.cfsc_mic",
                "i_mic.cfot_mic",
                "i_mic.cfpo_mic",
                "i_mic.cfta_mic",
                "i_mic.cftr_mic",
                "i_mic.cfsu_mic",
                "i_mic.cefa_mic",
                "i_mic.gent_mic",
                "i_mic.gehl_mic",
                "i_mic.tobr_mic",
                "i_mic.neti_mic",
                "i_mic.amik_mic",
                "i_mic.kana_mic",
                "i_mic.neom_mic",
                "i_mic.siso_mic",
                "i_mic.sulf_mic",
                "i_mic.trim_mic",
                "i_mic.trsu_mic",
                "i_mic.nitr_mic",
                "i_mic.fosf_mic",
                "i_mic.line_mic",
                "i_mic.tsta_mic",
                "i_mic.nali_mic",
                "i_mic.cnox_mic",
                "i_mic.pizu_mic",
                "i_mic.norf_mic",
                "i_mic.lome_mic",
                "i_mic.cipr_mic",
                "i_mic.moxi_mic",
                "i_mic.levo_mic",
                "i_mic.oflo_mic",
                "i_mic.vanc_mic",
                "i_mic.teic_mic",
                "i_mic.tetr_mic",
                "i_mic.tige_mic",
                "i_mic.doxy_mic",
                "i_mic.eryt_mic",
                "i_mic.qida_mic",
                "i_mic.azit_mic",
                "i_mic.imip_mic",
                "i_mic.mero_mic",
                "i_mic.mino_mic",
                "i_mic.aztr_mic",
                "i_mic.chlo_mic",
                "i_mic.poly_mic",
                "i_mic.baci_mic",
                "i_mic.coli_mic",
                "i_mic.roxi_mic",
                "i_mic.linc_mic",
                "i_mic.mupi_mic",
                "i_mic.spec_mic",
                "i_mic.dapt_mic",
                "i_mic.novo_mic",
                "i_mic.clin_mic",
                "i_mic.incl_mic",
                "i_mic.metr_mic",
                "i_mic.inh_mic",
                "i_mic.rifa_mic",
                "i_mic.pyra_mic",
                "i_mic.etha_mic",
                "i_mic.stre_mic",
                "i_mic.sthl_mic",
                "i_mic.cycl_mic",
                "i_mic.prot_mic",
                "i_mic.clof_mic",
                "i_mic.clar_mic",
                "i_mic.rifb_mic",
                "i_mic.ansa_mic",
                "i_mic.ethi_mic",
                "i_mic.thio_mic",
                "i_mic.amph_mic",
                "i_mic.fluz_mic",
                "i_mic.fluo_mic",
                "i_mic.5flu_mic",
                "i_mic.itra_mic",
                "i_mic.vori_mic",
                "i_mic.keto_mic",
                "i_mic.mico_mic",
                "i_mic.nyst_mic",
                "i_mic.fusi_mic",
                "i_mic.econ_mic",
                "i_mic.clot_mic",
                "i_mic.casp_mic",
                "i_mic.anid_mic",
                "i_mic.posa_mic")
  }
  if (zipcodes == TRUE) {
    select <- c(select, "p.postcode")
    if (info == TRUE) {
      message("Note: Adding 'p.postcode'.")
    }
  }
  
  select <- gsub(",$", "", select)
  select <- unique(select)
  select <- select %>% concat(',\n  ')
  
  query <- paste0('SELECT\n  ',
                  select, '\n',
                  'FROM\n  ',
                  '{from}\n',
                  'WHERE\n  ',
                  '{datelimit}',
                  '{additional_where}')
  
  ignore_start_stop <- FALSE
  
  if (tat_hours == FALSE) {
    # geen TAT, moet nog gedeparsed worden
    if (any(dates_depsub %like% '^where\\(')) {
      #enddate <- NULL
      where <- where_R2SQL(dates_depsub, info = info)
    } else {
      where <- where_R2SQL(deparse(substitute(where)), info = info)
    }
    # deparse-foutje als getmm_tat gebruikt wordt:
    if (where == 'enddate') {
      where <- ''
    }
  }
  
  # extra where
  if (where != '') {
    if (where %like% '[.]ontvangstdatum ' | where %like% '[.]jaar ') {
      if (info == TRUE) {
        warning(crayon::bold('`dates` will be ignored because date criteria has been set with `where`.'),
                call. = FALSE,
                immediate. = TRUE)
      }
      ignore_start_stop <- TRUE
      query <- query %>%
        sub("{additional_where}", where, ., fixed = TRUE)
    } else {
      query <- query %>%
        sub("{additional_where}", paste0('\n  AND ', where), ., fixed = TRUE)
    }
  } else {
    query <- query %>%
      sub("{additional_where}", '', ., fixed = TRUE)
  }
  
  if (ignore_start_stop == FALSE) {
    
    if (dates[1] > dates[2]) {
      stop('First date cannot be later than second date, it would return zero results.')
    }
    query <- query %>%
      sub("{datelimit}", paste('o.ontvangstdatum BETWEEN', dates[1], 'AND', dates[2]), ., fixed = TRUE)
  } else {
    query <- query %>%
      sub("{datelimit}", '', ., fixed = TRUE)
  }
  
  if (only_real_patients == TRUE) {
    if (query %like% '[.]is_echte_patient =') {
      stop('`is_echte_patient` cannot be set while `only_real_patients = TRUE`.')
    }
    query <- paste(query, '\n  AND u.is_echte_patient = TRUE')
  }
  if (only_conducted_tests == TRUE) {
    if (query %like% '[.]is_verricht =') {
      stop('`is_verricht` cannot be set while `only_conducted_tests = TRUE`.')
    }
    query <- paste(query, '\n  AND u.is_verricht = TRUE')
  }
  if (!missing(only_validated)) {
    if (query %like% '[.]uitslag_int =') {
      stop('`uitslag_int` cannot be set while `only_validated` is set.')
    }
    if (only_validated == TRUE) {
      query <- paste(query, "\n  AND uitslag_int <> '(in behandeling)'")
    }
  }
  
  
  # FROM clausule
  # if (query %like% ' dlt[.]') {
  #   # primair van dlt halen, orders en uitslagen eraan
  #   from <- paste0('temporary_certemm_doorlooptijden AS dlt',
  #                  "\n  LEFT JOIN\n  ",
  #                  "temporary_certemm_orders AS o ON o.ordernr = dlt.ordernr",
  #                  "\n  LEFT JOIN\n  ",
  #                  "temporary_certemm_uitslagen AS u ON u.ordernr_testcode_mtrlcode = dlt.ordernr_testcode_mtrlcode")
  # } else {
  # primair van uitslagen halen, orders eraan
  from <- paste0('temporary_certemm_uitslagen AS u',
                 "\n  LEFT JOIN\n  ",
                 "temporary_certemm_orders AS o ON o.ordernr = u.ordernr")
  # }
  from <- from_addjoins(query, from)
  query <- sub("{from}", from, query, fixed = TRUE)
  
  if (only_show_query == TRUE) {
    sql(paste0('\n', query, '\n'))
  } else {
    
    if (review_where == TRUE & base::interactive() == TRUE) {
      note <- ""
      if (is.null(getOption("review_where")) & runif(1) < 0.5) {
        # notitie in helft van de gevallen, wanneer gebruiker dit niet heeft ingesteld
        note <- silver("\nStel dit standaard in met options(review_where = TRUE/FALSE).")
      }
      # met qry_beautify, omdat dit in certedb_query ook gebruikt wordt
      choice <- utils::menu(choices = c("OK", "Annuleren", "Hele query weergeven"),
                            title = paste0(paste0("Query met deze WHERE uitvoeren?", note, "\n\n"),
                                           gsub("(.*)\nWHERE\n  (.*)", "\\2", qry_beautify(query))))
      if (choice == 3) {
        cat(paste0("\nHele query:\n\n", qry_beautify(query), "\n\n"))
        choice <- utils::menu(choices = c("OK", "Annuleren"),
                              title = "\nQuery uitvoeren?")
      }
      if (choice %in% c(0, 2)) { # 'Enter an item from the menu, or 0 to exit'
        return(invisible())
      }
    }
    
    starttime_total <- Sys.time()
    
    totaal <- certedb_query(query = query, con = con, dbname = dbname, info = info, limit = limit, ...)
    
    if (tat_hours == TRUE & select %like% ' dlt[.]') {
      # berekeningen in tijd toevoegen
      certedb_timestamp("Calculating time differences in hours...", print = info)
      totaal <- totaal %>%
        
        mutate(val1_usr_1e = val1_usr_1e %>% as.character(),
               val2_usr_1e = val2_usr_1e %>% as.character(),
               aut_usr_1e = aut_usr_1e %>% as.character(),
               val1_usr_def = val1_usr_def %>% as.character(),
               val2_usr_def = val2_usr_def %>% as.character(),
               aut_usr_def = aut_usr_def %>% as.character()) %>%
        
        mutate(val1_1e.val2_1e = (difftime(val2_1e, val1_1e, units = 'mins') / 60) %>% as.double(),
               val2_1e.aut_1e = (difftime(aut_1e, val2_1e, units = 'mins') / 60) %>% as.double(),
               ontvangst.val2_1e = (difftime(val2_1e, ontvangstdatumtijd, units = 'mins') / 60) %>% as.double(),
               ontvangst.aut_1e = (difftime(aut_1e, ontvangstdatumtijd, units = 'mins') / 60) %>% as.double(),
               
               val1_def.val2_def = (difftime(val2_def, val1_def, units = 'mins') / 60) %>% as.double(),
               val2_def.aut_def = (difftime(aut_def, val2_def, units = 'mins') / 60) %>% as.double(),
               ontvangst.val2_def = (difftime(val2_def, ontvangstdatumtijd, units = 'mins') / 60) %>% as.double(),
               ontvangst.aut_def = (difftime(aut_def, ontvangstdatumtijd, units = 'mins') / 60) %>% as.double(),
               
               dgn_ontvangst.val1_1e = as.integer(as.Date(val1_1e) - ontvangstdatum),
               dgn_ontvangst.val1_def = as.integer(as.Date(val1_def) - ontvangstdatum),
               dgn_ontvangst.val2_1e = as.integer(as.Date(val2_1e) - ontvangstdatum),
               dgn_ontvangst.val2_def = as.integer(as.Date(val2_def) - ontvangstdatum),
               dgn_ontvangst.aut_1e = as.integer(as.Date(aut_1e) - ontvangstdatum),
               dgn_ontvangst.aut_def = as.integer(as.Date(aut_def) - ontvangstdatum),
               
               weekdag = weekdays(as.Date(ontvangstdatum), abbreviate = FALSE)
        )
      
      certedb_timestamp("Adding region/year/quarter column `reg_jr_q`...", print = info)
      totaal <- totaal %>%
        mutate(reg_jr_q = paste0(noord_zuid %>% substr(1, 1), '|', format2(ontvangstdatum, 'yyyy-QQ')))
      
    }
    
    if (select %like% ' i[.]' & nrow(totaal) > 0) {
      if (!"mo" %in% colnames(totaal) & "bacteriecode" %in% colnames(totaal)) {
        if (!all(is.na(totaal$bacteriecode))) {
          suppressWarnings(
            totaal <- totaal %>%
              rename(bacteriecode_oud = bacteriecode) %>%
              mutate(bacteriecode = as.mo(bacteriecode_oud))
          )
        }
      }
      if (all(is.na(totaal$bacteriecode))) {
        message('Note: No isolates available.')
      } else {
        if (eucast_rules_setting != FALSE & "bacteriecode" %in% colnames(totaal)) {
          certedb_timestamp(paste('Applying EUCAST', eucast_rules_setting, 'rules...'), print = info, appendLF = FALSE)
          totaal <- suppressMessages(suppressWarnings(AMR::eucast_rules(totaal, col_mo = 'bacteriecode', rules = eucast_rules_setting, info = FALSE)))
          certedb_timestamp('OK', print = info, timestamp = FALSE, appendLF = TRUE)
        }
        
        if (first_isolates == TRUE) {
          totaal <- suppressMessages(tbl_first_isolates(totaal, info = info, timestamp = TRUE))
        }
        
        difference <- difftime(Sys.time(), starttime_total, units = 'mins')
        if (eucast_rules_setting == TRUE | first_isolates == TRUE) {
          certedb_timestamp('Done.', print = info)
        }
        if (info == TRUE) {
          cat('\nTotal run time:', diff_min_sec(difference), '\n\n')
        }
        
        if (info == TRUE) {
          if (eucast_rules_setting == FALSE) {
            message('Note: No EUCAST expert rules applied.')
          }
          if (first_isolates == FALSE) {
            message('Note: No first isolates determined.')
          }
        }
        
      }
    }
    
    if (zipcodes == TRUE & ziplength < 6) {
      certedb_timestamp("Transforming zip codes...", print = info)
      totaal <- totaal %>% mutate(postcode = postcode %>% substr(1, ziplength))
      if (inhabitants == TRUE) {
        certedb_timestamp("Joining inhabitants data...", print = info)
        inhabitants <- certedb_query("SELECT * FROM temporary_certemm_cbs", info = FALSE)
        totaal <- totaal %>% left_join(inhabitants, by = "postcode")
      }
    }
    
    allsame <- all_same(totaal, na.rm = FALSE)
    allempty <- all_empty(totaal)
    if (info == TRUE) {
      if (missing(unselect_all_same) && unselect_all_same == FALSE && length(allsame) > 0) {
        message('Column(s) with only 1 unique value: ', paste0("'", allsame, "'", collapse = ", "), " - consider `unselect_all_same = TRUE`")
      }
      if (missing(unselect_all_empty) && unselect_all_empty == FALSE && length(allempty) > 0) {
        message('Column(s) with only empty values: ', paste0("'", allempty, "'", collapse = ", "), " - consider `unselect_all_empty = TRUE`")
      }
    }
    if (unselect_all_same == TRUE & length(allsame) > 0) {
      certedb_timestamp("Unselecting column(s) with 1 unique value: ",
                        paste0("'", allsame, "'", collapse = ", ")
                        , "...", print = info)
      totaal <- totaal %>% select(!all_same(na.rm = FALSE))
    }
    if (unselect_all_empty == TRUE & length(allempty) > 0) {
      certedb_timestamp("Unselecting empty column(s): ",
                        paste0("'", allempty, "'", collapse = ", ")
                        , "...", print = info)
      totaal <- totaal %>% select(!all_empty())
    }
    
    qry(totaal) <- query
    # label voor hele df
    # label(totaal) <- paste0(Sys.getenv("R_USERNAME"), ", ",
    #                         format2(Sys.time(), "yyyy-mm-dd HH:MM:SS"))
    # label per kolom, uitleg van antibiotica
    # label(totaal) <- c("5flu" = "Flucytosine",
    #                    amcl = "Amoxicilline/clavulaanzuur",
    #                    amik = "Amikacine",
    #                    amox = "Amoxicilline",
    #                    amph = "Amfotericine B",
    #                    ampi = "Ampicilline",
    #                    amsu = "Ampicilline/sulbactam",
    #                    anid = "Anidulafungine",
    #                    ansa = "Ansamycine",
    #                    azit = "Azitromycine",
    #                    azlo = "Azlocilline",
    #                    aztr = "Aztreonam",
    #                    baci = "Bacitacine",
    #                    bepe = "Benzylpenicilline",
    #                    casp = "Caspofungine",
    #                    cefa = "Cefaloridine",
    #                    cfac = "Cefaclor",
    #                    cfal = "Cefalotine",
    #                    cfam = "Cefamandol",
    #                    cfep = "Cefepim",
    #                    cfix = "Cefixim",
    #                    cfot = "Cefotaxim",
    #                    cfox = "Cefoxitine",
    #                    cfpo = "Cefpodoxim",
    #                    cfra = "Cefradine",
    #                    cfsc = "Cefoxitine screen",
    #                    cfsu = "Cefsulodine",
    #                    cfta = "Ceftazidim",
    #                    cftr = "Ceftriaxon",
    #                    cfur = "Cefuroxim",
    #                    chlo = "Chlooramfenicol",
    #                    cipr = "Ciprofloxacine",
    #                    clar = "Claritromycine",
    #                    clin = "Clindamycine",
    #                    clof = "Clofazimine",
    #                    clot = "Clotrimazol",
    #                    clox = "Flucloxacilline",
    #                    cnox = "Cinoxacine",
    #                    coli = "Colistine",
    #                    cycl = "Cycloserine",
    #                    czol = "Cefazoline",
    #                    dapt = "Daptomycine",
    #                    doxy = "Doxycycline",
    #                    dum = "dummy antibioticum",
    #                    econ = "Econazol",
    #                    eryt = "Erytromycine",
    #                    etha = "Ethambutol",
    #                    ethi = "Ethionamide",
    #                    fluo = "Fluorocytosine",
    #                    fluz = "Fluconazol",
    #                    fosf = "Fosfomycine",
    #                    fusi = "Fusidinezuur",
    #                    gehl = "Gentamicine high level",
    #                    gent = "Gentamicine",
    #                    imip = "Imipenem",
    #                    incl = "ind. clinda",
    #                    inh = "Isoniazide",
    #                    itra = "Itraconazol",
    #                    kana = "Kanamycine",
    #                    keto = "Ketoconazol",
    #                    levo = "Levofloxacine",
    #                    linc = "Lincomycine",
    #                    line = "Linezolid",
    #                    lome = "Lomefloxacine",
    #                    mero = "Meropenem",
    #                    meti = "Meticilline",
    #                    metr = "Metronidazol",
    #                    mico = "Miconazol",
    #                    mino = "Minocycline",
    #                    moxa = "Moxalactamase",
    #                    moxi = "Moxifloxacine",
    #                    mupi = "Mupirocine",
    #                    nali = "Nalidixinezuur",
    #                    neom = "Neomycine",
    #                    neti = "Netilmicine",
    #                    nitr = "Nitrofurantoine",
    #                    norf = "Norfloxacine",
    #                    novo = "Novobiocine",
    #                    nyst = "Nystatine",
    #                    oflo = "Ofloxacine",
    #                    oxac = "Oxacilline",
    #                    peni = "Penicilline",
    #                    pipe = "Piperacilline",
    #                    pita = "Piperacilline/tazobactam",
    #                    pizu = "Pipemidinezuur",
    #                    poly = "Polymyxine B",
    #                    posa = "Posaconazol",
    #                    prot = "Protionamide",
    #                    pyra = "Pyrazinamide",
    #                    qida = "Quinupristine/dalfopristine",
    #                    rifa = "Rifampicine",
    #                    rifb = "Rifabutine",
    #                    roxi = "Roxitromycine",
    #                    siso = "Sisomicine",
    #                    spec = "Spectinomycine",
    #                    sthl = "Streptomycine high level",
    #                    stre = "Streptomycine",
    #                    sulf = "Sulfamethoxazol",
    #                    tazo = "Tazobactam",
    #                    teic = "Teicoplanine",
    #                    tes2 = "test automatisering",
    #                    test = "test automatisering",
    #                    tetr = "Tetracycline",
    #                    thio = "Metisazon",
    #                    tica = "Ticarcilline",
    #                    ticl = "Ticarcilline/clavulaanzuur",
    #                    tige = "Tigecycline",
    #                    tobr = "Tobramycine",
    #                    trim = "Trimethoprim",
    #                    trsu = "Cotrimoxazol",
    #                    tsta = "Testantibioticum",
    #                    vanc = "Vancomycine",
    #                    vori = "Voriconazol",
    #                    xct = "Cefotaxim",
    #                    xctl = "cefotaxim+clavulaanzuur",
    #                    xpm = "Cefepim",
    #                    xpml = "cefepim+clavulaanzuur",
    #                    xtz = "Ceftazidim",
    #                    xtzl = "ceftazidim+clavulaanzuur")
    totaal
  }
}

#' @rdname certedb_getmmb
#' @export
certedb_getmmb_tat <- function(dates = NULL,
                               where = NULL,
                               add_cols = NULL,
                               limit = 10000000,
                               con = NULL,
                               dbname = 'certemmb',
                               info = TRUE,
                               only_real_patients = TRUE,
                               only_conducted_tests = TRUE,
                               only_validated = TRUE,
                               only_show_query = FALSE,
                               ...) {
  
  where_test <- deparse(substitute(where))
  if (any(where_test %like% '^where\\(')) {
    where <- where_R2SQL(where_test, info = info)
  } else {
    where <- where_R2SQL(deparse(substitute(where)), info = info)
  }
  # deparse-foutje als getmm_tat gebruikt wordt:
  if (where == 'enddate') {
    where <- ''
  }
  
  certedb_getmmb(dates = dates,
                 where = where,
                 add_cols = add_cols,
                 limit = limit,
                 con = con,
                 dbname = dbname,
                 info = info,
                 only_real_patients = only_real_patients,
                 only_conducted_tests = only_conducted_tests,
                 only_validated = only_validated,
                 only_show_query = only_show_query,
                 select_preset = "tat",
                 first_isolates = FALSE,
                 eucast_rules = FALSE,
                 MIC = FALSE,
                 tat_hours = TRUE,
                 ...)
}

select_translate_asterisk <- function(select) {
  select.bak <- select
  select_list <- select %>% as.list()
  for (i in 1:length(select)) {
    # alle tabel.* uitschrijven naar c(tabel.a, tabel.b, ...)
    if (select[i] %like% '[.][*]') {
      found <- gregexpr('[.][*]', select[i])
      select_tbl <- substr(select[i], 1, found %>% as.integer() - 1)
      select_items <-
        certetools::db[names(certetools::db) %like% paste0('^', select_tbl, '[.][a-z]+')] %>%
        unlist() %>%
        unname()
      select_list[[i]] <- select_items
    }
  }
  select_list <- select_list %>% unlist()
  # ondersteuning voor add_cols = c("test" = db$m.mtrlcode) -> "m.mtrlcode AS test"
  for (i in 1:length(select.bak)) {
    if (!is.null(names(select.bak)[i])) {
      if (names(select.bak)[i] != "") {
        select_list[i] <- paste(select_list[i], "AS", names(select.bak)[i])
      }
    }
  }
  select_list
}

from_addjoins <- function(query, from) {
  # dependencies:
  
  # alles:
  # u < o
  # u < d (datums)
  # u < l_uitvoer
  # u < o < l_instelling
  # u < o < l_ontvangst
  # u < t
  # u < m
  # u < o < a
  # u < o < aanvr
  # u < o < beh
  # u < o < p
  # u < i
  # u < i_mic
  # u < b
  
  if (query %like% ' dlt[.]') {
    from <- paste0(from, "\n  ",
                   "LEFT JOIN\n  ",
                   "temporary_certemm_doorlooptijden AS dlt ON dlt.ordernr_testcode_mtrlcode = u.ordernr_testcode_mtrlcode")
  }
  if (query %like% ' r[.]') {
    from <- paste0(from, "\n  ",
                   "LEFT JOIN\n  ",
                   "certemm_res AS r ON r.ordernr = u.ordernr AND r.anamc = u.testcode AND r.mtrlcode = u.mtrlcode AND r.stamteller = u.stam")
  }
  if (query %like% ' d[.]') {
    from <- paste0(from, "\n  ",
                   "LEFT JOIN\n  ",
                   "temporary_certemm_aanmaakdatums AS d ON d.ordernr_testcode_mtrlcode = u.ordernr_testcode_mtrlcode")
  }
  if (query %like% ' l_instelling[.]') {
    from <- paste0(from, "\n  ",
                   "LEFT JOIN\n  ",
                   "temporary_certemm_locaties AS l_instelling ON l_instelling.instelling = o.instelling")
  }
  if (query %like% ' l_ontvangst[.]') {
    from <- paste0(from, "\n  ",
                   "LEFT JOIN\n  ",
                   "temporary_certemm_locaties AS l_ontvangst ON l_ontvangst.instelling = o.recsitenb")
  }
  if (query %like% ' l_uitvoer[.]') {
    from <- paste0(from, "\n  ",
                   "LEFT JOIN\n  ",
                   "temporary_certemm_locaties AS l_uitvoer ON l_uitvoer.instelling = u.uitvafd")
  }
  if (query %like% ' t[.]') {
    from <- paste0(from, "\n  ",
                   "LEFT JOIN\n  ",
                   "temporary_certemm_testgroepen AS t ON t.testcode = u.testcode")
  }
  if (query %like% ' m[.]') {
    from <- paste0(from, "\n  ",
                   "LEFT JOIN\n  ",
                   "temporary_certemm_materiaalgroepen AS m ON m.mtrlcode = u.mtrlcode")
  }
  if (query %like% ' aanvr[.]' & query %like% ' a[.]') {
    warning("Encountered table references `a.*` and `aanvr.*`. This will lead to an extra and unnecessary LEFT JOIN.",
            call. = FALSE,
            immediate. = TRUE)
  }
  if (query %like% ' a[.]') {
    from <- paste0(from, "\n  ",
                   "LEFT JOIN\n  ",
                   "temporary_certemm_aanvragers_praktijken AS a ON a.aanvragercode = o.aanvrager")
  }
  if (query %like% ' aanvr[.]') {
    from <- paste0(from, "\n  ",
                   "LEFT JOIN\n  ",
                   "temporary_certemm_aanvragers_praktijken AS aanvr ON aanvr.aanvragercode = o.aanvrager")
  }
  if (query %like% ' beh[.]') {
    from <- paste0(from, "\n  ",
                   "LEFT JOIN\n  ",
                   "temporary_certemm_aanvragers_praktijken AS beh ON beh.aanvragercode = o.behandelaar")
  }
  if (query %like% ' p[.]') {
    from <- paste0(from, "\n  ",
                   "LEFT JOIN\n  ",
                   "certemm_pat AS p ON p.patidnb = o.patidnb")
  }
  if (query %like% ' cbs[.]') {
    from <- paste0(from, "\n  ",
                   "LEFT JOIN\n  ",
                   "temporary_certemm_cbs AS cbs ON cbs.postcode = SUBSTRING(p.postcode FROM 1 FOR 4)")
  }
  if (query %like% ' pat[.]') {
    from <- paste0(from, "\n  ",
                   "LEFT JOIN\n  ",
                   "temporary_certemm_patienten AS pat ON pat.patidnb = o.patidnb")
  }
  if (query %like% ' i[.]') {
    from <- paste0(from, "\n  ",
                   "LEFT JOIN\n  ",
                   "temporary_certemm_isolaten_rsi AS i ON i.ordernr_testcode_mtrlcode_stam = u.ordernr_testcode_mtrlcode_stam")
  }
  if (query %like% ' i_mic[.]') {
    from <- paste0(from, "\n  ",
                   "LEFT JOIN\n  ",
                   "temporary_certemm_isolaten_mic AS i_mic ON i_mic.ordernr_testcode_mtrlcode_stam = u.ordernr_testcode_mtrlcode_stam")
  }
  if (query %like% ' b[.]') {
    from <- paste0(from, "\n  ",
                   "LEFT JOIN\n  ",
                   "temporary_certemm_bacterienlijst AS b ON b.bacteriecode = u.bacteriecode")
  }
  if (query %like% ' g[.]') {
    from <- paste0(from, "\n  ",
                   "LEFT JOIN\n  ",
                   "temporary_certemm_grampreparaten AS g ON g.ordernr = u.ordernr")
  }
  from
}

where_R2SQL <- function(where = NULL, info = TRUE) {
  
  where <- where %>%
    paste0(' ', ., ' ') %>%
    gsub('"', "'", ., fixed = TRUE) %>%
    concat(' ') %>%
    gsub(' {2,255}', ' ', .)
  
  if (where %>% substr(1, 1) == "'") {
    where <- where %>% substr(2, nchar(.))
  }
  if (where %>% substr(nchar(.), nchar(.)) == "'") {
    where <- where %>% substr(1, nchar(.) - 1)
  }
  
  if (where %like% '^ [where(].*[)] $') {
    where <- paste0(' ', substr(where, 8, nchar(where) - 2), ' ')
  }
  
  if (where == ' NULL ') {
    return('')
  }
  
  where <- where %>%
    # spaties voor en na toevoegen
    paste0(' ', ., ' ') %>%
    # logische negatie verwerken:
    # !test LIKE 'a' -> test NOT LIKE 'a'
    # !(test LIKE 'a') -> (test NOT LIKE 'a')
    gsub(' [!]([(]*)([0-9a-zA-Z$_.]+) ', ' \\1\\2 NOT ', .) %>%
    # !is.na(test) of !is.null(test) -> test IS NOT NULL
    gsub(' (!is.na|!is.null)[(]([0-9a-zA-Z$_.]+)[)] ', ' \\2 IS NOT NULL ', .) %>%
    # is.na(test) of is.null(test) -> test IS NULL
    gsub(' (is.na|is.null)[(]([0-9a-zA-Z$_.]+)[)] ', ' \\2 IS NULL ', .) %>%
    # komma's naar '&' als dit niet opgevolgd wordt door een getal of aanhalingsteken
    # gsub(', ?([a-zA-Z])', ' & \\1', .) %>% # 2018-10-16 zie mailwisseling - gaat fout bij waarden die , bevatten
    # operators vertalen
    gsub(' != ', ' <> ', ., fixed = TRUE) %>%
    gsub(' ! = ', ' <> ', ., fixed = TRUE) %>%
    gsub(' & ', ' AND ', ., fixed = TRUE) %>%
    gsub(' && ', ' AND ', ., fixed = TRUE) %>%
    gsub(' | ', ' OR ', ., fixed = TRUE) %>%
    gsub(' || ', ' OR ', ., fixed = TRUE) %>%
    gsub(' == ', ' = ', ., fixed = TRUE) %>%
    gsub(' %in% ', ' IN ', ., fixed = TRUE) %>%
    gsub(' c(', ' (', ., fixed = TRUE) %>%
    gsub(' %like_case% ', ' REGEXP BINARY ', ., fixed = TRUE) %>%
    gsub(' %like% ', ' REGEXP ', ., fixed = TRUE) %>%
    gsub(' %unlike_case% ', ' NOT REGEXP BINARY ', ., fixed = TRUE) %>%
    gsub(' %unlike% ', ' NOT REGEXP ', ., fixed = TRUE) %>%
    gsub(' NOT = ', ' != ', ., fixed = TRUE) # anders fout: !a == b -> a NOT = b
  
  # ondersteuning voor R-evaluaties:
  # o.jaar IN 2016:2018 -> o.jaar IN (2016, 2017, 2018)
  eval_num <- gregexpr(' [(]*[0-9]+[:][0-9]+[)]*', where)
  if (unlist(eval_num)[1] != -1) {
    starts <- eval_num %>% unlist()
    lengths <- attributes(eval_num[[1]])$match.length
    where_backup <- where
    for (i in 1:length(starts)) {
      # getallen als 2015:2017 zoeken
      nums_old <- where_backup %>%
        substr(starts[i] + 1,
               starts[i] + lengths[i])
      
      # uitrekenen en omzetten naar MySQL lijst
      nums_new <- eval(parse(text = nums_old)) %>%
        concat(', ') %>%
        paste0(' (', . , ') ')
      
      # nieuwe where maken
      where <- sub(nums_old, nums_new, where, fixed = TRUE)
    }
  }
  
  # ondersteuning van evaluaties zoals mtcars$mpg en last_week()
  obj_refs <- gregexpr("([a-zA-Z0-9._]+[$]`?[a-zA-Z0-9._]+`?|[a-zA-Z0-9._]+[(][)])", where)
  if (unlist(obj_refs)[1] != -1) {
    starts <- obj_refs %>% unlist()
    lengths <- attributes(obj_refs[[1]])$match.length
    where_backup <- where
    for (i in 1:length(starts)) {
      # objecten als mtcars$mpg zoeken
      obj <- where_backup %>%
        substr(starts[i],
               starts[i] + lengths[i])
      
      # evalueren
      obj_evaluated <- eval(parse(text = obj))
      if ((length(obj_evaluated) != 1 &
           !is.numeric(obj_evaluated) &
           any(obj_evaluated %unlike% "[a-z][.][a-z]")) |
          inherits(obj_evaluated, c("Date", "POSIXt"))) {
        obj_evaluated <- paste0("'", obj_evaluated, "'")
      }
      if (length(obj_evaluated) > 1) {
        # naar MySQL-lijst omzetten
        obj_evaluated <- obj_evaluated %>%
          concat(', ') %>%
          paste0(' (', . , ')')
      }
      
      if (info == TRUE) {
        message(paste0('Replacing element `',
                       trimws(obj),
                       '` with value: ',
                       ifelse(nchar(obj_evaluated) > 25,
                              paste0(obj_evaluated %>% substr(1, 25), '...)'),
                              obj_evaluated)))
      }
      
      # spatie toevoegen
      obj_evaluated <- paste0(obj_evaluated, " ")
      
      # nieuwe where maken
      where <- sub(obj, obj_evaluated, where, fixed = TRUE)
    }
  }
  
  # ondersteuning voor variabelen uit Global Environment
  where_list <- where %>% strsplit(' ') %>% unlist()
  for (i in 1:length(where_list)) {
    if (where_list[i] %in% ls(envir = .GlobalEnv)) {
      newval <- eval(parse(text = where_list[i]))
      if (NCOL(newval) == 1) {
        if (!all(is.double2(newval))) {
          newval <- paste0("'", newval, "'")
        }
        if (length(newval) > 1) {
          # naar MySQL-lijst omzetten
          newval <- newval %>%
            concat(', ') %>%
            paste0('(', . , ')')
        }
        
        if (info == TRUE) {
          message(paste0('Replacing object `',
                         where_list[i],
                         '` with value: ',
                         ifelse(nchar(newval) > 25,
                                paste0(newval %>% substr(1, 25), '...)'),
                                newval)))
        }
        where_list[i] <- newval
      } else {
        stop(paste0('variable `',
                    where_list[i],
                    '` can only be a single value or vector.'))
      }
    }
  }
  where <- where_list %>% concat(' ') %>%
    # extra spaties verwijderen
    gsub(' {2,255}', ' ', .) %>%
    trimws('both')
  
  # proberen om expressie te evalueren als er een functie voorkomt (dus met haakje)
  where_list <- where %>% strsplit('=') %>% unlist()
  for (i in 1:length(where_list)) {
    if (where_list[i] %like% "[(]") {
      where_list[i] <- tryCatch(paste0("'",
                                       eval(parse(text = trimws(where_list[i]))),
                                       "'"),
                                error = function(e) where_list[i])
    }
  }
  where <- where_list %>% concat(' = ') %>%
    # extra spaties verwijderen
    gsub(' {2,255}', ' ', .) %>%
    trimws('both')
  
  if (where %like% "^['].+[']$" | where %like% '^["].+["]$') {
    where <- where %>% substr(2, nchar(.) - 1)
  }
  
  # extra witregels maken zodat SQL-query de juiste indents krijgt
  if (where %like% ' OR ') {
    where <- paste0('(', where %>% gsub(' OR ', '\n    OR ', ., fixed = TRUE), ')')
    if (where %like% ' AND ') {
      where <- paste0(where %>% gsub(' AND ', '\n    AND ', ., fixed = TRUE))
    }
  } else if (where %like% ' AND ') {
    where <- paste0(where %>% gsub(' AND ', '\n  AND ', ., fixed = TRUE))
  }
  where
}

diff_min_sec <- function(diff) {
  mins <- diff %>% as.integer() %>% as.double()
  secs <- ((diff %>% as.double() - mins) * 60) %>% round(0)
  if (mins != 0) {
    diff_text <- paste(mins, 'minutes and ')
  } else {
    diff_text <- ''
  }
  diff_text <- paste0(diff_text, secs, ' seconds.')
  diff_text
}

# Timestamp plaatsen in Console
certedb_timestamp <- function(subject, print, timestamp = TRUE, appendLF = TRUE) {
  if (print == TRUE) {
    if (timestamp == FALSE) {
      cat(subject)
    } else {
      cat(paste0('[',
                 format2(Sys.time()),
                 '] ',
                 subject))
    }
    if (appendLF == TRUE) {
      cat('\n')
    }
  }
}
