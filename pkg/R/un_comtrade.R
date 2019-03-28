#  procedure to adjust treated UN-Comtrade data for Swiss trade in non-monetary gold [710812] prior to 2012
unct_swiss <- function(unct,df_swiss,t_in){
  # colnames(unct) is given by c("hs","i","j","k","v_?","q_code_?","q_?","q_kg_?") (where ?={M,X,rX,rM})
  # colnames(df_swiss) is given by c("mx","j","k","t","v","q_kg")
  # program replaces/appends Swiss data to UN-Comtrade dataset, excluding OECD balance adjustments for Germany (2005-2008), and UK (2007-2014)
  #      those might be introduced at country level study when time series are available (relative sharing is required per OECD)
  cols_out <- colnames(unct); names(cols_out) <- sub('_[^_]+$', '', cols_out)
  if ((t_in < 2012) & (t_in > 1987)) {
    hs <- NA
    # assume Swiss data are harmonized the same way as WITS data
    if (t_in %in% 2000:2001) {hs <- 1}
    if (t_in %in% 2002:2006) {hs <- 2}
    if (t_in %in% 2007:2011) {hs <- 3}
    df_swiss <- subset(df_swiss,df_swiss$t==t_in)
    n_df_swiss <- nrow(df_swiss)
    swiss_out <- data.frame( hs = rep(hs,n_df_swiss),
                             i = rep(757,n_df_swiss),
                             j = df_swiss$j,
                             k=rep("710812",n_df_swiss),
                             v = df_swiss$v,
                             q_code = rep(8,n_df_swiss), # output quantities in kg
                             q = df_swiss$q_kg,      # q=q_kg
                             q_kg = df_swiss$q_kg )
    colnames(swiss_out) <- cols_out[colnames(swiss_out)]
    unct <- subset(unct,!((unct$i=757)&(unct$k="710812")))  # eliminate any existing rows on Swiss reported trade in 710812
    unct <- rbind(unct,swiss_out)
    if(data.table::is.data.table(unct))setkeyv(unct, c('i', 'j', 'k'))
  }
  return(unct)
}

unct_hk <- function(unct, hk, yr, mx, logf, max_try = 10, out_bucket){
  ij <- switch(mx, m = c('j', 'i'), x = c('i', 'j'))
  cols_hk <- c(ij,"k","v_rx_hk")
  names(cols_hk) <- c("origin_un","consig_un","k","vrx_un")
  cols_out <- c("hs_rpt","hs_ptn",ij,"k","v_X","v_M","v_rM","v_rX","q_X","q_M","q_kg_X","q_kg_M","q_code_X","q_code_M")
  if(!setequal(names(cols_hk), colnames(hk)) || !all(cols_out %in% colnames(unct))){
    if(!missing(logf))logf(paste(yr, '!', 'HK adj col mismatch', sep = '\t'))
    return(NULL)
  }
  colnames(hk) <- cols_hk[colnames(hk)]
  if(data.table::is.data.table(unct))hk <- data.table::data.table(hk, key = c(ij, 'k'))
  unct <- merge(x=unct,y=hk,by=c(ij,"k"),all.x=TRUE)
  hk_344 <- hk[,c(ij[2],"k","v_rx_hk")]
  colnames(hk_344) <- c(ij[2],"k","adj_344")
  hk_344 <- aggregate(hk_344$adj_344,list(hk_344[, ij[2]],hk_344$k),sum) ; colnames(hk_344) <- c(ij[2],"k","adj_344")
  if(data.table::is.data.table(unct))hk_344 <- data.table::data.table(hk_344, key = c(ij[2],'k'))
  unct <- merge(x=unct,y=hk_344,by=c(ij[2],"k"),all.x=TRUE)
  unct[is.na(unct$v_rx_hk),"v_rx_hk"] <- 0
  unct[is.na(unct$adj_344),"adj_344"] <- 0
  tmp <- unct
  unct[unct[, ij[1]]!=344,"v_M"] <- unct[unct[, ij[1]]!=344,"v_M"] - unct[unct[, ij[1]]!=344,"v_rx_hk"]
  unct[unct[, ij[1]]==344,"v_M"] <- unct[unct[, ij[1]]==344,"v_M"] + unct[unct[, ij[1]]==344,"adj_344"]
  unct <- subset(unct, select = cols_out)
  junk <- subset(unct,unct$v_M<0)
  unct[unct$v_M<0,"v_M"] <- tmp[unct$v_M<0,"v_M"]  ; # undo the adjustment for negative values
  unct[unct[, ij[2]]==752,"v_M"] <- tmp[unct[, ij[2]]==752,"v_M"]  ; # undo the adjustment for Sweden  (OECD[2016], p. 19)
  unct[unct[, ij[2]]==348,"v_M"] <- tmp[unct[, ij[2]]==348,"v_M"]  ; # undo the adjustment for Hungary (OECD[2016], p. 19)
  if(!missing(logf))logf(paste(yr, ':', paste(mx, 'HK adjusted'), sep = '\t'))
  scripting::ecycle(aws.s3::s3write_using(junk, FUN = function(x, y)write.csv(x, file=bzfile(y), row.names = FALSE),
                                          bucket = out_bucket,
                                          object = paste('Comtrade', yr, mx, 'junked-HK-adj.csv.bz2', sep = '-')),
                    if(!missing(logf))logf(paste(yr, '.', paste('junked', mx, 'HK-adj not uploaded'), sep = '\t')), 
                    ceiling(max_try/3))
  return(unct)
}
#  procedure to treat UN-Comtrade data for know country and commodity quirks (as noted)
unct_treat <- function(unct,t_in) {
  # Eliminate redundant country categories
  x_cty <- c(0,     # World
             250,   # France excluding Monaco & overseas (UN data for France includes Monaco and overseas areas)
             1251,  # France including Monaco excluding overseas (UN data for France includes Monaco and overseas areas)
             492,   # part of Europe EU, nes
             312,   # Gaudeloupe (included in UN data for France)
             474,   # Martinique (included in UN data for France)
             638,   # Reunion (included in UN data for France)
             254,   # French Guiana (included in UN data for France)
             756,   # Switzerland excluding Liechtenstein (UN data for Switzerland includes Liechtenstein)
             438,   # Liechtenstein (UN data for Switzerland includes Liechtenstein)
             840,   # USA excluding Puerto Rico and Virgin Islands (UN data for USA should include PR & USVI)
             841,   # USA including Puerto Rico (UN data for USA should include PR & USVI)
             850,   # US Virgin Islands (UN data for USA should include PR & USVI)
             630,   # Puerto Rico (UN data for USA should include PR & USVI)
             849,   # US miscellaneous Pacific Islands (not in USA but likely redundant with US Minor Outlying Parnters which is kept)
             744,   # Svalbard & Jan Mayen Islands (UN data for Norway includes Svalbard & Jan Mayen Islands)
             578,   # Norway excluding Bouvet, Svalbard & Jan Mayen Islands (UN data for Norway includes Svalbard & Jan Mayen Islands)
             58,    # Belgium-Luxembourg aggregate (UN data include Belgium and Luxembourg as separate countries)
             1058,  # Belgium-Luxembourg aggregate (UN data include Belgium and Luxembourg as separate countries)
             532,   # Netherlands Antilles and Aruba (UN data includes the two as separate countries)
             658,   # Saint Kitts, Nevise & Anguilla (UN data have St Kitts/Nevis and Anguilla reported separately by all)
             1275,  # British Virgin Islands & Montserrat (UN data have BVI and Montserrate reported separately by all)
             536,   # non-geographic: Neutral zones
             837,   # non-geographic: Bunkers
             838,   # non-geographic: Free zones
             839,   # non-geographic: Special categories
             577,   # Africa, nes
             636,   # Rest of America, nes
             568,   #         Europe, nes
             637,   #         North & Central America, nes
             527,   #         Oceania, nes
             899,   #         Areas, nes
             473,   #         South America, nes
             879    #         Western Asia, nes
  )

  unct <- subset(unct,!(unct$i %in% x_cty) & !(unct$j %in% x_cty))

  # Special country treatments
  # (1) Beginning in 2000, exclude San Marino[674] & Vatican[381] (already included in Italy[381])
  if (t_in >= 2000) {
    x_cty <- c(674,381)
    unct <- subset(unct,!(unct$i %in% x_cty) & !(unct$j %in% x_cty))
  }
  # (2) Beginning in 2006, adjust Slovakia[703] reports to reflect Montenegro[499] independently from Serbia-Montegro[891]
  if (t_in >= 2006) {
    # unpack unct
    colnames_out <- colnames(unct)
    s_891  <-  subset(unct, unct$i==703 & unct$j==891)  # isolate portion of data for which Slovakia reports trade flows with Serbia-Montenegro
    n_891  <- dim(s_891)[1]
    if (n_891 > 0) {
      s_688 <-   subset(unct, unct$i==703 & unct$j==688)  #  subset of Slovakia reported trade with partner Serbia alone
      temp <- merge(x=s_891,y=s_688,by=c("hs","i","k"),all.x=TRUE)
      # colnames(unct) is given by c("hs","i","j","k","v_?","q_code_?","q_?","q_kg_?") (where ?={M,X,rX,rM}) so with the left merge
      # colnames(temp) will have column structure:
      #    1 "hs"
      #    2 "i"
      #    3 "j"
      #    4 "k"
      #    5 "v_?.x"
      #    6 "q_code_?.x"
      #    7 "q_?.x"
      #    8 "q_kg_?.x"
      #    9 "v_?.y"
      #   10 "q_code_?.y"
      #   11 "q_?.y"
      #   12 "q_kg_?.y"
      # so we'll be using merged trade flow values (cols 5 & 9) and quantities (if col6=col10, cols 7 & 11 + cols 8 & 12)
      s_891[,"j"] <- 499                   # replace partner code for Serbia-Montenegro [899] with Montenegro [499]
      s_891[,5] <- temp[,5] - temp[,9] # $ trade flow for Serbia = Serbia-Montenegro minus Serbia
      if (temp[,6]==temp[,10]) {
        s_891[,7] <- temp[,7]-temp[,11]
        s_891[,8] <- temp[,8]-temp[,12]
      } else {
        s_891[,6] <- 1                   # if mismatch on merged quantities, reset to No quantity
        s_891[,7] <- NA                  # missing
        s_891[,8] <- NA                  # missing
      }
      unct <- subset(unct,!(unct$i==703 & unct$j==891))
      unct <- rbind(unct,s_891)
    } # end if (n_891>0)
  } # end if (t>=2006) ...Slovakia treatment
  # NEC commodity exclusions
  unct <- unct[unct$k!='999999' & unct$k!="9999AA",]
  if(data.table::is.data.table(unct))setkeyv(unct, c('i', 'j', 'k'))
  return(unct)
}
