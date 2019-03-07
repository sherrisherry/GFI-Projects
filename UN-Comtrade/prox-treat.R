#  procedure to treat UN-Comtrade data for know country and commodity quirks (as noted)
#
treat <- function(df_in,t_in) {
  # Eliminate redundant country categories
  df_out <- df_in
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

    df_out <- subset(df_out,!(df_out$i %in% x_cty) & !(df_out$j %in% x_cty))
	
  # Special country treatments
  # (1) Beginning in 2000, exclude San Marino[674] & Vatican[381] (already included in Italy[381])
  if (t_in >= 2000) {
    x_cty <- c(674,381)
      df_out <- subset(df_out,!(df_out$i %in% x_cty) & !(df_out$j %in% x_cty))
  }
  # (2) Beginning in 2006, adjust Slovakia[703] reports to reflect Montenegro[499] independently from Serbia-Montegro[891]
  if (t_in >= 2006) {
    # unpack df_out
    colnames_out <- colnames(df_out)
    s_891  <-  subset(df_out, df_out$i==703 & df_out$j==891)  # isolate portion of data for which Slovakia reports trade flows with Serbia-Montenegro
    n_891  <- dim(s_891)[1]
    if (n_891 > 0) {
      s_688 <-   subset(df_out, df_out$i==703 & df_out$j==688)  #  subset of Slovakia reported trade with partner Serbia alone
      temp <- merge(x=s_891,y=s_688,by=c("hs","i","k"),all.x=TRUE)
      # colnames(df_IN) is given by c("hs","i","j","k","v_?","q_code_?","q_?","q_kg_?") (where ?={M,X,rX,rM}) so with the left merge
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
      df_out <- subset(df_out,!(df_out$i==703 & df_out$j==891))
      df_out <- rbind(df_out,s_891)
    } # end if (n_891>0)
  } # end if (t>=2006) ...Slovakia treatment
  # NEC commodity exclusions
  df_out <- df_out[(!df_out$k==999999),]; df_out <- df_out[(!df_out$k=="9999AA"),]
  # 
  return(df_out)
}
