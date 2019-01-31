#  procedure to adjust treated UN-Comtrade data for Swiss trade in non-monetary gold [710812] prior to 2012
#
swiss <- function(df_in,df_swiss,t_in) {
  # colnames(df_IN) is given by c("hs",i","j","k","v_?","q_code_?","q_?","q_kg_?") (where ?={M,X,rX,rM}) 
  # colnames(df_swiss) is given by c("i","j","k","t","v_?","q_kg_?")
  # program replaces/appends Swiss data to UN-Comtrade dataset, excluding OECD balance adjustments for Germany (2005-2008), and UK (2007-2014)
  #      those might be introduced at country level study when time series are available (relative sharing is required per OECD)
  df_out <- df_in
  col_names_in <- colnames(df_in)
  col_names_swiss_in <- colnames(df_swiss)
  col_names_temp <- colnames(df_swiss) <- c("i","j","k","t","v","q_kg")
  col_names_out <- colnames(df_out)
    if ((t_in < 2012) & (t_in > 1987)) {
        hs <- NA
        # assume Swiss data are harmonized the same way as WITS data
        if (t_in %in% 2000:2001) {hs <- 1}
        if (t_in %in% 2002:2006) {hs <- 2}
        if (t_in %in% 2007:2011) {hs <- 3}
        temp <- subset(df_swiss,df_swiss$t==t_in)
        n_temp <- dim(temp)[1]
        df_hs <- data.frame(hs=rep(hs,n_temp))
        temp <- cbind(df_hs,temp)                   
        swiss_out <- data.frame( hs = rep(hs,n_temp),
                                  i = rep(757,n_temp),
                                  j=rep(0,n_temp),
                                  k=rep(710812,n_temp),
                                  v=rep(0,n_temp),
                                  q_code = rep(8,n_temp), # output quantities in kg
                                  q = rep(1,n_temp),      # q=1 
                                  q_kg = rep(0,n_temp))
        swiss_out$j    <- temp$j
        swiss_out$v    <- temp$v
        swiss_out$q_kg <- temp$q_kg
        colnames(swiss_out) <- col_names_out  # also equal to col_names_in
        df_out <- subset(df_out,!((df_out$i=757)&(df_out$k=710812)))  # eliminate any existing rows on Swiss reported trade in 710812
        df_out <- rbind(df_out,swiss_out)                
  }
  return(df_out)
} 