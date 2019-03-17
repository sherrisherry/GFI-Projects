bridge_cols <- function(nm, cl){
  cols <- rep('NULL',21)
  names(cols) <- c('un_code','imf_code','imf_nm','un_nm','d_gfi','d_dev','d_ssa','d_asia','d_deur','d_mena','d_whem','d_adv','un_nm_en_full','un_nm_en_abbr','un_note','iso2','iso3','un_start','un_end','wb_code','wb_nm')
  cols[match(nm, names(cols))] <- cl
  return(cols)
}

gfi_cty <- function(logf){
  cols_bridge <- bridge_cols(c('un_code','d_gfi'), rep('integer',2))
  ecycle(bridge <- s3read_using(FUN = function(x)read.csv(x, colClasses=cols_bridge, header=TRUE),
                                object = 'bridge.csv', bucket = sup_bucket),
         {if(!missing(logf))logf(paste('0000', '!', 'loading bridge.csv failed', sep = '\t')); stop()}, max_try)
  bridge <- unique(bridge)
  cty <- bridge$un_code[bridge$d_gfi==1]
  logf(paste('0000', ':', 'decided cty', sep = '\t'))
  return(cty)
}
