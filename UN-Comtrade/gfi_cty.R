gfi_cty <- function(logf){
  cols_bridge <- rep('NULL',20)
  names(cols_bridge) <- c('un_code','imf_code','imf_nm','unct_nm','d_gfi','d_dev','d_ssa','d_asia','d_deur','d_mena','d_whem','d_adv','un_nm_en','un_nm_en_full','un_nm_en_abbr','un_note','iso2_a','iso3_a','un_start','un_end')
  cols_bridge[c(1,5)] <- rep('integer',2)
  ecycle(bridge <- s3read_using(FUN = function(x)read.csv(x, colClasses=cols_bridge, header=TRUE), 
                                object = 'bridge.csv', bucket = sup_bucket),
         {if(!missing(logf))logf(paste('0000', '!', 'loading bridge.csv failed', sep = '\t')); stop()}, max_try)
  bridge <- unique(bridge)
  cty <- bridge$un_code[bridge$d_gfi==1]
  return(cty)
}