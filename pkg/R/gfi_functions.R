bridge_cols <- function(nm, cl){
  cols <- rep('NULL',21)
  names(cols) <- c('un_code','imf_code','imf_nm','un_nm','d_gfi','d_dev','d_ssa','d_asia','d_deur','d_mena','d_whem','d_adv','un_nm_en_full','un_nm_en_abbr','un_note','iso2','iso3','un_start','un_end','wb_code','wb_nm')
  cols[match(nm, names(cols))] <- cl
  return(cols)
}

hkrx_cols <- function(nm, cl){
  cols <- rep('NULL',9)
  names(cols) <- c("t","origin_hk","consig_hk","k","vrx_hkd","origin_un","consig_un","usd_per_hkd","vrx_usd")
  if(missing(cl))
    cl <- c(rep("integer",3),"character","numeric",rep("integer",2),rep("numeric",2))[match(nm, names(cols))]
  cols[match(nm, names(cols))] <- cl
  return(cols)
}

in_hkrx <- function(yr, nm, cl, logf, max_try = 10){
  cols <- hkrx_cols(nm, cl)
  tmp <- tempfile()
  ecycle(save_object(object = paste('HK_trade', yr, 'rx.csv.bz2', sep = '_'), bucket = 'gfi-supplemental', file = tmp, overwrite = TRUE),
         {if(!missing(logf))logf(paste(year, '!', 'retrieving hkrx file failed', sep = '\t')); return(NULL)}, max_try) 
  ecycle(hk <- read.csv(pipe(paste("bzip2 -dc ", tmp, sep = '')), header=T, colClasses=cols, na.strings="", stringsAsFactors = F),
         {if(!missing(logf))logf(paste(year, '!', 'loading hkrx file failed', sep = '\t')); return(NULL)}, 
         max_try, cond = is.data.frame(hk) && nrow(hk)>10)
  return(hk)
}

gfi_cty <- function(logf, max_try = 10){
  cols_bridge <- bridge_cols(c('un_code','d_gfi'), rep('integer',2))
  ecycle(bridge <- s3read_using(FUN = function(x)read.csv(x, colClasses=cols_bridge, header=TRUE),
                                object = 'bridge.csv', bucket = 'gfi-supplemental'),
         {if(!missing(logf))logf(paste('0000', '!', 'loading bridge.csv failed', sep = '\t')); stop('loading bridge.csv failed')}, max_try)
  bridge <- unique(bridge)
  cty <- bridge$un_code[bridge$d_gfi==1]
  if(!missing(logf))logf(paste('0000', ':', 'decided cty', sep = '\t'))
  return(cty)
}

ec2env <- function(keycache, usr){
  Sys.setenv("AWS_ACCESS_KEY_ID" = keycache$Access_key_ID[keycache$service==usr],
             "AWS_SECRET_ACCESS_KEY" = keycache$Secret_access_key[keycache$service==usr])
  if(is.na(Sys.getenv()["AWS_DEFAULT_REGION"]))Sys.setenv("AWS_DEFAULT_REGION" = gsub('.{1}$', '', aws.ec2metadata::metadata$availability_zone()))
}
