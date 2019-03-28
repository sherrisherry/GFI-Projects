bridge_cols <- function(nm, cl){
  cols <- rep('NULL',21)
  names(cols) <- c('un_code','imf_code','imf_nm','un_nm','d_gfi','d_dev','d_ssa','d_asia','d_deur','d_mena','d_whem','d_adv','un_nm_en_full','un_nm_en_abbr','un_note','iso2','iso3','un_start','un_end','wb_code','wb_nm')
  cols[nm] <- cl
  return(cols)
}

hkrx_cols <- function(nm, cl){
  cols <- rep('NULL',9)
  names(cols) <- c("t","origin_hk","consig_hk","k","vrx_hkd","origin_un","consig_un","vrx_un")
  if(missing(cl))
    cl <- c(rep("integer",3),"character","numeric",rep("integer",2),"numeric")[match(nm, names(cols))]
    # c() matches names(cols)
  cols[nm] <- cl
  return(cols)
}

in_bridge <- function(nm, cl, logf, max_try = 10){
  cols <- bridge_cols(nm, cl)
  scripting::ecycle(bridge <- s3read_using(FUN = function(x)read.csv(x, colClasses=cols, header=T, fileEncoding = 'UTF8'),
                                           object = 'bridge.csv', bucket = 'gfi-supplemental'),
                    {if(!missing(logf))logf(paste('0000', '!', 'loading bridge.csv failed', sep = '\t')); return(NULL)}, max_try)
  return(bridge)
}

in_hkrx <- function(yr, nm, cl, logf, max_try = 10){
  cols <- hkrx_cols(nm, cl)
  tmp <- tempfile()
  scripting::ecycle(save_object(object = paste('HK', yr, 'rx.csv.bz2', sep = '_'), bucket = 'gfi-supplemental', file = tmp, overwrite = TRUE),
                    {if(!missing(logf))logf(paste(yr, '!', 'retrieving hkrx file failed', sep = '\t')); return(NULL)}, max_try) 
  scripting::ecycle(hk <- read.csv(pipe(paste("bzip2 -dc ", tmp, sep = '')), header=T, colClasses=cols, na.strings="", stringsAsFactors = F),
                    {if(!missing(logf))logf(paste(yr, '!', 'loading hkrx file failed', sep = '\t')); return(NULL)}, 
                    max_try, cond = is.data.frame(hk) && nrow(hk)>10)
  return(hk)
}

gfi_cty <- function(logf, max_try = 10){
  bridge <- in_bridge(c('un_code','d_gfi'), rep('integer',2), logf, max_try)
  if(is.null(bridge))return(NULL)
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
