nv_cols <- function(nm, cl, obj, io = TRUE){
  switch(obj,
         bridge = {
           cols <- rep('NULL',21)
           names(cols) <- c('un_code','imf_code','imf_nm','un_nm','d_gfi','d_dev','d_ssa','d_asia','d_deur','d_mena','d_whem','d_adv','un_nm_en_full','un_nm_en_abbr','un_note','iso2','iso3','un_start','un_end','wb_code','wb_nm')
         },
         geo = {
           cols <- c(rep("integer",4),"numeric",rep("integer",4))
           names(cols) <- c("j","i","area_j","area_i","distw","d_landlocked_j","d_landlocked_i","d_contig","d_conti")
         },
         eia = {
           cols <- rep("integer",4)
           names(cols) <- c("t","i","j","d_rta")
         },
         hkrx = {
           cols <- c(rep("integer",3),"character","numeric",rep("integer",2),"numeric")
           names(cols) <- c("t","origin_hk","consig_hk","k","vrx_hkd","origin_un","consig_un","vrx_un")
         })
  if(missing(nm))nm <- names(cols)
  if(!missing(cl))cols[nm] <- cl
  if(io)cols[setdiff(names(cols), nm)] <- 'NULL' else cols[nm] <- 'NULL'
  return(cols)
}

in_bridge <- function(nm, cl, logf, max_try = 10, io = TRUE){
  cols <- nv_cols(nm, cl, 'bridge', io)
  scripting::ecycle(bridge <- s3read_using(FUN = function(x)read.csv(x, colClasses=cols, header=T, fileEncoding = 'UTF8'),
                                           object = 'bridge.csv', bucket = 'gfi-supplemental'),
                    {if(!missing(logf))logf(paste('0000', '!', 'loading bridge.csv failed', sep = '\t')); return(NULL)}, max_try)
  return(bridge)
}

in_geo <- function(nm, cl, logf, max_try = 10, io = TRUE){
  cols <- nv_cols(nm, cl, 'geo', io)
  scripting::ecycle(geo <- s3read_using(FUN = function(x)read.csv(x, colClasses=cols, header=T, na.strings=''),
                                        object = 'CEPII_GeoDist.csv', bucket = 'gfi-supplemental'),
                    {if(!missing(logf))logf(paste('0000', '!', 'loading CEPII_GeoDist.csv failed', sep = '\t')); return(NULL)}, max_try)
  return(geo)
}

in_eia <- function(nm, cl, logf, max_try = 10, io = TRUE){
  cols <- nv_cols(nm, cl, 'eia', io)
  tmp <- tempfile()
  scripting::ecycle(save_object(object = 'EIA.csv.bz2', bucket = 'gfi-supplemental', file = tmp, overwrite = TRUE),
                    {if(!missing(logf))logf(paste('0000', '!', 'retrieving EIA file failed', sep = '\t')); return(NULL)}, max_try)
  scripting::ecycle(eia <- read.csv(bzfile(tmp), header=T, colClasses=cols, na.strings="", stringsAsFactors = F),
                    {if(!missing(logf))logf(paste('0000', '!', 'loading file failed', sep = '\t')); return(NULL)},
                    max_try, cond = is.data.frame(eia) && nrow(eia)>10)
  return(eia)
}

in_hkrx <- function(yr, nm, cl, logf, max_try = 10, io = TRUE){
  cols <- nv_cols(nm, cl, 'hkrx', io)
  tmp <- tempfile()
  scripting::ecycle(save_object(object = paste('HK', yr, 'rx.csv.bz2', sep = '_'), bucket = 'gfi-supplemental', file = tmp, overwrite = TRUE),
                    {if(!missing(logf))logf(paste(yr, '!', 'retrieving hkrx file failed', sep = '\t')); return(NULL)}, max_try)
  scripting::ecycle(hk <- read.csv(bzfile(tmp), header=T, colClasses=cols, na.strings="", stringsAsFactors = F),
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