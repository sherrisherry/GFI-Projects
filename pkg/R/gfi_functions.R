nv_cols <- function(nm, cl, obj, io = TRUE){
  switch(obj,
         bridge = {# "whem","mena","ssa","deur","asia" in imf_reg
           cols <- c(rep('integer',7), rep('character',9))
           names(cols) <- c('un_code','un_start','un_end','wb_code','imf_code','d_gfi','d_dev',
                            'imf_reg','imf_nm','un_nm','un_nm_en_full','un_nm_en_abbr','un_note','iso2','iso3','wb_nm')
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
  batchscr::ecycle(bridge <- aws.s3::s3read_using(FUN = function(x)read.csv(x, colClasses=cols, header=T, fileEncoding = 'UTF8'),
                                                   object = 'bridge.csv', bucket = 'gfi-supplement'),
                    {if(!missing(logf))logf(paste('0000', '!', 'loading bridge.csv failed', sep = '\t')); return(NULL)}, max_try)
  return(bridge)
}

in_geo <- function(nm, cl, logf, max_try = 10, io = TRUE){
  cols <- nv_cols(nm, cl, 'geo', io)
  batchscr::ecycle(geo <- aws.s3::s3read_using(FUN = function(x)read.csv(x, colClasses=cols, header=T, na.strings=''),
                                                object = 'CEPII_GeoDist.csv', bucket = 'gfi-supplement'),
                    {if(!missing(logf))logf(paste('0000', '!', 'loading CEPII_GeoDist.csv failed', sep = '\t')); return(NULL)}, max_try)
  return(geo)
}

in_eia <- function(nm, cl, logf, max_try = 10, io = TRUE){
  cols <- nv_cols(nm, cl, 'eia', io)
  tmp <- tempfile()
  batchscr::ecycle(aws.s3::save_object(object = 'EIA.csv.bz2', bucket = 'gfi-supplement', file = tmp, overwrite = TRUE),
                    {if(!missing(logf))logf(paste('0000', '!', 'retrieving EIA file failed', sep = '\t')); return(NULL)}, max_try)
  batchscr::ecycle(eia <- read.csv(bzfile(tmp), header=T, colClasses=cols, na.strings="", stringsAsFactors = F),
                    {if(!missing(logf))logf(paste('0000', '!', 'loading file failed', sep = '\t')); return(NULL)},
                    max_try, cond = is.data.frame(eia) && nrow(eia)>10)
  return(eia)
}

in_hkrx <- function(yr, nm, cl, logf, max_try = 10, io = TRUE){
  cols <- nv_cols(nm, cl, 'hkrx', io)
  tmp <- tempfile()
  batchscr::ecycle(aws.s3::save_object(object = paste('HK', yr, 'rx.csv.bz2', sep = '_'), bucket = 'gfi-supplement', file = tmp, overwrite = TRUE),
                    {if(!missing(logf))logf(paste(yr, '!', 'retrieving hkrx file failed', sep = '\t')); return(NULL)}, max_try)
  batchscr::ecycle(hk <- read.csv(bzfile(tmp), header=T, colClasses=cols, na.strings="", stringsAsFactors = F),
                    {if(!missing(logf))logf(paste(yr, '!', 'loading hkrx file failed', sep = '\t')); return(NULL)},
                    max_try, cond = is.data.frame(hk) && nrow(hk)>10)
  return(hk)
}

# opt can be missing, dev, adv, or a region abbrivation
gfi_cty <- function(opt, logf, max_try = 10){
  cols <- if(!missing(opt))switch(opt, dev = 'd_dev', adv = c('d_gfi', 'd_dev'), c('d_gfi', 'imf_reg'))else 'd_gfi'
  bridge <- in_bridge(c(cols, 'un_code'), logf = logf, max_try = max_try)
  if(is.null(bridge))return(NULL)
  bridge <- unique(bridge)
  cty <- if(length(cols)==1)subset(bridge, bridge[, cols]==1, 'un_code', drop = T)
          else switch(class(bridge[, setdiff(cols, 'd_gfi')]),
                      integer = subset(bridge, abs(bridge[, cols[1]]-bridge[, cols[2]])==1, 'un_code', drop = T),
                      character = bridge$un_code[bridge$d_gfi==1 & bridge$imf_reg==opt])
  if(!missing(logf))logf(paste('0000', ':', 'decided cty', sep = '\t'))
  return(cty)
}

ec2env <- function(keycache, usr){
  Sys.setenv("AWS_ACCESS_KEY_ID" = keycache$Access_key_ID[keycache$service==usr],
             "AWS_SECRET_ACCESS_KEY" = keycache$Secret_access_key[keycache$service==usr])
  if(is.na(Sys.getenv()["AWS_DEFAULT_REGION"]))Sys.setenv("AWS_DEFAULT_REGION" = gsub('.{1}$', '', aws.ec2metadata::metadata$availability_zone()))
}
