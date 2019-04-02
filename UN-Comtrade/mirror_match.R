# 1. decompress and subset the raw data to only keep aggregation level 6 and remove country names and commodity names;
# 3. compress the results and send them to AWS S3;
# 4. record the information of downloaded data;
# 5. send a progress report to Google Drive;
# 6. shutdown system after completion.

rm(list=ls()) # clean up environment
pkgs <- c('aws.s3', 'sparklyr', 'scripting')
for(i in pkgs)library(i, character.only = T)
remotes::install_github("sherrisherry/GFI-Cloud", subdir="pkg"); library(pkg)

#=====================================modify the following parameters for each new run==============================================#

usr <- 'aws00' # the user account for using AWS service
dates <- 2005:2001 # the years we want to download
out_bucket <- 'gfi-mirror-analysis' # save the results to a S3 bucket called 'gfi-mirror-analysis'
oplog <- 'mirror_match.log' # progress report file
opcounter <- 'mirror_match.csv'
dinfo <- 'bulk_download.log' # file of the information of the downloaded data
keycache <- read.csv('~/vars/accesscodes.csv', header = TRUE, stringsAsFactors = FALSE) # the database of our credentials

#===================================================================================================================================#

ec2env(keycache,usr)
options(stringsAsFactors= FALSE)
log_head <- 'Time\tZone\tYear\tMark\tStatus\n'
if(!file.exists('/efs/logs'))system('sudo mkdir -m777 /efs/logs')
oplog <- paste('/efs/logs/', oplog, sep = '')
config <- spark_config()
config$sparklyr.apply.env.AWS_ACCESS_KEY_ID <- Sys.getenv('AWS_ACCESS_KEY_ID')
config$sparklyr.apply.env.AWS_SECRET_ACCESS_KEY <- Sys.getenv('AWS_SECRET_ACCESS_KEY')
config$sparklyr.apply.env.AWS_DEFAULT_REGION <- Sys.getenv('AWS_DEFAULT_REGION')
config$sparklyr.apply.env.log_head <- log_head
config$sparklyr.apply.env.out_bucket <- out_bucket
config$sparklyr.apply.env.oplog <- oplog

sc <- spark_connect(master="local", config = config)
logg <- function(x)mklog(x, path = oplog)
cat(log_head, file = oplog, append = FALSE)
opcounter <- file.path('data', opcounter)
dinfo <- file.path('logs', dinfo); dinfo <- read.delim(dinfo)
dinfo <- dinfo[dinfo$Mark == '#' & dinfo$Year %in% dates, c('Year', 'Status')]
dinfo <- unique(dinfo); n_d <- nrow(dinfo)
stopifnot(n_d > 0 & !any(duplicated(dinfo$Year)))

dist_codes <- function(in_df){
  pkgs <- c('aws.s3', 'sparklyr', 'stats', 'scripting', 'data.table')
  for(i in pkgs)library(i, character.only = T)
  remotes::install_github("sherrisherry/GFI-Cloud", subdir="pkg"); library(pkg)
  #===================================================================================================================================#
  max_try <- 10 # the maximum number of attempts for a failed process
  in_bucket <- 'gfi-comtrade' # read in raw data from this bucket
  sup_bucket <- 'gfi-supplemental' # supplemental files
  tag <- "Comtrade"
  cols_UN <- rep('NULL', 15)
  names(cols_UN) <- c("classification","year","period","perioddesc","aggregatelevel","isleafcode","tradeflowcode",
                      "reportercode","partnercode", "commoditycode","qtyunitcode","qty","netweightkg","tradevalues","flag")
  incol <- c("tradeflowcode","classification","reportercode","partnercode","commoditycode", "tradevalues", "qtyunitcode", "qty", "netweightkg")
  names(incol) <- c("tf","hs","i","j","k","v","q_code","q","q_kg")
  cols_UN[incol] <- c("integer","character","integer","integer","character","numeric","integer","numeric","numeric")
  cols_swiss <- c("character","integer","character","integer",rep("numeric",2))
  names(cols_swiss) <- c('mx','j','k','t','v','q_kg')
  cols_hk <- c(rep('integer', 2), 'character', 'numeric')
  names(cols_hk) <- c("origin_un","consig_un","k","vrx_un")
  # use "character" for k or codes like '9999AA' messes up
  tfn <- c('M','rM','X','rX')
  names(tfn) <- c('1','4','2','3')
  out_names <- c('M_matched', 'X_matched', 'M_orphaned', 'M_lost')
  names(out_names) <- c('M.FALSE', 'X.FALSE', 'M.TRUE', 'X.TRUE') # M_lost == X_orphan
  #===================================================================================================================================#
  out_bucket <- Sys.getenv('out_bucket')
  oplog <- Sys.getenv('oplog')
  logg <- function(x)mklog(x, path = oplog)
  options(stringsAsFactors= FALSE)
  cat(Sys.getenv('log_head'), file = oplog, append = FALSE)
  # Prepare for SWISS module
  # Data M_swiss and X_swiss compiled by Joe Spanjers from Swiss source data
  ecycle(swiss <- s3read_using(FUN = function(x)fread(x, header=T, na.strings="", colClasses = cols_swiss), 
                               object = "Swiss_mx_71-72.csv", bucket = sup_bucket),
         {tmp <- paste('0000', '!', 'loading Swiss failed', sep = '\t'); logg(tmp); stop(tmp)},
         max_try)
  # # keep only records for commodity 710812 which comprises 98% of Swiss trade in non-monetary gold
  #     (NB, monetary gold trade flows should not be included in UN-Comtrade dataset for any country)
  swiss <- subset(swiss,swiss$k=='710812'); swiss <- split(swiss, swiss$mx)
  opcounter <- list() # setup counter
  # Loop by date
  for (year in in_df$Year){
    counter <- c(year = year)
    # Start RAW module
    ecycle(save_object(object = paste(year, '.csv.bz2', sep = ''), bucket = in_bucket, file = 'tmp/tmp.csv.bz2', overwrite = TRUE),
           {logg(paste(year, '!', 'retrieving file failed', sep = '\t')); next}, max_try)
    ecycle(rdata <- fread(cmd = "bzip2 -dkc ./tmp/tmp.csv.bz2", header=T, colClasses = cols_UN),
           {logg(paste(year, '!', 'loading file failed', sep = '\t')); next}, max_try,
           cond = is.data.table(rdata))
    if(!setequal(colnames(rdata), incol)){
      logg(paste(year, '!', 'colnames mismatching', sep = '\t'))
      next
    }
    logg(paste(year, ':', 'opened', sep = '\t'))
    if(nrow(rdata) != in_df$Status[in_df$Year==year]){logg(paste(year, '#', nrow(rdata), sep = '\t'))}
    # avoid reordering data table because of speed
    tmp <- colnames(rdata)
    colnames(rdata)[match(incol, tmp)] <- names(incol)
    rdata <- na.omit(rdata, cols=c("hs","i","j","k","v"))
    setkeyv(rdata, c('i', 'j', 'k'))
    logg(paste(year, ':', 'preprocessed', sep = '\t'))
    rdata<- split(rdata, rdata$tf)
    names(rdata) <- tfn[match(names(rdata),names(tfn))]
    rdata <- lapply(rdata, function(x){cols <- colnames(x); nm <- c("v","q_code","q","q_kg"); lsn <- tfn[as.character(x$tf[1])]
    colnames(x)[match(nm, cols)] <- paste(nm, lsn, sep = '_'); x$tf <- NULL
    return(x)})
    tmp <- sapply(rdata, nrow); names(tmp) <- paste('raw', names(tmp), sep = '_')
    counter <- append(counter, tmp)
    logg(paste(year, '#', paste(paste(names(tmp), tmp, sep = ':'), collapse = ','), sep = '\t'))
    #   End RAW module
    
    #   Implement TREAT module 
    rdata <- lapply(rdata, function(x)unct_treat(x, year))
    tmp <- sapply(rdata, nrow); names(tmp) <- paste('treat', names(tmp), sep = '_')
    counter <- append(counter, tmp)
    logg(paste(year, '#', paste(paste(names(tmp), tmp, sep = ':'), collapse = ','), sep = '\t'))
    
    #   End TREAT module
    
    #   Implement SWISS module
    rdata$M  <- unct_swiss(rdata$M,swiss$m,year); rdata$X  <- unct_swiss(rdata$X,swiss$x,year)
    tmp <- sapply(rdata, nrow); names(tmp) <- paste('swiss', names(tmp), sep = '_')
    counter <- append(counter, tmp)
    logg(paste(year, '#', paste(paste(names(tmp), tmp, sep = ':'), collapse = ','), sep = '\t'))
    # End SWISS module
    
    #   Start PAIR module
    # Pair M
    tmp <- colnames(rdata$M); colnames(rdata$M)[match('hs', tmp)] <- c('hs_rpt')
    tmp <- colnames(rdata$X); colnames(rdata$X)[match(c('hs', 'i', 'j'), tmp)] <- c('hs_ptn', 'j', 'i')
    tmp <- colnames(rdata$rX); colnames(rdata$rX)[match(c('i', 'j'), tmp)] <- c('j', 'i')
    mirror <- list()
    mirror$M <- merge(x=rdata$M,y=rdata$X,by = c("i","j","k"), all.x=TRUE)
    mirror$M <- merge(x=mirror$M,y=rdata$rX[, c("i","j","k","v_rX")],by = c("i","j","k"), all.x=TRUE)
    mirror$M <- merge(x=mirror$M,y=rdata$rM[, c("i","j","k","v_rM")],by = c("i","j","k"), all.x=TRUE)
    
    logg(paste(year, ':', 'M-paired', sep = '\t'))
    
    # Pair X
    tmp <- colnames(rdata$X); colnames(rdata$X)[match(c('hs_ptn', 'i', 'j'), tmp)] <- c('hs_rpt', 'j', 'i')
    tmp <- colnames(rdata$M); colnames(rdata$M)[match(c('hs_rpt', 'i', 'j'), tmp)] <- c('hs_ptn', 'j', 'i')
    tmp <- colnames(rdata$rM); colnames(rdata$rM)[match(c('i', 'j'), tmp)] <- c('j', 'i')
    tmp <- colnames(rdata$rX); colnames(rdata$rX)[match(c('i', 'j'), tmp)] <- c('j', 'i')
    mirror$X <- merge(x=rdata$X,y=rdata$M,by = c("i","j","k"), all.x=TRUE)
    mirror$X <- merge(x=mirror$X,y=rdata$rX[, c("i","j","k","v_rX")],by = c("i","j","k"), all.x=TRUE)
    mirror$X <- merge(x=mirror$X,y=rdata$rM[, c("i","j","k","v_rM")],by = c("i","j","k"), all.x=TRUE)
    
    rm(rdata)
    logg(paste(year, ':', 'X-paired', sep = '\t'))
    # end PAIR module
    
    # Start Hong Kong module
    # operate on X-paired and M-paired and then redo the matching etc
    # use normalized hkrx data
    hk <- in_hkrx(year, names(cols_hk), cols_hk, logf = logg, max_try = max_try)
    if(is.null(hk))next
    mirror$M <- unct_hk(mirror$M, hk, year, 'M', logg, max_try, out_bucket); if(is.null(mirror$M))next
    mirror$X <- unct_hk(mirror$X, hk, year, 'X', logg, max_try, out_bucket); if(is.null(mirror$X))next
    # end Hong Kong module
    
    tmp <- sapply(mirror, nrow); names(tmp) <- paste(names(tmp), 'pair', sep = '_')
    counter <- append(counter, tmp)
    logg(paste(year, '#', paste(paste(names(tmp), tmp, sep = ':'), collapse = ','), sep = '\t'))
    
    # Start MATCH module
    # Matching M
    mirror$M <- split(mirror$M, is.na(mirror$M$v_X))
    mirror$X <- split(mirror$X, is.na(mirror$X$v_M))
    mirror <- unlist(mirror, recursive = F)
    # mirror$M.FALSE should equal mirror$X.FALSE
    stopifnot(setequal(names(mirror),names(out_names)))
    names(mirror) <- out_names[names(mirror)]
    # renamed mirror to match out_names
    tmp <- sapply(mirror, nrow)
    counter <- append(counter, tmp)
    logg(paste(year, '#', paste(paste(names(tmp), tmp, sep = ':'), collapse = ','), sep = '\t'))
    # end MATCH module
    
    # verify matched M==X
    if(counter['M_matched']==counter['X_matched']){
      tmp <- colnames(mirror$X_matched)
      colnames(mirror$X_matched)[match(c("hs_rpt","hs_ptn","i","j"), tmp)] <- c("hs_ptn","hs_rpt","j","i")
      tmp <- duplicated(rbind(mirror$M_matched, mirror$X_matched))
      if(sum(tmp)!=counter['M_matched'])logg(paste(year, '!', 'MX nrow not identical', sep = '\t'))
      else{logg(paste(year, ':', 'identical', sep = '\t')); mirror$X_matched <-NULL}
    }else logg(paste(year, '!', 'MX not identical', sep = '\t'))
    
    # uploading results
    tmp <- paste(tag, year, names(mirror), sep = '-'); tmp <- paste('tmp/', tmp, '.csv.bz2', sep = '')
    names(tmp) <- names(mirror)
    for(i in names(mirror)){
      ecycle(write.csv(mirror[[i]], file = bzfile(tmp[i]), row.names=F, na=""), 
             ecycle(s3write_using(mirror[[i]], FUN = function(x, y)write.csv(x, file=bzfile(y), row.names = F, na=''),
                                  bucket = out_bucket, object = basename(tmp[i])),
                    logg(paste(year, '!', paste('uploading', basename(tmp[i]), 'failed', sep = ' '), sep = '\t')), max_try), 
             max_try,
             ecycle(put_object(tmp[i], basename(tmp[i]), bucket = out_bucket), 
                    logg(paste(year, '!', paste('uploading', basename(tmp[i]), 'failed', sep = ' '), sep = '\t')),
                    max_try,
                    {logg(paste(year, '|', paste('uploaded', basename(tmp[i]), sep = ' '), sep = '\t')); unlink(tmp[i])}))
    }
    rm(mirror)
    opcounter[[year]] <- counter
  }
  opcounter <- do.call(rbind, opcounter)
  return(opcounter)
}

logg(paste('0000', '|', 'cluster started', sep = '\t'))
tbl_dinfo <- sdf_copy_to(sc, dinfo, repartition = ceiling(n_d/2))
dist_counter <- spark_apply(tbl_dinfo, dist_codes, packages = F) %>% collect()
logg(paste('0000', '|', 'cluster ended', sep = '\t'))
write.csv(dist_counter, file = opcounter, row.names = F)
spark_disconnect(sc)
put_object(oplog, basename(oplog), bucket = out_bucket)
put_object(opcounter, basename(opcounter), bucket = out_bucket)
# system('sudo shutdown')
