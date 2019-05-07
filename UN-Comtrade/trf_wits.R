# 'p' = positive; 'n' = negative; 'x' = zero
# 'mu' = import underinvoicing; 'mo' = import overinvoicing; 'ng' = no gap
# 'eu' = export underinvoicing; 'eo' = export overinvoicing

rm(list=ls()) # clean up environment
pkgs <- c('aws.s3', 'aws.ec2metadata', 'stats', 'batchscr', 'remotes', 'data.table')
for(i in pkgs)library(i, character.only = T)
install_github("sherrisherry/GFI-Cloud", subdir="pkg"); library(pkg)

#=====================================modify the following parameters for each new run==============================================#

usr <- 'aws00' # the user account for using AWS service
years <- 2016:2015 # the years we want to download
out_bucket <- 'gfi-results' # save the results to a S3 bucket called 'gfi-mirror-analysis'
in_bucket <- 'gfi-work' # read in raw data from this bucket
sup_bucket <- 'gfi-supplemental' # supplemental files
tag <- "Comtrade"
oplog <- 'trf.log' # progress report file
max_try <- 10 # the maximum number of attempts for a failed process
keycache <- read.csv('~/vars/accesscodes.csv', header = TRUE, stringsAsFactors = FALSE) # the database of our credentials
all_trade <- TRUE
cty <- 170 # set to NULL to use all countries within GFI's consideration
infile <- paste('data/', 'f_k6_', paste(cty, collapse = '-'), all_trade, min(years), max(years), '.csv', sep = '')
trfile <- paste('data/', 'trf_', paste(cty, collapse = '-'), '_', min(years), max(years), '.csv', sep = '')
k_digit <- 2 # the number of digits of HS codes to be aggregated to
cols_in <- c(rep('integer', 3),rep('character',3), rep('numeric',3))
names(cols_in) <- c('t','j','i','k','f','mx','v_j','v_i','gap_wtd')
cols_trf <- c(rep('integer', 3), rep('character',2), 'numeric')
names(cols_trf) <- c('t', 'wb_code_i', 'wb_code_j', 'k', 'source', 'trf_wtd')
k_len <- 6

#===================================================================================================================================#

oplog <- file.path('logs', oplog)
logg <- function(x)mklog(x, path = oplog)
ec2env(keycache,usr)
options(stringsAsFactors= FALSE)
cat('Time\tZone\tYear\tMark\tStatus\n', file = oplog, append = FALSE)
options(stringsAsFactors= FALSE)
agg_lv <- paste('k', k_digit, sep = '')
k_digit <- k_len - k_digit

bridge <- in_bridge(c('un_code','wb_code'), rep('integer',2), logg, max_try)
if(is.null(bridge))stop()
bridge <- unique(bridge)

tm <- read.csv(infile, colClasses = cols_in)
trf <- read.csv(trfile, colClasses = cols_trf)

tm <- split(tm, tm$f=='mu')
tm[['TRUE']] <- merge(x= tm[['TRUE']], y= bridge, by.x='j', by.y = 'un_code', all.x=T)
colnames(tm[['TRUE']])[match('wb_code',colnames(tm[['TRUE']]))]<-'wb_code_j'
if(length(cty>1)){
  tm[['TRUE']] <- merge(x= tm[['TRUE']], y= bridge, by.x='i', by.y = 'un_code', all.x=T)
  colnames(tm[['TRUE']])[match('wb_code',colnames(tm[['TRUE']]))]<-'wb_code_i'
  tm[['TRUE']] <- merge(x= tm[['TRUE']], y= trf, by=c('t', 'wb_code_i','wb_code_j', 'k'), all.x=T)
}else{
  tm[['TRUE']] <- merge(x= tm[['TRUE']], y= trf, by=c('t', 'wb_code_j', 'k'), all.x=T)
}
tm[['TRUE']][, c('wb_code_i','wb_code_j')] <- NULL
tm[['FALSE']][, c('source','trf_wtd')] <- NA
tm <- do.call(rbind, tm)

outfile <- paste('tmp/', 'flowtrf_', paste(cty, collapse = '-'), '_', min(years), max(years), '.csv', sep = '')
# write.csv(tm, file= outfile,row.names = F)
ecycle(write.csv(tm, file = bzfile(outfile),row.names=FALSE,na=""), 
       ecycle(s3write_using(tm, FUN = function(x, y)write.csv(x, file=bzfile(y), row.names = F, na=""),
                            bucket = out_bucket, object = basename(outfile)),
              logg(paste(year, '!', paste('uploading', basename(outfile), 'failed', sep = ' '), sep = '\t')), max_try), 
       max_try,
       ecycle(put_object(outfile, basename(outfile), bucket = out_bucket), 
              logg(paste(year, '!', paste('uploading', basename(outfile), 'failed', sep = ' '), sep = '\t')),
              max_try,
              {logg(paste(year, '|', paste('uploaded', basename(outfile), sep = ' '), sep = '\t')); unlink(outfile)}))
put_object(oplog, basename(oplog), bucket = out_bucket)
rm(list=ls())
