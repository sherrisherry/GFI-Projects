# 'p' = positive; 'n' = negative; 'x' = zero
# 'mu' = import underinvoicing; 'mo' = import overinvoicing; 'ng' = no gap
# 'eu' = export underinvoicing; 'eo' = export overinvoicing

rm(list=ls()) # clean up environment
pkgs <- c('aws.s3', 'aws.ec2metadata', 'stats', 'scripting', 'remotes')
for(i in pkgs)library(i, character.only = T)
install_github("sherrisherry/GFI-Cloud", subdir="pkg")

#=====================================modify the following parameters for each new run==============================================#

usr <- 'aws00' # the user account for using AWS service
years <- 2016:2001 # the years we want to download
out_bucket <- 'gfi-results' # save the results to a S3 bucket called 'gfi-mirror-analysis'
in_bucket <- 'gfi-work' # read in raw data from this bucket
sup_bucket <- 'gfi-supplemental' # supplemental files
tag <- "Comtrade"
oplog <- 'trf.log' # progress report file
max_try <- 10 # the maximum number of attempts for a failed process
keycache <- read.csv('~/vars/accesscodes.csv', header = TRUE, stringsAsFactors = FALSE) # the database of our credentials
all_trade <- TRUE
cty <- 170 # set to NULL to use all countries within GFI's consideration
infile <- paste('data/', 'flow_k6_', paste(cty, collapse = '-'), all_trade, min(years), max(years), '.csv', sep = '')
trfile <- paste('data/', 'trf_', paste(cty, collapse = '-'), '_', min(years), max(years), '.csv', sep = '')
k_digit <- 2 # the number of digits of HS codes to be aggregated to
cols_in <- c(rep('integer', 5),'character', rep('numeric',3), rep('NULL',5))
names(cols_in) <- c("t","j","i","d_dev_i","d_dev_j","k","v_X",'v_M_fob','gap_wtd',
                    "q_code_M","q_M","q_X","v_M", 'a_wt')
cols_bridge <- bridge_cols(c('un_code','wb_code'),rep('integer',2))
k_len <- 6

#===================================================================================================================================#

oplog <- paste('logs/', oplog, sep = '')
logg <- function(x)mklog(x, path = oplog)
Sys.setenv("AWS_ACCESS_KEY_ID" = keycache$Access_key_ID[keycache$service==usr],
           "AWS_SECRET_ACCESS_KEY" = keycache$Secret_access_key[keycache$service==usr])
if(is.na(Sys.getenv()["AWS_DEFAULT_REGION"]))Sys.setenv("AWS_DEFAULT_REGION" = gsub('.{1}$', '', metadata$availability_zone()))
options(stringsAsFactors= FALSE)
cat('Time\tZone\tYear\tMark\tStatus\n', file = oplog, append = FALSE)
options(stringsAsFactors= FALSE)
agg_lv <- paste('k', k_digit, sep = '')
k_digit <- k_len - k_digit

ecycle(bridge <- s3read_using(FUN = function(x)read.csv(x, colClasses=cols_bridge, header=TRUE), 
                              object = 'bridge.csv', bucket = sup_bucket),
       {logg(paste('0000', '!', 'loading bridge.csv failed', sep = '\t')); stop()}, max_try)
bridge <- unique(bridge)

tm <- read.csv(infile)
trf <- read.csv(trfile)

tm <- split(tm, tm$f=='mu')
tm[['TRUE']] <- merge(x= tm[['TRUE']], y= bridge, by.x='j', by.y = 'un_code', all.x=T)
colnames(tm[['TRUE']])[match('wb_code',colnames(tm[['TRUE']]))]<-'wb_code_j'
if(length(cty>1)){
  tm[['TRUE']] <- merge(x= tm[['TRUE']], y= bridge, by.x='i', by.y = 'un_code', all.x=T)
  colnames(tm[['TRUE']])[match('wb_code',colnames(tm[['TRUE']]))]<-'wb_code_i'
  tm[['TRUE']] <- merge(x= tm[['TRUE']], y= trf, by=c('t', 'wb_code_i','wb_code_j'), all.x=T)
  tm[['TRUE']][, c('wb_code_i','wb_code_j')] <- NULL
}else{
  tm[['TRUE']] <- merge(x= tm[['TRUE']], y= trf, by=c('t', 'wb_code_j'), all.x=T)
  tm[['TRUE']][, 'wb_code_j'] <- NULL
}
tm[['FALSE']][, 'trf_rate'] <- NA
tm <- do.call(rbind, tm)

outfile <- paste('tmp/', 'flowtrf_', paste(cty, collapse = '-'), '_', min(years), max(years), '.csv', sep = '')
# write.csv(output, file= outfile,row.names = F)
ecycle(write.csv(output, file = bzfile(outfile),row.names=FALSE,na=""), 
       ecycle(s3write_using(output, FUN = function(x, y)write.csv(x, file=bzfile(y), row.names = F, na=""),
                            bucket = out_bucket, object = basename(outfile)),
              logg(paste(year, '!', paste('uploading', basename(outfile), 'failed', sep = ' '), sep = '\t')), max_try), 
       max_try,
       ecycle(put_object(outfile, basename(outfile), bucket = out_bucket), 
              logg(paste(year, '!', paste('uploading', basename(outfile), 'failed', sep = ' '), sep = '\t')),
              max_try,
              {logg(paste(year, '|', paste('uploaded', basename(outfile), sep = ' '), sep = '\t')); unlink(outfile)}))
put_object(oplog, basename(oplog), bucket = out_bucket)
rm(list=ls())
