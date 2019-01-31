# 1. automatically download Comtrade data that is newer than our existing data from the UN;
# 2. decompress and subset the raw data to only keep aggregation level 6 and remove country names and commodity names;
# 3. compress the results and send them to AWS S3;
# 4. record the information of downloaded data;
# 5. send a progress report to Google Drive;
# 6. shutdown system after completion.

rm(list=ls()) # clean up environment
library("aws.s3")
library('aws.ec2metadata')
library('jsonlite')
library('googledrive')

#=====================================modify the following parameters for each new run==============================================#

usr <- 'aws00' # the user account for using AWS service
client <- 'comtrade00' # the user account for downloading Comtrade data
years <- 2016:2000 # the years we want to download
bucket <- 'gfi-comtrade' # save the results to a S3 bucket called 'gfi-comtrade'
subdir <- NULL # currently not using
oplog <- 'bulk_download.log' # progress report file
dinfo <- 'download.log' # file of the information of the downloaded data
max_try <- 10 # the maximum number of attempts for a failed process
keycache <- read.csv('~/vars/accesscodes.csv', header = TRUE, stringsAsFactors = FALSE) # the database of our credentials
googlekey <- '~/vars/googleapi.json' # Google service credentials
gpath <- 'UN-Comtrade' # the folder name of this project on Google Drive
# column names of the raw data; if it gets changed, we may also need to change the subsetting code.
col_names_UN <- c("classification","year","period","perioddesc","aggregatelevel","isleafcode","tradeflowcode",
                  "tradeflow","reportercode","reporter","reporteriso","partnercode","partner","partneriso",
                  "commoditycode","commodity","qtyunitcode","qtyunit","qty","netweightkg","tradevalueus","flag")

#===================================================================================================================================#
				  
oplog <- paste('logs/', oplog, sep = '')
dinfo <- paste('logs/', dinfo, sep = '')
token <- function(){
  url <- paste('https://comtrade.un.org/api/getAuthToken?username=',
               keycache$user[keycache$service==client],'&password=',
               keycache$password[keycache$service==client], sep = '')
  fromJSON(url)
}
logg <- function(x){
  txt <- paste(Sys.time(), Sys.timezone(), x, sep = '\t')
  cat(paste(txt,'\n', sep=''), file = oplog, append = TRUE)
}
Sys.setenv("AWS_ACCESS_KEY_ID" = keycache$Access_key_ID[keycache$service==usr],
           "AWS_SECRET_ACCESS_KEY" = keycache$Secret_access_key[keycache$service==usr])
if(is.na(Sys.getenv()["AWS_DEFAULT_REGION"]))Sys.setenv("AWS_DEFAULT_REGION" = gsub('.{1}$', '', metadata$availability_zone()))
options(stringsAsFactors= FALSE)
cat(NULL, file = oplog, append = FALSE)
dlog <- read.csv(dinfo, header = TRUE)

for(t in years){
  info <- NULL
  trycount <- 0
  while(is.null(info)&&trycount <= max_try){
    try(info <- fromJSON(paste('http://comtrade.un.org/api//refs/da/bulk?r=ALL&freq=A&type=C&px=HS&ps=',t,'&token=',token(),sep='')))
    trycount <- trycount + 1
  }
  if(!is.list(info)){
    logg(paste(t, '|', 'failed to obtain info', sep = '\t'))
    next
  }
  trycount <- 0
  if(nrow(info)!=1){
    logg(paste(t, '|', 'more than one file', sep = '\t'))
    next
  }
  if(sum(dlog$Year==t)>0&&dlog[dlog$Year==t, 'extractDate']==info$extractDate){
    logg(paste(t, '!', 'already the newest', sep = '\t'))
    next
  }
  msg <- 1
  while(msg != 0 && trycount <= max_try){
    try(msg <- download.file(paste('http://comtrade.un.org',info$downloadUri,'?token=',token(), sep = ''), 
                             destfile = 'tmp/tmp.zip'))
    trycount <- trycount + 1
  }
  if(msg!=0){
    logg(paste(t, '|', 'download failed', sep = '\t'))
    next
  }
  trycount <- 0
  rdata <- FALSE
  try(rdata <- read.csv(unz("tmp/tmp.zip",sub('.zip','.csv',info$name)), header=T))
  if(!is.data.frame(rdata)){
    logg(paste(t, '|', 'reading file failed', sep = '\t'))
    next
  }
  trycount <- 0
  logg(paste(t, ':', 'opened', sep = '\t'))
  colnames(rdata) <- col_names_UN
  rdata <- rdata[rdata$aggregatelevel==6, c(-10,-13,-16)] # remove reporter, partner, commodity
  logg(paste(t, ':', 'subseted', sep = '\t'))
  bak <- paste('tmp/', t, '.csv.xz', sep = '')
  class(msg) <- 'try-error'
  while(class(msg)=='try-error' && trycount <= max_try){
    msg <- try(write.csv(rdata, file=xzfile(bak), row.names = FALSE))
    trycount <- trycount + 1
  }
  trycount <- 0
  if(class(msg)=='try-error'){
    logg(paste(t, '|', 'saving file failed', sep = '\t'))
    msg <- FALSE
    while(!msg && trycount <= max_try){
      try(msg <- s3write_using(rdata, FUN = function(x, y)write.csv(x, file=xzfile(y), row.names = FALSE),
                           bucket = bucket,
                           object = paste(t, '.csv.xz', sep = '')))
      trycount <- trycount + 1
    }
  }else{
    msg <- FALSE
    while(!msg && trycount <= max_try){
      try(msg <- put_object(bak, paste(t, '.csv.xz', sep = ''), bucket = bucket))
      trycount <- trycount + 1
    }
  }
  if(!msg){
    logg(paste(t, '|', 'uploading file failed', sep = '\t'))
    next
  }
  logg(paste(t, '!', 'uploaded', sep = '\t'))
  unlink(bak)
  if(sum(dlog$Year==t)>0){
    dlog[dlog$Year==t, c('extractDate','publicationDate')]<-c(info$extractDate,info$publicationDate)
  }else{dlog <- rbind(dlog,c(t,info$extractDate,info$publicationDate))}
  write.csv(dlog, dinfo, row.names = FALSE)
  rm(rdata, info)
}

put_object(oplog, oplog, bucket = bucket)
put_object(dinfo, dinfo, bucket = bucket)

class(msg) <- 'try-error'
while(class(msg)=='try-error' && trycount <= max_try){
  msg <- try({drive_auth(oauth_token = NULL, service_token = '~/vars/googleapi.json', reset = FALSE,
                         cache = getOption("httr_oauth_cache"),
                         use_oob = getOption("httr_oob_default"), verbose = TRUE)
    tmp <- paste(gpath, oplog, sep = '/')
    drive_rm(tmp)
    drive_upload(oplog, path = tmp)
    tmp <- paste(gpath, dinfo, sep = '')
    drive_rm(tmp)
    drive_upload(dinfo, path = tmp)})
}
if(class(msg)=='try-error')logg(paste(t, '|', 'google sync failed', sep = '\t'))

system('shutdown -s')
