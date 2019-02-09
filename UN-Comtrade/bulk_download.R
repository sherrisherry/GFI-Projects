# 1. automatically download Comtrade data that is newer than our existing data from the UN;
# 2. decompress and subset the raw data to only keep aggregation level 6 and remove country names and commodity names;
# 3. compress the results and send them to AWS S3;
# 4. record the information of downloaded data;
# 5. shutdown system after completion.

rm(list=ls()) # clean up environment
library("aws.s3")
library('aws.ec2metadata')
library('jsonlite')
library('scripting')
library('data.table')

#=====================================modify the following parameters for each new run==============================================#

usr <- 'aws00' # the user account for using AWS service
client <- 'comtrade00' # the user account for downloading Comtrade data
years <- c(2016:2000, 2017) # the years we want to download, order as download order
bucket <- 'gfi-comtrade' # save the results to a S3 bucket called 'gfi-comtrade'
subdir <- NULL # currently not using
oplog <- 'bulk_download.log' # progress report file
dinfo <- 'download.log' # file of the information of the downloaded data
max_try <- 10 # the maximum number of attempts for a failed process
keycache <- read.csv('~/vars/accesscodes.csv', header = TRUE, stringsAsFactors = FALSE) # the database of our credentials
# column names of the raw data; if it gets changed, we may also need to change the subsetting code.
excol <- c('reporter', 'partner', 'commodity')
colc_UN <- c("character","integer","integer","character","integer","integer","integer",
                    "character","character","character","character","character","character",
                    "character","character","character","character","character","numeric",
                    "numeric","numeric","integer")
coln_UN <- c("classification","year","period","perioddesc","aggregatelevel","isleafcode","tradeflowcode",
                  "tradeflow","reportercode","reporter","reporteriso","partnercode","partner","partneriso",
                  "commoditycode","commodity","qtyunitcode","qtyunit","qty","netweightkg","tradevalueus","flag")
# don't use named vector because 'read' tries to match input colnames with vector names even with header=F
# check nc match
# rbind(coln_UN, colc_UN)

#===================================================================================================================================#
				  
oplog <- paste('logs/', oplog, sep = '')
dinfo <- paste('logs/', dinfo, sep = '')
excol <- match(excol, coln_UN)
token <- function(){
  url <- paste('https://comtrade.un.org/api/getAuthToken?username=',
               keycache$user[keycache$service==client],'&password=',
               keycache$password[keycache$service==client], sep = '')
  fromJSON(url)
}
logg <- function(x)mklog(x, path = oplog)
Sys.setenv("AWS_ACCESS_KEY_ID" = keycache$Access_key_ID[keycache$service==usr],
           "AWS_SECRET_ACCESS_KEY" = keycache$Secret_access_key[keycache$service==usr])
if(is.na(Sys.getenv()["AWS_DEFAULT_REGION"]))Sys.setenv("AWS_DEFAULT_REGION" = gsub('.{1}$', '', metadata$availability_zone()))
options(stringsAsFactors= FALSE)
cat('Time\tZone\tYear\tMark\tStatus\n', file = oplog, append = FALSE)
dlog <- read.csv(dinfo, header = TRUE)

for(t in years){
  info <- NULL
  trycount <- 0
  while(is.null(info)&&trycount <= max_try){
    try(info <- fromJSON(paste('http://comtrade.un.org/api//refs/da/bulk?r=ALL&freq=A&type=C&px=HS&ps=',t,'&token=',token(),sep='')))
    trycount <- trycount + 1
  }
  if(!is.list(info)){
    logg(paste(t, '!', 'failed to obtain info', sep = '\t'))
    next
  }
  trycount <- 0
  if(length(info)<1){
    logg(paste(t, '!', 'not yet available', sep = '\t'))
    next
  }
  if(nrow(info)>1){
    logg(paste(t, '!', 'more than one file', sep = '\t'))
    next
  }
  if(sum(dlog$Year==t)>0&&dlog[dlog$Year==t, 'extractDate']==info$extractDate){
    logg(paste(t, '|', 'already the newest', sep = '\t'))
    next
  }
  msg <- 1
  while(msg != 0 && trycount <= max_try){
    try(msg <- download.file(paste('http://comtrade.un.org',info$downloadUri,'?token=',token(), sep = ''), 
                             destfile = 'tmp/tmp.zip'))
    trycount <- trycount + 1
  }
  if(msg!=0){
    logg(paste(t, '!', 'download failed', sep = '\t'))
    next
  }
  trycount <- 0
  rdata <- FALSE
  colc_UN[excol] <- 'NULL'
#  try(rdata <- read.csv(pipe('./prepd.sh'), header = F, colClasses = colc_UN))
  try(rdata <- fread(cmd = "./prepd.sh", header=F, colClasses = colc_UN))
  # add a blank line to the end output or fread fails:
  # Avoidable 73.574 seconds. This file is very unusual: it ends abruptly without a final newline, and also its size is a multiple of 4096 bytes.
  # pipe stream usually strippes out the final blank line.
  if(!is.data.frame(rdata) || nrow(rdata)<10){
    logg(paste(t, '!', 'reading file failed', sep = '\t'))
    next
  }
  trycount <- 0
  logg(paste(t, ':', 'opened', sep = '\t'))
  tmp <- nrow(rdata)
  logg(paste(t, ':', tmp, sep = '\t'))
  coln_UN <- coln_UN[-excol]
  if(ncol(rdata)!=length(coln_UN)){
    logg(paste(t, '!', '# of cols mismatched', sep = '\t'))
    next
  }
  colnames(rdata) <- coln_UN
  rdata <- rdata[rdata$aggregatelevel==6, ] # double check
  logg(paste(t, ':', 'subseted', sep = '\t'))
  if(nrow(rdata)!=tmp){
    logg(paste(t, '!', '# of rows mismatched', sep = '\t'))
  }
  bak <- paste('tmp/', t, '.csv.bz2', sep = '')
  class(msg) <- 'try-error'
  while(class(msg)=='try-error' && trycount <= max_try){
    tmpcon <- bzfile(bak, open = 'wb')
    msg <- try(write.csv(rdata, file=tmpcon, row.names = FALSE))
    close(tmpcon)
    trycount <- trycount + 1
  }
  trycount <- 0
  if(class(msg)=='try-error'){
    logg(paste(t, '!', 'saving file failed', sep = '\t'))
    msg <- FALSE
    while(!msg && trycount <= max_try){
      try(msg <- s3write_using(rdata, FUN = function(x, y)write.csv(x, file=bzfile(y), row.names = FALSE),
                           bucket = bucket,
                           object = paste(t, '.csv.bz2', sep = '')))
      trycount <- trycount + 1
    }
  }else{
    msg <- FALSE
    while(!msg && trycount <= max_try){
      try(msg <- put_object(bak, paste(t, '.csv.bz2', sep = ''), bucket = bucket))
      trycount <- trycount + 1
    }
  }
  if(!msg){
    logg(paste(t, '!', 'uploading file failed', sep = '\t'))
    next
  }
  logg(paste(t, '|', 'uploaded', sep = '\t'))
  unlink(bak)
  if(sum(dlog$Year==t)>0){
    dlog[dlog$Year==t, c('extractDate','publicationDate')]<-c(info$extractDate,info$publicationDate)
  }else{dlog <- rbind(dlog,c(t,info$extractDate,info$publicationDate))}
  write.csv(dlog, dinfo, row.names = FALSE)
  rm(rdata, info)
}

# put_object(oplog, oplog, bucket = bucket)
# put_object(dinfo, dinfo, bucket = bucket)

unlink('tmp/tmp.zip')

rm(list=ls())
# system('sudo shutdown')
