# 1. automatically download Comtrade data that is newer than our existing data from the UN;
# 2. decompress and subset the raw data to only keep aggregation level 6 and remove country names and commodity names;
# 3. compress the results and send them to AWS S3;
# 4. record the information of downloaded data;
# 5. shutdown system after completion.

rm(list=ls()) # clean up environment
pkgs <- c('aws.s3', 'aws.ec2metadata', 'jsonlite', 'scripting', 'remotes', 'data.table')
for(i in pkgs)library(i, character.only = T)
install_github("sherrisherry/GFI-Cloud", subdir="pkg")

#=====================================modify the following parameters for each new run==============================================#

usr <- 'aws00' # the user account for using AWS service
client <- 'comtrade00' # the user account for downloading Comtrade data
years <- c(2010:2001, 2016:2011) # the years we want to download, order as download order
bucket <- 'gfi-comtrade' # save the results to a S3 bucket called 'gfi-comtrade'
subdir <- NULL # currently not using
oplog <- 'bulk_download.log' # progress report file
dinfo <- 'download.log' # file of the information of the downloaded data
max_try <- 10 # the maximum number of attempts for a failed process
keycache <- read.csv('~/vars/accesscodes.csv', header = TRUE, stringsAsFactors = FALSE) # the database of our credentials
excol <- c('reporter', 'reporteriso', 'partner', 'partneriso', 'commodity', 'qtyunit')
colc_UN <- c("character","integer","integer","character","integer","integer","integer",
                    "character","character","character","character","character","character",
                    "character","character","character","character","character","numeric",
                    "numeric","numeric","integer")
coln_UN <- c("classification","year","period","perioddesc","aggregatelevel","isleafcode","tradeflowcode",
                  "tradeflow","reportercode","reporter","reporteriso","partnercode","partner","partneriso",
                  "commoditycode","commodity","qtyunitcode","qtyunit","qty","netweightkg","tradevalues","flag")
# don't use named vector because 'read' tries to match input colnames with vector names even with header=F

#===================================================================================================================================#
				  
oplog <- file.path('logs', oplog); dinfo <- file.path('logs', dinfo)
excol <- match(excol, coln_UN)
coln_UN <- coln_UN[-excol]
colc_UN[excol] <- 'NULL'
token <- function(){
  url <- paste('https://comtrade.un.org/api/getAuthToken?username=',
               keycache$user[keycache$service==client],'&password=',
               keycache$password[keycache$service==client], sep = '')
  fromJSON(url)
}
logg <- function(x)mklog(x, path = oplog)
ec2env(keycache,usr)
options(stringsAsFactors= FALSE)
cat('Time\tZone\tYear\tMark\tStatus\n', file = oplog, append = FALSE)
dlog <- read.csv(dinfo, header = TRUE)

for(t in years){
  ecycle(info <- fromJSON(paste('http://comtrade.un.org/api//refs/da/bulk?r=ALL&freq=A&type=C&px=HS&ps=',t,'&token=',token(),sep='')),
         {logg(paste(t, '!', 'failed to obtain info', sep = '\t')); next}, max_try, cond = is.list(info))
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
  ecycle(msg <- download.file(paste('http://comtrade.un.org',info$downloadUri,'?token=',token(), sep = ''), destfile = 'tmp/tmp.zip'),
         {logg(paste(t, '!', 'download failed', sep = '\t')); next}, max_try, cond = msg == 0)
  logg(paste(t, ':', 'downloaded', sep = '\t'))
  rdata <- FALSE
#  try(rdata <- read.csv(pipe('./prepd.sh'), header = F, colClasses = colc_UN))
  try(rdata <- fread(cmd = "./prepd.sh", header=F, colClasses = colc_UN))
  # pipe stream usually stripes out the final blank line; add a blank line to the end output or fread fails:
  # unz() the whole file is extremely IO intensive, thus slow.
  if(!is.data.frame(rdata) || nrow(rdata)<10){
    logg(paste(t, '!', 'reading file failed', sep = '\t'))
    next
  }
  logg(paste(t, ':', 'opened', sep = '\t'))
  tmp <- nrow(rdata)
  logg(paste(t, '#', tmp, sep = '\t'))
  if(ncol(rdata)!=length(coln_UN)){
    logg(paste(t, '!', '# of cols mismatched', sep = '\t'))
    next
  }
  colnames(rdata) <- coln_UN
  rdata <- rdata[rdata$aggregatelevel==6, ] # double check
  logg(paste(t, ':', 'subseted', sep = '\t'))
  if(nrow(rdata)!=tmp){
    logg(paste(t, '!', '# of rows mismatched', sep = '\t')); stop('# of rows mismatched')
  }
  bak <- paste('tmp/', t, '.csv.bz2', sep = '')
  ecycle(write.csv(rdata, file=bzfile(bak), row.names = FALSE),
         ecycle(s3write_using(rdata, FUN = function(x, y)write.csv(x, file=bzfile(y), row.names = FALSE),
                           bucket = bucket, object = basename(bak)),
                {logg(paste(t, '!', 'uploading file failed', sep = '\t')); next}, max_try),
         max_try,
         ecycle(msg <- put_object(bak, paste(t, '.csv.bz2', sep = ''), bucket = bucket),
                {logg(paste(t, '!', 'uploading file failed', sep = '\t')); next},
                max_try,
                unlink(bak), cond = msg))
  logg(paste(t, '|', 'uploaded', sep = '\t'))
  if(sum(dlog$Year==t)>0){
    dlog[dlog$Year==t, c('extractDate','publicationDate')]<-c(info$extractDate,info$publicationDate)
  }else{dlog <- rbind(dlog,c(t,info$extractDate,info$publicationDate))}
  write.csv(dlog, dinfo, row.names = FALSE)
  rm(rdata, info)
}

put_object(oplog, basename(oplog), bucket = bucket)
put_object(dinfo, basename(dinfo), bucket = bucket)

unlink('tmp/tmp.zip')

rm(list=ls())
system('sudo shutdown')
