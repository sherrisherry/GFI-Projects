rm(list=ls()) # clean up environment
pkgs <- c('aws.s3', 'aws.ec2metadata', 'jsonlite', 'scripting', 'data.table')
for(i in pkgs)library(i, character.only = T)

#=====================================modify the following parameters for each new run==============================================#

usr <- 'aws00' # the user account for using AWS service
years <- 2016:2001 # the years we want to download
in_bucket <- 'gfi-mirror-analysis' # read in raw data from this bucket
sup_bucket <- 'gfi-supplemental' # supplemental files
tag <- "Comtrade"
oplog <- 'matchver.log' # progress report file
max_try <- 10 # the maximum number of attempts for a failed process
keycache <- read.csv('~/vars/accesscodes.csv', header = TRUE, stringsAsFactors = FALSE) # the database of our credentials

cols_match <- c(rep("character",3),rep("integer",2),rep("numeric",8),rep("integer",2))
names(cols_match) <- c("hs_rpt","hs_ptn","k","i","j","v_M","v_X","v_rX","v_rM","q_M","q_X","q_kg_M","q_kg_X","q_code_M","q_code_X")

#===================================================================================================================================#

oplog <- paste('logs/', oplog, sep = '')
logg <- function(x)mklog(x, path = oplog)
Sys.setenv("AWS_ACCESS_KEY_ID" = keycache$Access_key_ID[keycache$service==usr],
           "AWS_SECRET_ACCESS_KEY" = keycache$Secret_access_key[keycache$service==usr])
if(is.na(Sys.getenv()["AWS_DEFAULT_REGION"]))Sys.setenv("AWS_DEFAULT_REGION" = gsub('.{1}$', '', metadata$availability_zone()))
options(stringsAsFactors= FALSE)
cat('Time\tZone\tYear\tMark\tStatus\n', file = oplog, append = FALSE)
for(year in years){
  M <- paste(tag, year, 'M-matched.csv.bz2', sep = '-')
  ecycle(save_object(object = M, bucket = in_bucket, file = 'tmp/tmp.csv.bz2', overwrite = TRUE),
         {logg(paste(year, '!', 'retrieving M file failed', sep = '\t')); next}, max_try)
  ecycle(M <- fread(cmd="bzip2 -dkc ./tmp/tmp.csv.bz2", header=T, colClasses = cols_match),
         {logg(paste(year, '!', 'loading M file failed', sep = '\t')); next}, max_try,
         cond = is.data.table(M) && nrow(M)>10)
  X <- paste(tag, year, 'X-matched.csv.bz2', sep = '-')
  ecycle(save_object(object = X, bucket = in_bucket, file = 'tmp/tmp.csv.bz2', overwrite = TRUE),
         {logg(paste(year, '!', 'retrieving X file failed', sep = '\t')); next}, max_try)
  ecycle(X <- fread(cmd="bzip2 -dkc ./tmp/tmp.csv.bz2", header=T, colClasses = cols_match),
         {logg(paste(year, '!', 'loading X file failed', sep = '\t')); next}, max_try,
         cond = is.data.table(X) && nrow(X)>10)
  tmp <- colnames(X)
  colnames(X)[match(c("hs_rpt","hs_ptn","i","j"), tmp)] <- c("hs_ptn","hs_rpt","j","i")
  d <- duplicated(rbind(M, X))
  if(sum(d)!=nrow(M))logg(paste(year, '!', 'MX not identical', sep = '\t'))else logg(paste(year, '|', 'identical', sep = '\t'))
}
