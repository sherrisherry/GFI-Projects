rm(list=ls()) # clean up environment
library("aws.s3")
library('aws.ec2metadata')
library('jsonlite')
library('scripting')
library('data.table')

#=====================================modify the following parameters for each new run==============================================#

usr <- 'aws00' # the user account for using AWS service
dates <- 2016:2001 # the years we want to download
out_bucket <- 'gfi-mirror-analysis' # save the results to a S3 bucket called 'gfi-mirror-analysis'
in_bucket <- 'gfi-mirror-analysis' # read in raw data from this bucket
sup_bucket <- 'gfi-supplemental' # supplemental files
tag <- "Comtrade"
oplog <- 'model_input_adj.log' # progress report file
dinfo <- 'bulk_download.log' # file of the information of the downloaded data
max_try <- 10 # the maximum number of attempts for a failed process
keycache <- read.csv('~/vars/accesscodes.csv', header = TRUE, stringsAsFactors = FALSE) # the database of our credentials

cols_match <- c(rep("character",3),rep("integer",2),rep("numeric",8),rep("integer",2))
names(cols_match) <- c("hs_rpt","hs_ptn","k","i","j","v_M","v_X","v_rX","v_rM","q_M","q_X","q_kg_M","q_kg_X","q_code_M","q_code_X")
cols_fob <- rep("integer",3)
names(cols_fob) <- c("t","i","d_fob")
cols_bridge <- rep('NULL',12)
names(cols_bridge) <- c('i_imf','i_imf_name','i_unct','i_unct_name','d_gfi','d_dev','d_ssa','d_asia','d_deur','d_mena','d_whem','d_adv')
cols_bridge[c(3,6)] <- rep('integer',2)
cols_geo <- c(rep("integer",2),rep("NULL",2),"numeric",rep("integer",4))
names(cols_geo) <- c("j","i","area_j","area_i","distw","d_landlocked_j","d_landlocked_i","d_contig","d_conti")
cols_eia <- rep("integer",4)
names(cols_eia) <- c("t","i","j","value")
#===================================================================================================================================#
				  
oplog <- paste('logs/', oplog, sep = '')
logg <- function(x)mklog(x, path = oplog)
Sys.setenv("AWS_ACCESS_KEY_ID" = keycache$Access_key_ID[keycache$service==usr],
           "AWS_SECRET_ACCESS_KEY" = keycache$Secret_access_key[keycache$service==usr])
if(is.na(Sys.getenv()["AWS_DEFAULT_REGION"]))Sys.setenv("AWS_DEFAULT_REGION" = gsub('.{1}$', '', metadata$availability_zone()))
options(stringsAsFactors= FALSE)
cat('Time\tZone\tYear\tMark\tStatus\n', file = oplog, append = FALSE)

ecycle(fob <- s3read_using(FUN = function(x)fread(x, colClasses=cols_fob, header=TRUE, na.strings=""), 
                                 object = 'FOB.csv', bucket = sup_bucket),
           {logg(paste(year, '!', 'loading FOB.csv failed', sep = '\t')); stop()}, max_try)
fob <- subset(fob,fob$t %in% dates)
setkeyv(fob, c('t','i'))
ecycle(bridge <- s3read_using(FUN = function(x)fread(x, colClasses=cols_bridge, header=TRUE, na.strings=""), 
                                 object = 'Bridge.csv', bucket = sup_bucket),
           {logg(paste(year, '!', 'loading Bridge.csv failed', sep = '\t')); stop()}, max_try)
colnames(bridge)[1] <- 'i'
bridge <- unique(bridge)
setkey(bridge, 'i')
ecycle(geo <- s3read_using(FUN = function(x)fread(x, colClasses=cols_geo, header=TRUE, na.strings=""), 
                                 object = 'CEPII_GeoDist.csv', bucket = sup_bucket),
           {logg(paste(year, '!', 'loading CEPII_GeoDist.csv failed', sep = '\t')); next}, max_try)
geo <- subset(geo,geo$j!=geo$i)
setkeyv(geo, c('i','j'))
ecycle(save_object(object = 'EIA.csv.bz2', bucket = sup_bucket, file = 'tmp/tmp.csv.bz2', overwrite = TRUE),
		   {logg(paste(year, '!', 'retrieving EIA file failed', sep = '\t')); stop()}, max_try)
ecycle(eia <- fread(cmd ="bzip2 -dkc ./tmp/tmp.csv.bz2", header=T, colClasses = cols_eia),
		   {logg(paste(year, '!', 'loading file failed', sep = '\t')); stop()}, max_try)
colnames(eia)[4] <- 'd_rta'
eia <- subset(eia, eia$t %in% dates)
setkeyv(eia, c('t','i','j'))
logg(paste('0000', ':', 'prepared', sep = '\t'))

for(year in dates){
obj_nm <- paste(tag, year, 'M-matched-q.csv.bz2', sep = '-')
ecycle(save_object(object = obj_nm, bucket = in_bucket, file = 'tmp/tmp.csv.bz2', overwrite = TRUE),
		   {logg(paste(year, '!', 'retrieving file failed', sep = '\t')); next}, max_try)
ecycle(output <- fread(cmd="bzip2 -dkc ./tmp/tmp.csv.bz2", header=T, colClasses = cols_match),
		   {logg(paste(year, '!', 'loading file failed', sep = '\t')); next}, max_try,
		   cond = is.data.table(output) && nrow(output)>10)
setkeyv(output, c('i','j'))
output <- merge(output, fob[fob$t==year,c("i","d_fob")], by = 'i', all.x = T)
tmp <- colnames(output); colnames(output)[match('d_fob',tmp)]<-'d_fob_i'
output <- merge(output, fob[fob$t==year,c("i","d_fob")], by.x = 'j', by.y = 'i', all.x = T)
tmp <- colnames(output); colnames(output)[match('d_fob',tmp)]<-'d_fob_j'
logg(paste(year, ':', 'merged FOB', sep = '\t'))
output <- merge(output, bridge, by.x = 'i', by.y = 'i', all.x = T)
tmp <- colnames(output); colnames(output)[match('d_dev',tmp)]<-'d_dev_i'
output <- merge(output, bridge, by.x = 'j', by.y = 'i', all.x = T)
tmp <- colnames(output); colnames(output)[match('d_dev',tmp)]<-'d_dev_j'
logg(paste(year, ':', 'merged Bridge', sep = '\t'))
output <- merge(output, geo, by=c("i","j"), all.x=TRUE)
logg(paste(year, ':', 'merged Geo', sep = '\t'))
output <- merge(output, eia[eia$t==year,],by=c("i","j"),all.x=TRUE)
logg(paste(year, ':', 'merged EIA', sep = '\t'))
obj_nm <- paste('tmp/', obj_nm, sep = '')
ecycle(write.csv(output, file = bzfile(obj_nm),row.names=FALSE,na=""), 
             ecycle(s3write_using(output, FUN = function(x, y)write.csv(x, file=bzfile(y), row.names = FALSE),
                                  bucket = out_bucket, object = basename(obj_nm)),
                     logg(paste(year, '!', paste('uploading', basename(obj_nm), 'failed', sep = ' '), sep = '\t')), max_try), 
             max_try,
             ecycle(put_object(obj_nm, basename(obj_nm), bucket = out_bucket), 
                    logg(paste(year, '!', paste('uploading', basename(obj_nm), 'failed', sep = ' '), sep = '\t')),
                    max_try,
                    {logg(paste(year, '|', paste('uploaded', basename(obj_nm), sep = ' '), sep = '\t')); unlink(obj_nm)}))
}		   

unlink('tmp/tmp.csv.bz2')
rm(list=ls())
