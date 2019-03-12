rm(list=ls()) # clean up environment
pkgs <- c('aws.s3', 'aws.ec2metadata', 'stats', 'scripting', 'data.table')
for(i in pkgs)library(i, character.only = T)

#=====================================modify the following parameters for each new run==============================================#

usr <- 'aws00' # the user account for using AWS service
dates <- 2016:2001 # the years we want to download
out_bucket <- 'gfi-work' # save the results to a S3 bucket called 'gfi-work'
in_bucket <- 'gfi-mirror-analysis' # read in raw data from this bucket
sup_bucket <- 'gfi-supplemental' # supplemental files
tag <- "Comtrade"
oplog <- 'input_adj.log' # progress report file
max_try <- 10 # the maximum number of attempts for a failed process
keycache <- read.csv('~/vars/accesscodes.csv', header = TRUE, stringsAsFactors = FALSE) # the database of our credentials

cols_match <- c(rep("character",3),rep("integer",2),rep("numeric",4),rep("integer",2),rep('NULL',4))
names(cols_match) <- c("hs_rpt","hs_ptn","k","i","j","v_M","v_X","q_M","q_X","q_code_M","q_code_X","v_rX","v_rM","q_kg_M","q_kg_X")
cols_fob <- rep("integer",2)
names(cols_fob) <- c("t","i")
cols_bridge <- rep('NULL',20)
names(cols_bridge) <- c('un_code','imf_code','imf_nm','unct_nm','d_gfi','d_dev','d_ssa','d_asia','d_deur','d_mena','d_whem','d_adv','un_nm_en','un_nm_en_full','un_nm_en_abbr','un_note','iso2_a','iso3_a','un_start','un_end')
cols_bridge[c(1,6)] <- rep('integer',2)
cols_geo <- c(rep("integer",2),rep("NULL",2),"numeric",rep("integer",4))
names(cols_geo) <- c("j","i","area_j","area_i","distw","d_landlocked_j","d_landlocked_i","d_contig","d_conti")
cols_eia <- rep("integer",4)
names(cols_eia) <- c("t","i","j","d_rta")
cols_out <- c('t',"k","i","j","v_M","v_X","q_M","q_X","q_code_M","q_code_X", 'ln_distw', 'ln_distw_squared', 'ln_uvmdn',
  'd_fob', 'd_contig', 'd_conti', 'd_rta', 'd_landlocked_i', 'd_landlocked_j', 'd_dev_i', 'd_dev_j', 'd_hs_diff')

#===================================================================================================================================#

oplog <- paste('logs/', oplog, sep = '')
logg <- function(x)mklog(x, path = oplog)
Sys.setenv("AWS_ACCESS_KEY_ID" = keycache$Access_key_ID[keycache$service==usr],
           "AWS_SECRET_ACCESS_KEY" = keycache$Secret_access_key[keycache$service==usr])
if(is.na(Sys.getenv()["AWS_DEFAULT_REGION"]))Sys.setenv("AWS_DEFAULT_REGION" = gsub('.{1}$', '', metadata$availability_zone()))
options(stringsAsFactors= FALSE)
cat('Time\tZone\tYear\tMark\tStatus\n', file = oplog, append = FALSE)

ecycle(fob <- s3read_using(FUN = function(x)fread(x, colClasses=cols_fob, header=TRUE, na.strings=""), 
                                 object = 'MFOB.csv', bucket = 'gfi-comtrade'),
           {logg(paste('0000', '!', 'loading MFOB.csv failed', sep = '\t')); stop()}, max_try)
fob <- subset(fob,fob$t %in% dates)
fob$d_fob <- 1
setkeyv(fob, c('t','i'))
ecycle(bridge <- s3read_using(FUN = function(x)fread(x, colClasses=cols_bridge, header=TRUE), 
                                 object = 'bridge.csv', bucket = sup_bucket),
           {logg(paste('0000', '!', 'loading bridge.csv failed', sep = '\t')); stop()}, max_try)
tmp <- colnames(bridge); colnames(bridge)[match('un_code', tmp)] <- 'i'
bridge <- unique(bridge)
setkey(bridge, 'i')
ecycle(geo <- s3read_using(FUN = function(x)fread(x, colClasses=cols_geo, header=TRUE, na.strings=""), 
                                 object = 'CEPII_GeoDist.csv', bucket = sup_bucket),
           {logg(paste('0000', '!', 'loading CEPII_GeoDist.csv failed', sep = '\t')); next}, max_try)
geo <- subset(geo,geo$j!=geo$i)
setkeyv(geo, c('i','j'))
ecycle(save_object(object = 'EIA.csv.bz2', bucket = sup_bucket, file = 'tmp/tmp.csv.bz2', overwrite = TRUE),
		   {logg(paste('0000', '!', 'retrieving EIA file failed', sep = '\t')); stop()}, max_try)
ecycle(eia <- fread(cmd ="bzip2 -dkc ./tmp/tmp.csv.bz2", header=T, colClasses = cols_eia),
		   {logg(paste('0000', '!', 'loading file failed', sep = '\t')); stop()}, max_try)
eia <- subset(eia, eia$t %in% dates)
eia_yr_max <- max(eia$t)
setkeyv(eia, c('t','i','j'))
logg(paste('0000', ':', 'prepared', sep = '\t'))

for(year in dates){
obj_nm <- paste(tag, year, 'M_matched.csv.bz2', sep = '-')
ecycle(save_object(object = obj_nm, bucket = in_bucket, file = 'tmp/tmp.csv.bz2', overwrite = TRUE),
		   {logg(paste(year, '!', 'retrieving file failed', sep = '\t')); next}, max_try)
ecycle(output <- fread(cmd="bzip2 -dkc ./tmp/tmp.csv.bz2", header=T, colClasses = cols_match),
		   {logg(paste(year, '!', 'loading file failed', sep = '\t')); next}, max_try,
		   cond = is.data.table(output) && nrow(output)>10)
setkeyv(output, c('i','j','k'))
output <- subset(output, output$v_X>0 & output$v_M>0)
logg(paste(year, ':', 'non0v', sep = '\t'))
logg(paste(year, '#', nrow(output), sep = '\t'))
output <- subset(output,output$q_code_M==output$q_code_X)
logg(paste(year, ':', 'q_code', sep = '\t'))
logg(paste(year, '#', nrow(output), sep = '\t'))
output <- subset(output, (output$q_M>0)&(output$q_X>0))
logg(paste(year, ':', 'non0q', sep = '\t'))
logg(paste(year, '#', nrow(output), sep = '\t'))
output <- merge(output, fob[fob$t==year,c("i","d_fob")], by = 'i', all.x = T)
output$d_fob[is.na(output$d_fob)] <- 0
logg(paste(year, ':', 'merged FOB', sep = '\t'))
output <- merge(output, bridge, by.x = 'i', by.y = 'i', all.x = T)
tmp <- colnames(output); colnames(output)[match('d_dev',tmp)]<-'d_dev_i'
output <- merge(output, bridge, by.x = 'j', by.y = 'i', all.x = T)
tmp <- colnames(output); colnames(output)[match('d_dev',tmp)]<-'d_dev_j'
logg(paste(year, ':', 'merged bridge', sep = '\t'))
output <- na.omit(output)
logg(paste(year, ':', 'nona_bridge', sep = '\t'))
logg(paste(year, '#', nrow(output), sep = '\t'))
output <- merge(output, geo, by=c("i","j"), all.x=TRUE)
logg(paste(year, ':', 'merged Geo', sep = '\t'))
output <- na.omit(output)
logg(paste(year, ':', 'nona_Geo', sep = '\t'))
logg(paste(year, '#', nrow(output), sep = '\t'))
if(year > eia_yr_max){
tmp <- eia[eia$t==eia_yr_max,]; tmp$t <- year
logg(paste(year, ':', 'extended EIA', sep = '\t'))
}else tmp <- eia[eia$t==year,]
output <- merge(output, tmp, by=c("i","j"),all.x=TRUE)
logg(paste(year, ':', 'merged EIA', sep = '\t'))
output <- na.omit(output)
logg(paste(year, ':', 'nona_EIA', sep = '\t'))
logg(paste(year, '#', nrow(output), sep = '\t'))
output$p_X <- output$v_X / output$q_X
tmp <- aggregate(output$p_X,list(output$k),median, na.rm=TRUE) ; colnames(tmp) <- c("k","uvmdn")
output <- merge(x=output,y=tmp,by=c("k"),all.x=TRUE)
output$ln_distw <- log(output$distw)
output$ln_distw_squared <- output$ln_distw * output$ln_distw
output$ln_uvmdn <- log(output$uvmdn)
output$d_hs_diff <- as.integer(output$hs_ptn!=output$hs_rpt)
logg(paste(year, ':', 'added model inputs', sep = '\t'))
obj_nm <- paste('tmp/', sub('M_matched', 'input', obj_nm, fixed = T), sep = '')
ecycle(write.csv(subset(output, TRUE, cols_out), file = bzfile(obj_nm),row.names=FALSE,na=""), 
             ecycle(s3write_using(subset(output, TRUE, cols_out), FUN = function(x, y)write.csv(x, file=bzfile(y), row.names = FALSE),
                                  bucket = out_bucket, object = basename(obj_nm)),
                     logg(paste(year, '!', paste('uploading', basename(obj_nm), 'failed', sep = ' '), sep = '\t')), max_try), 
             max_try,
             ecycle(put_object(obj_nm, basename(obj_nm), bucket = out_bucket), 
                    logg(paste(year, '!', paste('uploading', basename(obj_nm), 'failed', sep = ' '), sep = '\t')),
                    max_try,
                    {logg(paste(year, '|', paste('uploaded', basename(obj_nm), sep = ' '), sep = '\t')); unlink(obj_nm)}))
}
unlink('tmp/tmp.csv.bz2')
put_object(oplog, basename(oplog), bucket = in_bucket)
rm(list=ls())
