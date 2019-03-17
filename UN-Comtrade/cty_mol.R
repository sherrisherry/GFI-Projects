rm(list=ls()) # clean up environment
pkgs <- c('aws.s3', 'aws.ec2metadata', 'scripting', 'data.table')
for(i in pkgs)library(i, character.only = T)

#=====================================modify the following parameters for each new run==============================================#

usr <- 'aws00' # the user account for using AWS service
years <- 2016:2001 # the years we want to download
out_bucket <- 'gfi-work' # save the results to a S3 bucket called 'gfi-mirror-analysis'
in_bucket <- 'gfi-mirror-analysis' # read in raw data from this bucket
sup_bucket <- 'gfi-supplemental' # supplemental files
oplog <- 'cty_mol.log' # progress report file
max_try <- 10 # the maximum number of attempts for a failed process
keycache <- read.csv('~/vars/accesscodes.csv', header = TRUE, stringsAsFactors = FALSE) # the database of our credentials
tag <- 'Comtrade'
cty <- NULL # set to NULL to select all countries within GFI's consideration; or exp. c(231, 404, 800)
in_nm <- c('input','M_matched','M_orphaned','M_lost'); names(in_nm) <- in_nm
cols_in1 <- c(rep("integer",3),rep("character",3),rep("numeric",8),rep("integer",5), "numeric", rep("integer",5))
names(cols_in1) <- c("t","j","i","hs_rpt","hs_ptn","k","v_rX","v_rM","v_M","v_X","q_M","q_X","q_kg_M","q_kg_X","q_code_M","q_code_X","d_fob","d_dev_i","d_dev_j",
                    "distw","d_landlocked_j","d_landlocked_i","d_contig","d_conti","d_rta")
cols_in <- c(rep("integer",2),rep("character",3),rep("numeric",8),rep("integer",2))
names(cols_in) <- c("j","i","hs_rpt","hs_ptn","k","v_rX","v_rM","v_M","v_X","q_M","q_X","q_kg_M","q_kg_X","q_code_M","q_code_X")
#===================================================================================================================================#
oplog <- paste('logs/', oplog, sep = '')
logg <- function(x)mklog(x, path = oplog)
Sys.setenv("AWS_ACCESS_KEY_ID" = keycache$Access_key_ID[keycache$service==usr],
           "AWS_SECRET_ACCESS_KEY" = keycache$Secret_access_key[keycache$service==usr])
if(is.na(Sys.getenv()["AWS_DEFAULT_REGION"]))Sys.setenv("AWS_DEFAULT_REGION" = gsub('.{1}$', '', metadata$availability_zone()))
options(stringsAsFactors= FALSE)
cat('Time\tZone\tYear\tMark\tStatus\n', file = oplog, append = FALSE)

if(is.null(cty))cty <- gfi_cty(logg)

for(i in 1:length(in_nm)){
	output <- list()
	for(j in 1:length(years)){
		year <- years[j]
		in_nm[] <- paste(names(in_nm),'.csv.bz2', sep =''); in_nm[] <- paste(tag,year,in_nm,sep="-")
    cat("\n","...reading files for ",as.character(year)," ... ")
		ecycle(save_object(object = in_nm[i], bucket = in_bucket, file = 'tmp/tmp.csv.bz2', overwrite = TRUE),
				{logg(paste(year, '!', 'retrieving file failed', sep = '\t')); next}, max_try)
		ecycle(tmp <- fread(cmd="bzip2 -dkc ./tmp/tmp.csv.bz2", header=T, colClasses = if(i == 1)cols_in1 else cols_in),
				{logg(paste(year, '!', 'loading file failed', sep = '\t')); next}, max_try,
				cond = is.data.table(tmp))
		logg(paste(year, ':', paste('opened', in_nm[i]), sep = '\t'))
		unlink('tmp/tmp.csv.bz2')
    
    cat("... processing")
		tmp <- subset(tmp, tmp$i %in% cty | tmp$j %in% cty)
		tmp$t <- year
		output[[j]] <- tmp
		logg(paste(year, ':', paste('processed', in_nm[i]), sep = '\t'))
    }# end loop by date
	output <- do.call(rbind, output)
	tmp <- paste('tmp/', paste(tag, names(in_nm)[i], sep = '-'), '.csv.bz2', sep = '')
	ecycle(write.csv(output, file = bzfile(tmp),row.names=FALSE,na=""), 
	       ecycle(s3write_using(output, FUN = function(x, y)write.csv(x, file=bzfile(y), row.names = FALSE),
	                            bucket = out_bucket, object = basename(tmp)),
	              logg(paste(year, '!', paste('uploading', basename(tmp), 'failed', sep = ' '), sep = '\t')), max_try), 
	       max_try,
	       ecycle(put_object(tmp, basename(tmp), bucket = out_bucket), 
	              logg(paste(year, '!', paste('uploading', basename(tmp), 'failed', sep = ' '), sep = '\t')),
	              max_try,
	              {logg(paste(year, '|', paste('uploaded', basename(tmp), sep = ' '), sep = '\t')); unlink(tmp)}))
}
rm(output)
put_object(oplog, basename(oplog), bucket = out_bucket)
rm(list=ls())
