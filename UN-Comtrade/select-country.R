rm(list=ls()) # clean up environment
pkgs <- c('aws.s3', 'aws.ec2metadata', 'scripting', 'data.table')
for(i in pkgs)library(i, character.only = T)

#=====================================modify the following parameters for each new run==============================================#

usr <- 'aws00' # the user account for using AWS service
years <- 2016:2001 # the years we want to download
# out_bucket <- 'gfi-mirror-analysis' # save the results to a S3 bucket called 'gfi-mirror-analysis'
in_bucket <- 'gfi-mirror-analysis' # read in raw data from this bucket
sup_bucket <- 'gfi-supplemental' # supplemental files
oplog <- 'select.log' # progress report file
max_try <- 10 # the maximum number of attempts for a failed process
keycache <- read.csv('~/vars/accesscodes.csv', header = TRUE, stringsAsFactors = FALSE) # the database of our credentials
tag <- 'Comtrade'
countries <- c(231, 404, 800)
in_nm <- c('M-matched-q','M-matched','M-orphaned','M-lost'); names(in_nm) <- in_nm
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

row_count <- data.frame(Year = years); row_count$raw <- NA; row_count$nona <- NA; row_count$inlim <- NA

output <- list()
# start loop by country
for(i in 1:length(in_nm)){
	x_in <- list()
	for(j in 1:length(years)){
		year <- years[j]
		in_nm[] <- paste(names(in_nm),'.csv.bz2', sep ='')
		in_nm[] <- paste(tag,year,in_nm,sep="-")
    cat("\n","...reading files for ",as.character(year)," ... ")
		ecycle(save_object(object = in_nm[i], bucket = in_bucket, file = 'tmp/tmp.csv.bz2', overwrite = TRUE),
				{logg(paste(year, '!', 'retrieving file failed', sep = '\t')); next}, max_try)
		ecycle(tmp <- fread(cmd="bzip2 -dkc ./tmp/tmp.csv.bz2", header=T, colClasses = if(i == 1)cols_in1 else cols_in),
				{logg(paste(year, '!', 'loading file failed', sep = '\t')); next}, max_try,
				cond = is.data.table(tmp))
		logg(paste(year, ':', paste('opened', in_nm[i]), sep = '\t'))
		unlink('tmp/tmp.csv.bz2')
    
    cat("... processing")
		tmp <- subset(tmp, tmp$i %in% countries | tmp$j %in% countries)
		tmp$t <- year
		x_in[[j]] <- tmp
		logg(paste(year, ':', paste('processed', in_nm[i]), sep = '\t'))
    }# end loop by date
	output[[names(in_nm)[i]]] <- do.call(rbind, x_in)
  }
  rm(x_in)
  tmp <- paste('data/', names(output), '.csv.bz2', sep = '')
for(i in 1:length(output)){
	write.csv(output[[i]], file = bzfile(tmp[i]),row.names=FALSE,na="")
    logg(paste('0000', '|', paste('saved', basename(tmp[i]), sep = ' '), sep = '\t'))
	}
rm(list=ls())
