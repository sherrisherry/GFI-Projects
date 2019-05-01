rm(list=ls()) # clean up environment
pkgs <- c('aws.s3', 'aws.ec2metadata', 'batchscr', 'remotes', 'data.table')
for(i in pkgs)library(i, character.only = T)
install_github("sherrisherry/DC-Trees-App", subdir="pkg"); library(pkg)

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
k_digit <- 2 # the number of digits of HS codes to be aggregated to
cty <- NULL # set to NULL to select all countries within GFI's consideration; or exp. c(231, 404, 800)
all_trade <- TRUE
in_nm <- c('M_matched','M_orphaned','M_lost'); names(in_nm) <- in_nm
cols_in <- c(rep("integer",2),rep("character",3),rep("numeric",8),rep("integer",2))
names(cols_in) <- c("j","i","hs_rpt","hs_ptn","k","v_rX","v_rM","v_M","v_X","q_M","q_X","q_kg_M","q_kg_X","q_code_M","q_code_X")
k_len <- 6

#===================================================================================================================================#
oplog <- file.path('logs', oplog)
logg <- function(x)mklog(x, path = oplog)
ec2env(keycache,usr)
options(stringsAsFactors= FALSE)
cat('Time\tZone\tYear\tMark\tStatus\n', file = oplog, append = FALSE)
agg_lv <- paste('k', k_digit, sep = '')
k_digit <- k_len - k_digit

if(is.null(cty))cty <- gfi_cty('dev', logg)

for(i in 1:length(in_nm)){
	for(j in 1:length(years)){
		year <- years[j]
		in_nm[] <- paste(names(in_nm),'.csv.bz2', sep =''); in_nm[] <- paste(tag,year,in_nm,sep="-")
		ecycle(save_object(object = in_nm[i], bucket = in_bucket, file = 'tmp/tmp.csv.bz2', overwrite = TRUE),
				{logg(paste(year, '!', 'retrieving file failed', sep = '\t')); next}, max_try)
		ecycle(input <- fread(cmd="bzip2 -dkc ./tmp/tmp.csv.bz2", header=T, colClasses = cols_in),
				{logg(paste(year, '!', 'loading file failed', sep = '\t')); next}, max_try,
				cond = is.data.table(input))
		logg(paste(year, ':', paste('opened', in_nm[i]), sep = '\t'))
		unlink('tmp/tmp.csv.bz2')
		partition <- c('i', 'j')
		colnames(input)[match(c('v_M','v_X'), colnames(input))] <- c('v_i','v_j')
		input <- subset(input, input$i %in% cty | input$j %in% cty)
		output <- list()
		output$M <- subset(input, input$i %in% cty, c('j',"i","k","v_j",'v_i'))
		output$X <- subset(input, input$j %in% cty, c("j","i","k","v_j",'v_i'))
		logg(paste(year, ':', 'divided mx', sep = '\t'))
		rm(input)
		colnames(output$X)[match(c('i','j','v_i','v_j'), colnames(output$X))] <- c('j','i','v_j','v_i')
		agg <- c('v_i','v_j'); agg <- agg[!is.na(subset(output, 1, agg))]
		if(!all_trade)output <- subset(output, output$j %in% gfi_cty('adv', logg))
		if(k_digit>0){
		  if(k_digit < k_len){
		    output$k <- gsub(paste('.{',k_digit,'}$', sep = ''), '', output$k)
		    partition <- append(partition, 'k')
		  }
		  setkeyv(output, partition)
		  output <- lapply(output, function(x)aggregate(subset(x, select = agg), 
		                                                as.list(subset(x, select = partition)), sum, na.rm=T))
		  logg(paste(year, ':', 'aggregated k', sep = '\t'))
		}
		output$M$mx <- 'm'; output$X$mx <- 'x'; output <- do.call(rbind, output); output$t <- year
		logg(paste(year, ':', paste('processed', in_nm[i]), sep = '\t'))
		tmp <- paste('tmp/', paste(tag, year, names(in_nm)[i], all_trade, sep = '-'), '.csv.bz2', sep = '')
		ecycle(write.csv(output, file = bzfile(tmp), row.names=F, na=""), 
		       ecycle(s3write_using(output, FUN = function(x, y)write.csv(x, file=bzfile(y), row.names = FALSE),
		                            bucket = out_bucket, object = basename(tmp)),
		              logg(paste(year, '!', paste('uploading', basename(tmp), 'failed', sep = ' '), sep = '\t')), max_try), 
		       max_try,
		       ecycle(put_object(tmp, basename(tmp), bucket = out_bucket), 
		              logg(paste(year, '!', paste('uploading', basename(tmp), 'failed', sep = ' '), sep = '\t')),
		              max_try,
		              {logg(paste(year, '|', paste('uploaded', basename(tmp), sep = ' '), sep = '\t')); unlink(tmp)}))
    }# end loop by date
  rm(output)
}
put_object(oplog, basename(oplog), bucket = out_bucket)
rm(list=ls())
