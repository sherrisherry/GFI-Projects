rm(list=ls()) # clean up environment
pkgs <- c('aws.s3', 'aws.ec2metadata', 'batchscr', 'remotes', 'data.table')
for(i in pkgs)library(i, character.only = T)
install_github("sherrisherry/DC-Trees-App", subdir="pkg"); library(pkg)

#=====================================modify the following parameters for each new run==============================================#

usr <- 'aws00' # the user account for using AWS service
years <- 2016:2001 # the years we want to download
out_dir <- '/efs/work' # save the results to a S3 bucket called 'gfi-mirror-analysis'
in_bucket <- 'gfi-mirror-analysis' # read in raw data from this bucket
cty <- NULL # set to NULL to select all countries within GFI's consideration; or exp. c(231, 404, 800)
all_trade <- TRUE
oplog <- 'mol_cty.log' # progress report file
max_try <- 10 # the maximum number of attempts for a failed process
keycache <- read.csv('~/vars/accesscodes.csv', header = TRUE, stringsAsFactors = FALSE) # the database of our credentials
tag <- 'Comtrade'
k_digit <- 2 # the number of digits of HS codes to be aggregated to
in_nm <- c('M_matched','M_orphaned','M_lost'); names(in_nm) <- c('mm', 'mo', 'ml')
cols_in <- c(rep("integer",2),rep("character",3),rep("numeric",8),rep("integer",2))
names(cols_in) <- c("j","i","hs_rpt","hs_ptn","k","v_rX","v_rM","v_M","v_X","q_M","q_X","q_kg_M","q_kg_X","q_code_M","q_code_X")
k_len <- 6

#===================================================================================================================================#
if(!file.exists(out_dir))system(paste('sudo mkdir -m777', out_dir))
oplog <- file.path('logs', oplog)
logg <- function(x)mklog(x, path = oplog)
ec2env(keycache,usr)
options(stringsAsFactors= FALSE)
cat('Time\tZone\tYear\tMark\tStatus\n', file = oplog, append = FALSE)
agg_lv <- paste('k', k_digit, sep = '')
k_digit <- k_len - k_digit

if(is.null(cty))cty <- gfi_cty('dev', logg)
if(!all_trade)ptn <- gfi_cty('adv', logg)

for(i in 1:length(in_nm)){
    out_m <- list(); out_x <- list()
	for(j in 1:length(years)){
		year <- years[j]
		nm <- paste(in_nm[i],'.csv.bz2', sep =''); nm <- paste(tag,year,nm,sep="-")
		ecycle(save_object(object = nm, bucket = in_bucket, file = 'tmp/tmp.csv.bz2', overwrite = TRUE),
				{logg(paste(year, '!', 'retrieving file failed', sep = '\t')); next}, max_try)
		ecycle(input <- fread(cmd="bzip2 -dkc ./tmp/tmp.csv.bz2", header=T, colClasses = cols_in),
				{logg(paste(year, '!', 'loading file failed', sep = '\t')); next}, max_try,
				cond = is.data.table(input))
		logg(paste(year, ':', paste('opened', in_nm[i]), sep = '\t'))
		unlink('tmp/tmp.csv.bz2')
		colnames(input)[match(c('v_M','v_X'), colnames(input))] <- c('v_i','v_j')
		input <- subset(input, input$i %in% cty | input$j %in% cty)
		output <- list()
		output$M <- subset(input, if(all_trade)input$i %in% cty else input$i %in% cty & input$j %in% ptn, c('j',"i","k","v_j",'v_i'))
		output$X <- subset(input, if(all_trade)input$j %in% cty else input$j %in% cty & input$i %in% ptn, c("j","i","k","v_j",'v_i'))
		logg(paste(year, ':', 'divided mx', sep = '\t'))
		rm(input)
		colnames(output$X)[match(c('i','j','v_i','v_j'), colnames(output$X))] <- c('j','i','v_j','v_i')
		partition <- c('i', 'j')
		if(k_digit>0){
		  agg <- c('v_i','v_j')
		  if(k_digit < k_len){
		    output <- lapply(output, function(x){x$k <- gsub(paste('.{',k_digit,'}$', sep = ''), '', x$k); return(x)})
		    partition <- append(partition, 'k')
		  }
		  output <- lapply(output, function(x)aggregate(subset(x, TRUE, select = agg[!is.na(x[1, agg, with = F])]), 
		                                                as.list(subset(x, TRUE, select = partition)), sum, na.rm=T))
		  logg(paste(year, ':', 'aggregated k', sep = '\t'))
		}
		output$M$t <- year; output$X$t <- year
		logg(paste(year, ':', paste('processed', in_nm[i]), sep = '\t'))
		out_m[[j]] <- output$M; out_x[[j]] <- output$X
		rm(output)
    }
  out_m <- do.call(rbind, out_m); out_x <- do.call(rbind, out_x)
  tmp <- paste(out_dir, '/', paste(names(in_nm)[i], '_m_', agg_lv, all_trade, sep = ''), '.csv.bz2', sep = '')
  ecycle(write.csv(out_m, file = bzfile(tmp), row.names=F, na=""), 
		       logg(paste('0000', '!', paste('saving', basename(tmp), 'failed', sep = ' '), sep = '\t')),
			   max_try,
			   logg(paste('0000', '|', paste('saved', basename(tmp), sep = ' '), sep = '\t')))
  tmp <- paste(out_dir, '/', paste(names(in_nm)[i], '_x_', agg_lv, all_trade, sep = ''), '.csv.bz2', sep = '')
  ecycle(write.csv(out_x, file = bzfile(tmp), row.names=F, na=""), 
         logg(paste('0000', '!', paste('saving', basename(tmp), 'failed', sep = ' '), sep = '\t')),
         max_try,
         logg(paste('0000', '|', paste('saved', basename(tmp), sep = ' '), sep = '\t')))
}
rm(list=ls())
