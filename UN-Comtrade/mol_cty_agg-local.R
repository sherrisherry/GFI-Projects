rm(list=ls()) # clean up environment
pkgs <- c('aws.s3', 'aws.ec2metadata', 'batchscr', 'remotes', 'data.table')
for(i in pkgs)library(i, character.only = T)
install_github("sherrisherry/DC-Trees-App", subdir="pkg"); library(pkg)

#=====================================modify the following parameters for each new run==============================================#

usr <- 'aws00' # the user account for using AWS service
years <- 2016:2015 # the years we want to download
in_bucket <- 'gfi-mirror-analysis' # read in raw data from this bucket
oplog <- 'mol_cty_agg.log' # progress report file
max_try <- 10 # the maximum number of attempts for a failed process
keycache <- read.csv('~/vars/accesscodes.csv', header = TRUE, stringsAsFactors = FALSE) # the database of our credentials
tag <- 'Comtrade'
k_digit <- 2 # the number of digits of HS codes to be aggregated to
cty <- c(699, 360, 818) # set to NULL to select all countries within GFI's consideration; or exp. c(231, 404, 800)
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
	output <- list()
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
		tmp <- list()
		tmp$M <- subset(input, input$i %in% cty, c('j',"i","k","v_j",'v_i'))
		tmp$X <- subset(input, input$j %in% cty, c("j","i","k","v_j",'v_i'))
		logg(paste(year, ':', 'divided mx', sep = '\t'))
		rm(input)
		colnames(tmp$X)[match(c('i','j','v_i','v_j'), colnames(tmp$X))] <- c('j','i','v_j','v_i')
		agg <- c('v_i','v_j'); agg <- agg[!is.na(subset(tmp, 1, agg))]
		if(!all_trade)tmp <- subset(tmp, tmp$j %in% gfi_cty('adv', logg))
		if(k_digit>0){
		  if(k_digit < k_len){
		    tmp$k <- gsub(paste('.{',k_digit,'}$', sep = ''), '', tmp$k)
		    partition <- append(partition, 'k')
		  }
		  setkeyv(tmp, partition)
		  tmp <- lapply(output, function(x)aggregate(subset(x, select = agg), 
		                                             as.list(subset(x, select = partition)), sum, na.rm=T))
		  logg(paste(year, ':', 'aggregated k', sep = '\t'))
		}
		tmp$M$mx <- 'm'; tmp$X$mx <- 'x'; tmp <- do.call(rbind, tmp); tmp$t <- year
		output[[j]] <- tmp
		logg(paste(year, ':', paste('processed', in_nm[i]), sep = '\t'))
    }# end loop by date
	output <- do.call(rbind, output)
	tmp <- paste('data/', paste(tag, names(in_nm)[i], all_trade, sep = '-'), '.csv.bz2', sep = '')
	ecycle(write.csv(output, file = bzfile(tmp),row.names=FALSE,na=""), 
	       {logg(paste('0000', '!', paste('saving', basename(tmp), 'failed', sep = ' '), sep = '\t')); next}, 
	       max_try,
	       logg(paste('0000', '|', paste('saved', basename(tmp), sep = ' '), sep = '\t')))
}
rm(list=ls())
