# use k2 and cty NULL for lots of cty
# use k6 for a few cty to add trf
# 'p' = positive; 'n' = negative; 'x' = zero
# 'mu' = import underinvoicing; 'mo' = import overinvoicing; 'ng' = no gap
# 'eu' = export underinvoicing; 'eo' = export overinvoicing

rm(list=ls()) # clean up environment
pkgs <- c('aws.s3', 'stats', 'batchscr', 'remotes', 'data.table')
for(i in pkgs)library(i, character.only = T)
install_github("sherrisherry/GFI-Cloud", subdir="pkg"); library(pkg)

#=====================================modify the following parameters for each new run==============================================#

usr <- 'aws00' # the user account for using AWS service
years <- 2016:2001 # the years we want to download
out_dir <- '/efs/work' # save the results to a S3 bucket called 'gfi-mirror-analysis'
in_bucket <- 'gfi-work' # read in raw data from this bucket
sup_bucket <- 'gfi-supplemental' # supplemental files
tag <- "Comtrade"
oplog <- 'tm_cty.log' # progress report file
max_try <- 10 # the maximum number of attempts for a failed process
keycache <- read.csv('~/vars/accesscodes.csv', header = TRUE, stringsAsFactors = FALSE) # the database of our credentials
all_trade <- TRUE
cty <- NULL # set to NULL to use all dev countries within GFI's consideration
k_digit <- 2 # the number of digits of HS codes to be aggregated to
cols_in <- c(rep('integer', 5),'character', rep('numeric',3), rep('NULL',5))
names(cols_in) <- c("t","j","i","d_dev_i","d_dev_j","k","v_X",'v_M_fob','gap_wtd',
                    "q_code_M","q_M","q_X","v_M", 'a_wt')
k_len <- 6

#===================================================================================================================================#

# if(is.null(cty) && all_trade)stop('all cty and all trade mutually exclusive') # for our methodology; mechanically feasible
oplog <- paste('logs/', oplog, sep = '')
logg <- function(x)mklog(x, path = oplog)
ec2env(keycache,usr)
options(stringsAsFactors= FALSE)
cat('Time\tZone\tYear\tMark\tStatus\n', file = oplog, append = FALSE)
options(stringsAsFactors= FALSE)
agg_lv <- paste('k', k_digit, sep = '')
k_digit <- k_len - k_digit
if(is.null(cty))cty <- gfi_cty('dev', logf = logg) # select cty for security
outputs <- list()
for(year in years){
  ecycle(save_object(object = paste(tag, year,"gaps.csv.bz2",sep="-"), bucket = in_bucket, file = 'tmp/tmp.csv.bz2', overwrite = TRUE),
         {logg(paste(year, '!', 'retrieving file failed', sep = '\t')); next}, max_try)
  ecycle(input <- fread(cmd = "bzip2 -dkc ./tmp/tmp.csv.bz2", colClasses=cols_in, na.strings="", header = T),
         {logg(paste(year, '!', 'loading file failed', sep = '\t')); next}, max_try)
  logg(paste(year, ':', 'loaded data', sep = '\t'))
    input <- subset(input, input$i %in% cty | input$j %in% cty)
    logg(paste(year, ':', 'extracted cty', sep = '\t'))
  setkeyv(input, c('i', 'j'))
  input$f <- ifelse(input$gap_wtd > 0, 'p', 'n'); input$f[input$gap_wtd==0] <- 'x'
  input$gap_wtd <- abs(input$gap_wtd)
  if(all_trade){
    colnames(input)[match(c('v_M_fob','v_X'), colnames(input))] <- c('v_i','v_j')
    output <- list()
    output$m <- subset(input, input$i %in% cty, c("t","j","i","k","v_j",'v_i','f','gap_wtd'))
    output$x <- subset(input, input$j %in% cty, c("t","j","i","k","v_j",'v_i','f','gap_wtd'))
    logg(paste(year, ':', 'divided mx', sep = '\t'))
    rm(input)
    if(k_digit>0){
      if(k_digit < k_len){
		output$m$k <- gsub(paste('.{',k_digit,'}$', sep = ''), '', output$m$k)
		output$x$k <- gsub(paste('.{',k_digit,'}$', sep = ''), '', output$x$k)
		partition <- c('i', 'j', 'k', 'f')
      }else{
        partition <- c('i', 'j', 'f')
      }
	  setkeyv(output$m, partition); setkeyv(output$x, partition)
      output <- lapply(output, function(x)aggregate(x[,c('v_i','v_j','gap_wtd')], as.list(subset(x, select = partition)),sum, na.rm=T))
      logg(paste(year, ':', 'aggregated k', sep = '\t'))
    }
    output$m$mx <- 'm'; output$x$mx <- 'x'
  }else{
    colnames(input)[match(c('d_dev_i','v_M_fob','v_X'), colnames(input))] <- c('mx','v_i','v_j')
    output <- subset(input,input$d_dev_i+input$d_dev_j<2 & input$d_dev_i+input$d_dev_j>0, c("t","j","i","mx","k","v_j",'v_i','f','gap_wtd'))
    rm(input)
    if(k_digit>0){
      if(k_digit < k_len){
        output$k <- gsub(paste('.{',k_digit,'}$', sep = ''), '', output$k)
        partition <- c('i', 'j', 'k', 'mx', 'f')
      }else{
        partition <- c('i', 'j','mx', 'f')
      }
	  setkeyv(output, partition)
      output <- aggregate(output[,c('v_i','v_j','gap_wtd')],
                       as.list(subset(output, select = partition)),sum, na.rm=T)
      logg(paste(year, ':', 'aggregated k', sep = '\t'))
    }
    output$mx <- ifelse(output$mx==1, 'm', 'x')
    output <- split(output, output$mx)
    logg(paste(year, ':', 'divided mx', sep = '\t'))
  }
  output$m$f <- c(p='mo', n='mu', x='ng')[output$m$f]
  output$x$f <- c(p='xu', n='xo', x='ng')[output$x$f]
  colnames(output$x)[match(c('i','j','v_i','v_j'), colnames(output$x))] <- c('j','i','v_j','v_i')
  output <- do.call(rbind, output)
  output$t <- year
  outputs[[as.character(year)]] <- output
  rm(output)
  logg(paste(year, '|', 'processed flows', sep = '\t'))
}
outputs <- do.call(rbind, outputs)
outfile <- paste('data/', 'f_', agg_lv, all_trade, '.csv.bz2', sep = '')
ecycle(write.csv(outputs, file = bzfile(outfile),row.names=FALSE,na=""), 
              logg(paste('0000', '!', paste('saving', basename(outfile), 'failed', sep = ' '), sep = '\t')),
       max_try,
              logg(paste('0000', '|', paste('saved', basename(outfile), sep = ' '), sep = '\t')))
rm(list=ls())
