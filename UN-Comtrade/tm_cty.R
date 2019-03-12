# 'p' = positive; 'n' = negative; 'x' = zero
# 'mu' = import underinvoicing; 'mo' = import overinvoicing; 'ng' = no gap
# 'eu' = export underinvoicing; 'eo' = export overinvoicing

rm(list=ls()) # clean up environment
pkgs <- c('aws.s3', 'aws.ec2metadata', 'stats', 'scripting', 'data.table')
for(i in pkgs)library(i, character.only = T)

#=====================================modify the following parameters for each new run==============================================#

usr <- 'aws00' # the user account for using AWS service
years <- 2016:2001 # the years we want to download
out_bucket <- 'gfi-results' # save the results to a S3 bucket called 'gfi-mirror-analysis'
in_bucket <- 'gfi-work' # read in raw data from this bucket
sup_bucket <- 'gfi-supplemental' # supplemental files
tag <- "Comtrade"
oplog <- 'tm_cty.log' # progress report file
max_try <- 10 # the maximum number of attempts for a failed process
keycache <- read.csv('~/vars/accesscodes.csv', header = TRUE, stringsAsFactors = FALSE) # the database of our credentials
all_trade <- TRUE
cty <- 170
cols_in <- c(rep('integer', 6),'character', rep('numeric',7))
names(cols_in) <- c("t","j","i","q_code_M","d_dev_i","d_dev_j","k",
                    "v_M","v_X","q_M","q_X",'v_M_fob', 'a_wt', 'gap_wtd')

#===================================================================================================================================#

oplog <- paste('logs/', oplog, sep = '')
logg <- function(x)mklog(x, path = oplog)
Sys.setenv("AWS_ACCESS_KEY_ID" = keycache$Access_key_ID[keycache$service==usr],
           "AWS_SECRET_ACCESS_KEY" = keycache$Secret_access_key[keycache$service==usr])
if(is.na(Sys.getenv()["AWS_DEFAULT_REGION"]))Sys.setenv("AWS_DEFAULT_REGION" = gsub('.{1}$', '', metadata$availability_zone()))
options(stringsAsFactors= FALSE)
cat('Time\tZone\tYear\tMark\tStatus\n', file = oplog, append = FALSE)

options(stringsAsFactors= FALSE)

output <- list()
for(year in years){
  ecycle(save_object(object = paste(tag, year,"gaps.csv.bz2",sep="-"), bucket = in_bucket, file = 'tmp/tmp.csv.bz2', overwrite = TRUE),
         {logg(paste(year, '!', 'retrieving file failed', sep = '\t')); stop()}, max_try)
  ecycle(input <- fread(cmd = "bzip2 -dkc ./tmp/tmp.csv.bz2", colClasses=cols_in, na.strings="", header = T),
         {logg(paste(year, '!', 'loading file failed', sep = '\t')); stop()}, max_try)
  logg(paste(year, ':', 'loaded data', sep = '\t'))
  if(!is.null(cty)){
    input <- subset(input, input$i %in% cty | input$j %in% cty)
    logg(paste(year, ':', 'extracted cty', sep = '\t'))
    }
  input$f <- ifelse(input$gap_wtd > 0, 'p', 'n'); input$f[input$gap_wtd==0] <- 'x'
  input$k <- gsub('.{4}$', '', input$k)
  setkeyv(input, c('i', 'j', 'k', 'f'))
  if(all_trade){
    tmp <- list()
    tmp$M <- subset(input, input$i %in% cty); tmp$X <- subset(input, input$j %in% cty)
    logg(paste(year, ':', 'divided mx', sep = '\t'))
    rm(input)
    tmp <- lapply(tmp, function(x)aggregate(x[,c('v_M_fob','v_X','gap_wtd')],
                                            list(x$i,x$j,x$k,x$f),sum, na.rm=TRUE))
    logg(paste(year, ':', 'aggregated k', sep = '\t'))
    colnames(tmp$M) <- c('i','j','k2','f','v_i','v_j','gap_wtd'); tmp$M$mx <- 'm'
    tmp$M$f <- c(p='mo', n='mu', x='ng')[tmp$M$f]
    colnames(tmp$X) <- c('j','i','k2','f','v_j','v_i','gap_wtd'); tmp$X$mx <- 'x'
    tmp$X$f <- c(p='xu', n='xo', x='ng')[tmp$X$f]
  }else{
    tmp <- tmp[tmp$d_dev_i+tmp$d_dev_j<2 & tmp$d_dev_i+tmp$d_dev_j>0, ]
    tmp <- aggregate(tmp[,c('v_M_fob','v_X','gap_wtd')],
                      list(tmp$i,tmp$j,tmp$k,tmp$d_dev_i,tmp$f),sum, na.rm=TRUE)
    logg(paste(year, ':', 'aggregated k', sep = '\t'))
    colnames(tmp) <- c('i','j','k2','mx','f','v_i','v_j','gap_wtd')
    tmp$mx <- ifelse(tmp$mx==1, 'm', 'x')
    tmp <- split(tmp, tmp$mx)
    tmp$M$f <- c(p='mo', n='mu', x='ng')[tmp$M$f]
    tmp$X$f <- c(p='xu', n='xo', x='ng')[tmp$X$f]
    colnames(tmp$X)[match(c('i','j','v_i','v_j'), colnames(tmp$X))] <- c('j','i','v_j','v_i')
  }
  tmp <- do.call(rbind, tmp)
  tmp$t <- year
  output[[year]] <- tmp
  logg(paste(year, '|', 'processed flows', sep = '\t'))
}
output <- do.call(rbind, output)

outfile <- paste('data/', 'flow_agged_k', paste(cty, collapse = '-'), all_trade, min(years), max(years), '.csv', sep = '')
write.csv(output, file= out_file,row.names = F)
