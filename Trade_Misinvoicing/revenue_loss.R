# 'mu' = import underinvoicing; 'mo' = import overinvoicing; 'mn' = no gap
# 'xu' = export underinvoicing; 'xo' = export overinvoicing; 'xn' = no gap

rm(list=ls()) # clean up environment
pkgs <- c('stats', 'data.table')
for(i in pkgs){if(!require(i, character.only = T))install.packages(i); library(i, character.only = T)}
if(!require(remotes))install.packages('remotes')
remotes::install_github("sherrisherry/GFI-Projects", subdir="pkg"); library(pkg)

#=====================================modify the following parameters for each new run==============================================#

usr <- 'aws00' # the user account for using AWS service
oplog <- 'trf.log' # progress report file
max_try <- 10 # the maximum number of attempts for a failed process
infile <- '/efs/work/tm_k6TRUE.csv.bz2'
trfile <- '/efs/work/trf.csv'
keycache <- read.csv('~/vars/accesscodes.csv', header = TRUE, stringsAsFactors = FALSE) # the database of our credentials
cols_in <- c(rep('integer', 3),rep('character',2), rep('numeric',3))
names(cols_in) <- c('t','j','i','k','f','v_j','v_i','gap_wtd')
cols_trf <- c(rep('integer', 3), rep('character',2), 'numeric')
names(cols_trf) <- c('t', 'wb_code_i', 'wb_code_j', 'k', 'source', 'trf_wtd')
# k_digit <- 2 # the number of digits of HS codes to be aggregated to
# k_len <- 6

#===================================================================================================================================#

oplog <- file.path('logs', oplog)
logg <- function(x)mklog(x, path = oplog)
ec2env(keycache, usr)
options(stringsAsFactors= FALSE)
cat('Time\tZone\tYear\tMark\tStatus\n', file = oplog, append = FALSE)
options(stringsAsFactors= FALSE)
# agg_lv <- paste('k', k_digit, sep = '')
# k_digit <- k_len - k_digit

bridge <- in_bridge(c('un_code','wb_code'), rep('integer',2), logg, max_try)
if(is.null(bridge))stop()
bridge <- unique(bridge)

tm <- read.csv(bzfile(infile), colClasses = cols_in)
trf <- read.csv(trfile, colClasses = cols_trf)

cty <- unique(tm$i)

tm <- split(tm, tm$f=='mu')
tm[['TRUE']] <- merge(x= tm[['TRUE']], y= bridge, by.x='j', by.y = 'un_code', all.x=T)
colnames(tm[['TRUE']])[match('wb_code',colnames(tm[['TRUE']]))]<-'wb_code_j'
if(length(cty>1)){
  tm[['TRUE']] <- merge(x= tm[['TRUE']], y= bridge, by.x='i', by.y = 'un_code', all.x=T)
  colnames(tm[['TRUE']])[match('wb_code',colnames(tm[['TRUE']]))]<-'wb_code_i'
  tm[['TRUE']] <- merge(x= tm[['TRUE']], y= trf, by=c('t', 'wb_code_i','wb_code_j', 'k'), all.x=T)
}else{
  tm[['TRUE']] <- merge(x= tm[['TRUE']], y= trf, by=c('t', 'wb_code_j', 'k'), all.x=T)
}
tm[['TRUE']][, c('wb_code_i','wb_code_j')] <- NULL
tm[['FALSE']][, c('source','trf_wtd')] <- NA
tm <- do.call(rbind, tm)

outfile <- paste('data/rl', paste(cty, collapse = '-'), '.csv', sep = '')
write.csv(tm, file= outfile,row.names = F)
rm(list=ls())
