rm(list=ls()) # clean up environment
pkgs <- c('aws.s3', 'batchscr', 'cleandata', 'data.table', 'remotes')
for(i in pkgs)library(i, character.only = T)
install_github("sherrisherry/GFI-Cloud", subdir="pkg"); library(pkg)

#=====================================modify the following parameters for each new run==============================================#

usr <- 'aws00' # the user account for using AWS service
years <- 2016:2001 # the years we want to download
yrs_model <- 2016:2001 # the years for cifob_model
in_dir <- '/efs/unct'
out_dir <- '/efs/work' # save the results to this S3 bucket
in_bucket <- 'gfi-work' # read in raw data from this bucket
max_try <- 10 # the maximum number of attempts for a failed process
tag <- 'Comtrade'
keycache <- read.csv('~/vars/accesscodes.csv', header = TRUE, stringsAsFactors = FALSE) # the database of our credentials
cty <- c(231, 404, 800) # set to NULL to select all countries within GFI's consideration; or exp. c(231, 404, 800)
all_trade <- TRUE
k_digit <- 2 # the number of digits of HS codes to be aggregated to
cifob_model <- 'cifob_model.rds.bz2'
oplog <- 'gap_tm.log' # progress report file
cols_in <- c('character', rep("integer",3),rep("numeric",7),rep("integer",11)) 
names(cols_in) <- c('k','t',"i","j","v_M","v_X","q_M","q_X", 'ln_distw', 'ln_distw_squared', 'ln_uvmdn', "q_code_M","q_code_X",
                    'd_fob', 'd_contig', 'd_conti', 'd_rta', 'd_landlocked_i', 'd_landlocked_j', 'd_dev_i', 'd_dev_j', 'd_hs_diff')
cols_out <- c("t","j","i","k","v_M","v_X","q_M","q_X","q_code_M","d_dev_i","d_dev_j", 'v_M_fob', 'a_wt', 'gap_wtd')
cols_model <- c('ln_distw', 'ln_distw_squared', 'ln_uvmdn', 'd_contig', 'd_conti', 'd_rta', 'd_landlocked_i', 'd_landlocked_j', 'd_dev_i', 'd_dev_j', 'd_hs_diff')
cols_model <- append(cols_model, paste('d', yrs_model[-1], sep = '_'))
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

ecycle(save_object(object = cifob_model, bucket = in_bucket, file = 'tmp/tmp.csv.bz2', overwrite = TRUE),
       {logg(paste('0000','!', 'retrieving file failed', sep = '\t')); stop()}, max_try)
ecycle(cifob_model <- readRDS(pipe("bzip2 -dkc ./tmp/tmp.csv.bz2")),
       {logg(paste('0000','!', 'loading file failed', sep = '\t')); stop()}, max_try)
logg(paste('0000',':', 'loaded model', sep = '\t'))
unlink('tmp/tmp.csv.bz2')
if(is.null(cty))cty <- gfi_cty(logf = logg)
output <- list()
for(year in years){
  ecycle(tinv <- read.csv(bzfile(file.path(in_dir, paste(tag, year,"input.csv.bz2",sep="-"))), colClasses=cols_in, na.strings="", header = T),
         {logg(paste(year, '!', 'loading file failed', sep = '\t')); next}, max_try)
  logg(paste(year, ':', 'loaded data', sep = '\t'))
  unlink('tmp/tmp.csv.bz2')
  tinv <- subset(tinv, tinv$i %in% cty | tinv$j %in% cty) # subset cty
  logg(paste(year, '#', paste('sub_cty', nrow(tinv), sep = ':'), sep = '\t'))
  tinv$d <- factor(tinv$t, levels = yrs_model); tmp <- encode_onehot(tinv[,'d', drop=FALSE], drop1st = T)
  tinv$d <- NULL; tinv <- cbind(tinv, tmp)
  logg(paste(year, ':', 'prepared data', sep = '\t'))
  tinv$v_M_fob <- 0 
  tinv[tinv$d_fob == 1,'v_M_fob'] <- tinv[tinv$d_fob == 1,'v_M']
  tinv[tinv$d_fob == 0, 'v_M_fob'] <- predict(cifob_model, tinv[tinv$d_fob == 0, cols_model],type='response')
  logg(paste(year, ':', 'applied model', sep = '\t'))
  tinv[tinv$d_fob == 0, 'v_M_fob'] <- exp(tinv[tinv$d_fob == 0, 'v_M_fob'])
  tinv[(tinv$d_fob == 0),"v_M_fob"] <- tinv[(tinv$d_fob == 0),"v_M"] / tinv[tinv$d_fob == 0, 'v_M_fob']
  tinv[(tinv$v_M_fob>tinv$v_M),"v_M_fob"] <- tinv[(tinv$v_M_fob>tinv$v_M),"v_M"]# DEFAULT: when fob>cif set FOB = CIF
  # calculate weights & gaps
  tinv$a_wt <- 1
  tinv[(tinv$q_M < tinv$q_X),"a_wt"] <- 1. - ((tinv[(tinv$q_M < tinv$q_X),"q_X"]- tinv[(tinv$q_M < tinv$q_X),"q_M"])/tinv[(tinv$q_M < tinv$q_X),"q_X"])
  tinv[(tinv$q_M >= tinv$q_X),"a_wt"] <- 1. - ((tinv[(tinv$q_M >= tinv$q_X),"q_M"]- tinv[(tinv$q_M >= tinv$q_X),"q_X"])/tinv[(tinv$q_M >= tinv$q_X),"q_M"])
  tinv$gap <- tinv$v_M_fob - tinv$v_X
  tinv$gap_wtd <- tinv$gap * tinv$a_wt
  logg(paste(year, ':', 'weighted gaps', sep = '\t'))
# start flow section
input <- data.table(tinv[, cols_out], key = c('i', 'j'))
  rm(tinv)
  input$f <- ifelse(input$gap_wtd > 0, 'p', 'n'); input$f[input$gap_wtd==0] <- 'x'
  input$gap_wtd <- abs(input$gap_wtd)
  if(all_trade){
    colnames(input)[match(c('v_M_fob','v_X'), colnames(input))] <- c('v_i','v_j')
    tmp <- list()
    tmp$'m' <- subset(input, input$i %in% cty, c("t","j","i","k","v_j",'v_i','f','gap_wtd'))
    tmp$'x' <- subset(input, input$j %in% cty, c("t","j","i","k","v_j",'v_i','f','gap_wtd'))
    logg(paste(year, ':', 'divided mx', sep = '\t'))
    rm(input)
    if(k_digit>0){
      if(k_digit < k_len){
        tmp$m$k <- gsub(paste('.{',k_digit,'}$', sep = ''), '', tmp$m$k)
		tmp$x$k <- gsub(paste('.{',k_digit,'}$', sep = ''), '', tmp$x$k)
        setkeyv(tmp$m, c('i', 'j', 'k', 'f')); setkeyv(tmp$x, c('i', 'j', 'k', 'f'))
        partition <- expression(list(i=x$i,j=x$j,k=x$k,f=x$f))
      }else{
        setkeyv(tmp$m, c('i', 'j', 'f')); setkeyv(tmp$x, c('i', 'j', 'f'))
        partition <- expression(list(i=x$i,j=x$j,f=x$f))
      }
      tmp <- lapply(tmp, function(x)aggregate(x[,c('v_i','v_j','gap_wtd')],
                                              eval(partition),sum, na.rm=TRUE))
      logg(paste(year, ':', 'aggregated k', sep = '\t'))
    }
  }else{
    colnames(input)[match(c('d_dev_i','v_M_fob','v_X'), colnames(input))] <- c('mx','v_i','v_j')
    tmp <- subset(input,input$d_dev_i+input$d_dev_j<2 & input$d_dev_i+input$d_dev_j>0, c("t","j","i","mx","k","v_j",'v_i','f','gap_wtd'))
    rm(input)
    if(k_digit>0){
      if(k_digit < k_len){
        tmp$k <- gsub(paste('.{',k_digit,'}$', sep = ''), '', tmp$k)
        setkeyv(tmp, c('i', 'j', 'k', 'mx', 'f'))
        partition <- expression(list(i=tmp$i,j=tmp$j,k=tmp$k,mx=tmp$mx,f=tmp$f))
      }else{
        setkeyv(tmp, c('i', 'j','mx', 'f'))
        partition <- expression(list(i=tmp$i,j=tmp$j,mx=tmp$mx,f=tmp$f))
      }
      tmp <- aggregate(tmp[,c('v_i','v_j','gap_wtd')],
                       eval(partition),sum, na.rm=TRUE)
      logg(paste(year, ':', 'aggregated k', sep = '\t'))
    }
    tmp <- split(tmp, tmp$mx); names(tmp) <- ifelse(names(tmp)=='1', 'm', 'x')
    logg(paste(year, ':', 'divided mx', sep = '\t'))
    tmp$'m'$'mx' <- NULL; tmp$'x'$'mx' <- NULL
  }
    tmp$m$f <- c(p='mo', n='mu', x='mn')[tmp$m$f]
    tmp$x$f <- c(p='xu', n='xo', x='xn')[tmp$x$f]
  colnames(tmp$'x')[match(c('i','j','v_i','v_j'), colnames(tmp$'x'))] <- c('j','i','v_j','v_i')
  tmp <- do.call(rbind, tmp); tmp$t <- year
  output[[as.character(year)]] <- tmp
logg(paste(year, '|', 'processed flows', sep = '\t'))
}
output <- do.call(rbind, output)
file_out <- paste(out_dir, '/tm_', agg_lv, all_trade, '.csv.bz2', sep = '')
  ecycle(write.csv(output, file = bzfile(file_out),row.names=FALSE,na=""), 
         logg(paste(year, '!', paste('saving', basename(file_out), 'failed', sep = ' '), sep = '\t')), 
         max_try)
logg(paste('0000', ':', 'done', sep = '\t'))
