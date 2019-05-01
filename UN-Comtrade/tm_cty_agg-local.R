# gaps already chosen cty; a few cty
# 'p' = positive; 'n' = negative; 'x' = zero
# 'mu' = import underinvoicing; 'mo' = import overinvoicing; 'ng' = no gap
# 'eu' = export underinvoicing; 'eo' = export overinvoicing

rm(list=ls()) # clean up environment
pkgs <- c('stats', 'batchscr', 'data.table')
for(i in pkgs)library(i, character.only = T)

#=====================================modify the following parameters for each new run==============================================#

years <- 2016:2001 # the years we want to download
in_dir <- '/efs/work' # read in raw data from this bucket
tag <- "Comtrade"
oplog <- 'tm_cty.log' # progress report file
max_try <- 10 # the maximum number of attempts for a failed process
all_trade <- TRUE
cty <- c(699, 360, 818) # no NULL to use all countries within GFI's consideration
k_digit <- 2 # the number of digits of HS codes to be aggregated to
cols_in <- c(rep('integer', 5),'character', rep('numeric',3), rep('NULL',5))
names(cols_in) <- c("t","j","i","d_dev_i","d_dev_j","k","v_X",'v_M_fob','gap_wtd',
                    "q_code_M","q_M","q_X","v_M", 'a_wt')
k_len <- 6

#===================================================================================================================================#

if(is.null(cty))stop('no NULL option')
oplog <- paste('logs/', oplog, sep = '')
logg <- function(x)mklog(x, path = oplog)
options(stringsAsFactors= FALSE)
cat('Time\tZone\tYear\tMark\tStatus\n', file = oplog, append = FALSE)
options(stringsAsFactors= FALSE)
agg_lv <- paste('k', k_digit, sep = '')
k_digit <- k_len - k_digit

output <- list()
for(year in years){
  in_file <- file.path(in_dir, paste(tag, year,"gaps.csv.bz2",sep="-"))
  ecycle(input <- fread(cmd = paste("bzip2 -dkc", in_file), colClasses=cols_in, na.strings="", header = T),
         {logg(paste(year, '!', 'loading file failed', sep = '\t')); stop()}, max_try)
  logg(paste(year, ':', 'loaded data', sep = '\t'))
  setkeyv(input, c('i', 'j'))
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
    tmp$'m'$f <- c(p='mo', n='mu', x='ng')[tmp$'m'$f]; tmp$'m'$mx <- 'm'
    tmp$'x'$f <- c(p='xu', n='xo', x='ng')[tmp$'x'$f]; tmp$'x'$mx <- 'x'
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
    tmp$mx <- ifelse(tmp$mx==1, 'm', 'x')
    tmp <- split(tmp, tmp$mx)
    logg(paste(year, ':', 'divided mx', sep = '\t'))
    tmp$m$f <- c(p='mo', n='mu', x='ng')[tmp$m$f]
    tmp$x$f <- c(p='xu', n='xo', x='ng')[tmp$x$f]
  }
  colnames(tmp$'x')[match(c('i','j','v_i','v_j'), colnames(tmp$'x'))] <- c('j','i','v_j','v_i')
  tmp <- do.call(rbind, tmp)
  tmp$t <- year
  output[[as.character(year)]] <- tmp
  logg(paste(year, '|', 'processed flows', sep = '\t'))
}
output <- do.call(rbind, output)

outfile <- paste('data/', 'flow_', agg_lv, '_', paste(cty, collapse = '-'), all_trade, min(years), max(years), '.csv', sep = '')
write.csv(output, file= outfile, row.names = F)
logg(paste('0000', '|', 'saved flows', sep = '\t'))
rm(list=ls())
