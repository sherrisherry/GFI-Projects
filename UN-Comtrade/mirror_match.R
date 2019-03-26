# 1. decompress and subset the raw data to only keep aggregation level 6 and remove country names and commodity names;
# 3. compress the results and send them to AWS S3;
# 4. record the information of downloaded data;
# 5. send a progress report to Google Drive;
# 6. shutdown system after completion.

rm(list=ls()) # clean up environment
pkgs <- c('aws.s3', 'aws.ec2metadata', 'stats', 'jsonlite', 'scripting', 'remotes', 'data.table')
for(i in pkgs)library(i, character.only = T)
install_github("sherrisherry/GFI-Cloud", subdir="pkg"); library(pkg)

#=====================================modify the following parameters for each new run==============================================#

usr <- 'aws00' # the user account for using AWS service
dates <- 2005:2001 # the years we want to download
hk_years <- 2016:2000 # years HK re-export data is available
out_bucket <- 'gfi-mirror-analysis' # save the results to a S3 bucket called 'gfi-mirror-analysis'
in_bucket <- 'gfi-comtrade' # read in raw data from this bucket
sup_bucket <- 'gfi-supplemental' # supplemental files
tag <- "Comtrade"
oplog <- 'mirror_match.log' # progress report file
dinfo <- 'bulk_download.log' # file of the information of the downloaded data
opcounter <- 'process_mirror_match.json'
max_try <- 10 # the maximum number of attempts for a failed process
keycache <- read.csv('~/vars/accesscodes.csv', header = TRUE, stringsAsFactors = FALSE) # the database of our credentials
cols_UN <- rep('NULL', 15)
names(cols_UN) <- c("classification","year","period","perioddesc","aggregatelevel","isleafcode","tradeflowcode",
                  "reportercode","partnercode", "commoditycode","qtyunitcode","qty","netweightkg","tradevalues","flag")
incol <- c("tradeflowcode","classification","reportercode","partnercode","commoditycode", "tradevalues", "qtyunitcode", "qty", "netweightkg")
names(incol) <- c("tf","hs","i","j","k","v","q_code","q","q_kg")
cols_UN[incol] <- c("integer","character","integer","integer","character","numeric","integer","numeric","numeric")
cols_hk <- c(rep("integer",3),"character","numeric",rep("integer",2),rep("numeric",2))
names(cols_hk) <- c("t","origin_hk","consig_hk","k","vrx_hkd","origin_un","consig_un","usd_per_hkd","vrx_usd")
cols_swiss <- c("character","integer","character","integer",rep("numeric",2))
names(cols_swiss) <- c('mx','j','k','t','v','q_kg')
# use "character" for k or codes like '9999AA' messes up
tfn <- c('M','rM','X','rX')
names(tfn) <- c('1','4','2','3')

#===================================================================================================================================#
				  
oplog <- file.path('logs', oplog); dinfo <- file.path('logs', dinfo)
opcounter <- paste('data/', opcounter, sep = '')
logg <- function(x)mklog(x, path = oplog)
ec2env(keycache,usr)
options(stringsAsFactors= FALSE)
cat('Time\tZone\tYear\tMark\tStatus\n', file = oplog, append = FALSE)
dinfo <- read.delim(dinfo); dyears <- unique(dinfo[dinfo$Status=='uploaded', 'Year'])
dates <- intersect(dates, dyears)
n_dates <- length(dates); stopifnot(length(n_dates)>0)
dinfo <- dinfo[dinfo$Mark == '#', c('Year', 'Status')]
rm(dyears)

# Set up counters to collect for multiple years

counter <- list()
counter$n_RAW <- data.frame(Year = dates, M = NA, X = NA, rX = NA, rM = NA)
counter$n_TREAT <-  counter$n_SWISS <- counter$n_RAW
out_names <- c('M_matched', 'X_matched', 'M_orphaned', 'M_lost')
names(out_names) <- c('M.FALSE', 'X.FALSE', 'M.TRUE', 'X.TRUE') # M_lost == X_orphan
tmp <- c('M_paired','X_paired', out_names)
counter$n_pair <- matrix(nrow = n_dates, ncol = 6, dimnames = list(dates, paste('n_', tmp, sep = '')))
counter$n_pair <- as.data.frame(counter$n_pair)

# Prepare for SWISS module
    # Data M_swiss and X_swiss compiled by Joe Spanjers from Swiss source data
      ecycle(swiss <- s3read_using(FUN = function(x)fread(x, header=T, na.strings="", colClasses = cols_swiss), 
                                     object = "Swiss_trade_m.csv", bucket = sup_bucket),
             {tmp <- paste('0000', '!', 'loading Swiss failed', sep = '\t'); logg(tmp); stop(tmp)},
             max_try)
      # # keep only records for commodity 710812 which comprises 98% of Swiss trade in non-monetary gold
      #     (NB, monetary gold trade flows should not be included in UN-Comtrade dataset for any country)
      swiss <- subset(swiss,swiss$k=='710812'); swiss <- split(swiss, swiss$mx)

# Loop by date
for (t in 1:n_dates) {
  year <- dates[t]
  cat("\n","\n","PROCESSING DATA FOR ",as.character(year))

# Start RAW module
    cat("\n","   (1) RAW module")
    cat("\n","        ...reading data file downloaded from UN-COMTRADE...","\n")
    ecycle(save_object(object = paste(year, '.csv.bz2', sep = ''), bucket = in_bucket, file = 'tmp/tmp.csv.bz2', overwrite = TRUE),
		   {logg(paste(year, '!', 'retrieving file failed', sep = '\t')); next}, max_try)
	ecycle(rdata <- fread(cmd = "bzip2 -dkc ./tmp/tmp.csv.bz2", header=T, colClasses = cols_UN),
		   {logg(paste(year, '!', 'loading file failed', sep = '\t')); next}, max_try,
		   cond = is.data.table(rdata))
	if(!setequal(colnames(rdata), incol)){
      logg(paste(year, '!', 'colnames mismatching', sep = '\t'))
      next
      }
	logg(paste(year, ':', 'opened', sep = '\t'))
	if(nrow(rdata) != dinfo$Status[dinfo$Year==year]){logg(paste(year, '#', nrow(rdata), sep = '\t'))}
	# avoid reordering data table because of speed
    tmp <- colnames(rdata)
    colnames(rdata)[match(incol, tmp)] <- names(incol)
	rdata <- na.omit(rdata, cols=c("hs","i","j","k","v"))
    setkeyv(rdata, c('i', 'j', 'k'))
	logg(paste(year, ':', 'preprocessed', sep = '\t'))
    cat("\n","        ...dividing raw data...")
	rdata<- split(rdata, rdata$tf)
	names(rdata) <- tfn[match(names(rdata),names(tfn))]
	rdata <- lapply(rdata, function(x){cols <- colnames(x); nm <- c("v","q_code","q","q_kg"); lsn <- tfn[as.character(x$tf[1])]
										colnames(x)[match(nm, cols)] <- paste(nm, lsn, sep = '_'); x$tf <- NULL
										return(x)})
    counter$n_RAW[t, tfn] <- as.data.frame(lapply(rdata, nrow))[tfn]
	logg(paste(year, ':', 'divided', sep = '\t'))
#   End RAW module

#   Implement TREAT module 
    cat("\n","   (2) TREAT module")
	rdata <- lapply(rdata, function(x)treat(x,t))
	counter$n_TREAT[t, tfn] <- as.data.frame(lapply(rdata, nrow))[tfn]
	logg(paste(year, ':', 'treated', sep = '\t'))

    #   End TREAT module

#   Implement SWISS module
    cat("\n","   (3) SWISS module")
    rdata$M  <- unct_swiss(rdata$M,swiss$m,t); rdata$X  <- unct_swiss(rdata$X,swiss$x,t)
   # counts
   counter$n_SWISS[t, tfn] <- as.data.frame(lapply(rdata, nrow))[tfn]
    logg(paste(year, ':', 'swiss-adjusted', sep = '\t'))
    # End SWISS module
   
#   Start PAIR module 
    cat("\n","   (4) PAIR module")
    
    # Pair M
    cat("\n","        ...pairing M data")
	tmp <- colnames(rdata$M); colnames(rdata$M)[match('hs', tmp)] <- c('hs_rpt')
	tmp <- colnames(rdata$X); colnames(rdata$X)[match(c('hs', 'i', 'j'), tmp)] <- c('hs_ptn', 'j', 'i')
	tmp <- colnames(rdata$rX); colnames(rdata$rX)[match(c('i', 'j'), tmp)] <- c('j', 'i')
	mirror <- list()
    cat("\n","            ...M to X") ;  mirror$M <- merge(x=rdata$M,y=rdata$X,by = c("i","j","k"), all.x=TRUE)
    cat("\n","            ...M to rX") ; mirror$M <- merge(x=mirror$M,y=rdata$rX[, c("i","j","k","v_rX")],by = c("i","j","k"), all.x=TRUE)
    cat("\n","            ...M to rM") ; mirror$M <- merge(x=mirror$M,y=rdata$rM[, c("i","j","k","v_rM")],by = c("i","j","k"), all.x=TRUE)

    logg(paste(year, ':', 'M-paired', sep = '\t'))
  
    # Pair X
    cat("\n","        ...pairing X data")
    tmp <- colnames(rdata$X); colnames(rdata$X)[match(c('hs_ptn', 'i', 'j'), tmp)] <- c('hs_rpt', 'j', 'i')
	tmp <- colnames(rdata$M); colnames(rdata$M)[match(c('hs_rpt', 'i', 'j'), tmp)] <- c('hs_ptn', 'j', 'i')
	tmp <- colnames(rdata$rM); colnames(rdata$rM)[match(c('i', 'j'), tmp)] <- c('j', 'i')
	tmp <- colnames(rdata$rX); colnames(rdata$rX)[match(c('i', 'j'), tmp)] <- c('j', 'i')
    cat("\n","            ...X to M") ;  mirror$X <- merge(x=rdata$X,y=rdata$M,by = c("i","j","k"), all.x=TRUE)
    cat("\n","            ...X to rX") ; mirror$X <- merge(x=mirror$X,y=rdata$rX[, c("i","j","k","v_rX")],by = c("i","j","k"), all.x=TRUE)
    cat("\n","            ...X to rM") ; mirror$X <- merge(x=mirror$X,y=rdata$rM[, c("i","j","k","v_rM")],by = c("i","j","k"), all.x=TRUE)

    rm(rdata)
    logg(paste(year, ':', 'X-paired', sep = '\t'))
    # end PAIR module

# Start Hong Kong module
    cat("\n","   (5) Hong Kong module","\n")
    # operate on X-paired and M-paired and then redo the matching etc
    if (year %in% hk_years) {
    # use normalized hkrx data
    ecycle(save_object(object = paste('HK_trade', year, 'rx.csv.bz2', sep = '_'), bucket = sup_bucket, file = 'tmp/tmp.csv.bz2', overwrite = TRUE),
		       {logg(paste(year, '!', 'retrieving hkrx file failed', sep = '\t')); next}, max_try) 
	  ecycle(hk <- fread(cmd = "bzip2 -dc ./tmp/tmp.csv.bz2", header=T, colClasses=cols_hk,na.strings=""),
		              {logg(paste(year, '!', 'loading hkrx file failed', sep = '\t')); next}, 
		              max_try, cond = is.data.table(hk) && nrow(hk)>10)
    hk <- hk[,c("origin_un","consig_un","k","vrx_usd")]
    colnames(hk) <- c("i","j","k","v_rx_hk")
        logg(paste(year, ':', 'HK prepared', sep = '\t'))
        # adjusting M variables
        tmp <- mirror$M
        colnames(hk) <- c("j","i","k","v_rx_hk")
        setkeyv(hk, c('i', 'j', 'k'))
        mirror$M <- merge(x=mirror$M,y=hk,by=c("i","j","k"),all.x=TRUE)
        hk_344 <- hk[,c("i","k","v_rx_hk")]
        colnames(hk_344) <- c("i","k","adj_344")
        hk_344 <- aggregate(hk_344$adj_344,list(hk_344$i,hk_344$k),sum) ; colnames(hk_344) <- c("i","k","adj_344")
        hk_344 <- data.table(hk_344, key = c('i','k'))
        mirror$M <- merge(x=mirror$M,y=hk_344,by=c("i","k"),all.x=TRUE)
        mirror$M[is.na(mirror$M$v_rx_hk),"v_rx_hk"] <- 0
        mirror$M[is.na(mirror$M$adj_344),"adj_344"] <- 0
        tmp <- mirror$M
        mirror$M[!mirror$M$j==344,"v_M"] <- mirror$M[!mirror$M$j==344,"v_M"] - mirror$M[!mirror$M$j==344,"v_rx_hk"]
        mirror$M[ mirror$M$j==344,"v_M"] <- mirror$M[ mirror$M$j==344,"v_M"] + mirror$M[ mirror$M$j==344,"adj_344"]
        mirror$M <- mirror$M[,c("hs_rpt","hs_ptn","i","j","k","v_M","v_X","v_rX","v_rM","q_M","q_X","q_kg_M","q_kg_X","q_code_M","q_code_X")]
        junk <- subset(mirror$M,mirror$M$v_M<0)
        mirror$M[mirror$M$v_M<0,"v_M"] <- tmp[mirror$M$v_M<0,"v_M"]  ;   # undo the adjustment for negative values
        mirror$M[mirror$M$i==752,"v_M"] <- tmp[mirror$M$i==752,"v_M"]  ; # undo the adjustment for Sweden  (OECD[2016], p. 19)
        mirror$M[mirror$M$i==348,"v_M"] <- tmp[mirror$M$i==348,"v_M"]  ; # undo the adjustment for Hungary (OECD[2016], p. 19)
        logg(paste(year, ':', 'HK M-adjusted', sep = '\t'))
        ecycle(s3write_using(junk, FUN = function(x, y)write.csv(x, file=bzfile(y), row.names = FALSE),
                               bucket = out_bucket,
                               object = paste(tag, year, 'M-junked-HK-adj.csv.bz2', sep = '-')),
                logg(paste(year, '.', 'junked M HK-adj not uploaded', sep = '\t')), 3)
        # adjusting X variables
        colnames(hk) <- c("i","j","k","v_rx_hk")
        setkeyv(hk, c('i', 'j', 'k'))
        mirror$X <- merge(x=mirror$X,y=hk,by=c("i","j","k"),all.x=TRUE)
        hk_344 <- hk[,c("j","k","v_rx_hk")]
        colnames(hk_344) <- c("j","k","adj_344")
        hk_344 <- aggregate(hk_344$adj_344,list(hk_344$j,hk_344$k),sum) ; colnames(hk_344) <- c("j","k","adj_344")
        hk_344 <- data.table(hk_344, key = c('j','k'))
        mirror$X <- merge(x=mirror$X,y=hk_344,by=c("j","k"),all.x=TRUE)
        mirror$X[is.na(mirror$X$v_rx_hk),"v_rx_hk"] <- 0
        mirror$X[is.na(mirror$X$adj_344),"adj_344"] <- 0
        tmp <- mirror$X
        mirror$X[!mirror$X$i==344,"v_M"] <- mirror$X[!mirror$X$i==344,"v_M"] - mirror$X[!mirror$X$i==344,"v_rx_hk"]
        mirror$X[ mirror$X$i==344,"v_M"] <- mirror$X[ mirror$X$i==344,"v_M"] + mirror$X[ mirror$X$i==344,"adj_344"]
        mirror$X <- mirror$X[,c("hs_rpt","hs_ptn","i","j","k","v_X","v_M","v_rM","v_rX","q_X","q_M","q_kg_X","q_kg_M","q_code_X","q_code_M")]
        junk <- subset(mirror$X,mirror$X$v_M<0)
        mirror$X[mirror$X$v_M<0,"v_M"] <- tmp[mirror$X$v_M<0,"v_M"]  ; # undo the adjustment for negative values
        mirror$X[mirror$X$j==752,"v_M"] <- tmp[mirror$X$j==752,"v_M"]  ; # undo the adjustment for Sweden  (OECD[2016], p. 19)
        mirror$X[mirror$X$j==348,"v_M"] <- tmp[mirror$X$j==348,"v_M"]  ; # undo the adjustment for Hungary (OECD[2016], p. 19)
        logg(paste(year, ':', 'HK X-adjusted', sep = '\t'))
        ecycle(s3write_using(junk, FUN = function(x, y)write.csv(x, file=bzfile(y), row.names = FALSE),
                               bucket = out_bucket,
                               object = paste(tag, year, 'X-junked-HK-adj.csv.bz2', sep = '-')),
                 logg(paste(year, '.', 'junked X HK-adj not uploaded', sep = '\t')), 3)
        remove(tmp,junk, hk, hk_344)
    }
    # end Hong Kong module

	counter$n_pair[t, c('n_M_paired', 'n_X_paired')] <- as.data.frame(lapply(mirror, nrow))[c('M', 'X')]
    
# Start MATCH module 
    cat("\n","   (6) MATCH module")
      # Matching M
      cat("\n","        ...matching data")
      mirror$M <- split(mirror$M, is.na(mirror$M$v_X))
      mirror$X <- split(mirror$X, is.na(mirror$X$v_M))
      mirror <- unlist(mirror, recursive = F)
      # mirror$M.FALSE should equal mirror$X.FALSE
      stopifnot(setequal(names(mirror),names(out_names)))
      names(mirror) <- out_names[names(mirror)]
      # renamed mirror to match out_names
  # end MATCH module
	  # update counts   
	  cat("\n","   (7) updating counts")
	  counter$n_pair[t, paste('n_', names(mirror), sep = '')] <- as.data.frame(lapply(mirror, nrow))
	  # verify matched M==X
	  if(counter$n_pair$n_M_matched==counter$n_pair$n_X_matched){
	    tmp <- colnames(mirror$X_matched)
	    colnames(mirror$X_matched)[match(c("hs_rpt","hs_ptn","i","j"), tmp)] <- c("hs_ptn","hs_rpt","j","i")
	    tmp <- duplicated(rbind(mirror$M_matched, mirror$X_matched))
	    if(sum(tmp)!=counter$n_pair$n_M_matched[t])logg(paste(year, '!', 'MX nrow not identical', sep = '\t'))
	    else{logg(paste(year, ':', 'identical', sep = '\t')); mirror$X_matched <-NULL}
	  }else logg(paste(year, '!', 'MX not identical', sep = '\t'))
	  
	  # uploading results
	  tmp <- paste(tag, year, names(mirror), sep = '-'); tmp <- paste('tmp/', tmp, '.csv.bz2', sep = '')
	  names(tmp) <- names(mirror)
	  for(i in names(mirror)){
	  ecycle(write.csv(mirror[[i]], file = bzfile(tmp[i]),row.names=FALSE,na=""), 
             ecycle(s3write_using(mirror[[i]], FUN = function(x, y)write.csv(x, file=bzfile(y), row.names = FALSE),
                                  bucket = out_bucket, object = basename(tmp[i])),
                     logg(paste(year, '!', paste('uploading', basename(tmp[i]), 'failed', sep = ' '), sep = '\t')), max_try), 
             max_try,
             ecycle(put_object(tmp[i], basename(tmp[i]), bucket = out_bucket), 
                    logg(paste(year, '!', paste('uploading', basename(tmp[i]), 'failed', sep = ' '), sep = '\t')),
                    max_try,
                    {logg(paste(year, '|', paste('uploaded', basename(tmp[i]), sep = ' '), sep = '\t')); unlink(tmp[i])}))
	  }
      
    # Cache counts
	writeLines(toJSON(counter), opcounter)

    # cleanup some
    rm(mirror)
} # end t loop
#
put_object(oplog, basename(oplog), bucket = out_bucket)
put_object(opcounter, basename(opcounter), bucket = out_bucket)
# system('sudo shutdown')
