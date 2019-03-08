# 1. decompress and subset the raw data to only keep aggregation level 6 and remove country names and commodity names;
# 3. compress the results and send them to AWS S3;
# 4. record the information of downloaded data;
# 5. send a progress report to Google Drive;
# 6. shutdown system after completion.

rm(list=ls()) # clean up environment
pkgs <- c('aws.s3', 'aws.ec2metadata', 'stats', 'jsonlite', 'scripting', 'data.table')
for(i in pkgs)library(i, character.only = T)

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
cols_UN <- rep('NULL', 19)
names(cols_UN) <- c("classification","year","period","perioddesc","aggregatelevel","isleafcode","tradeflowcode",
                  "tradeflow","reportercode","reporteriso","partnercode","partneriso",
                  "commoditycode","qtyunitcode","qtyunit","qty","netweightkg","tradevalueus","flag")
incol <- c("tradeflowcode","classification","reportercode","partnercode","commoditycode", "tradevalueus", "qtyunitcode", "qty", "netweightkg")
names(incol) <- c("tf","hs","i","j","k","v","q_code","q","q_kg")
cols_UN[incol] <- c("integer","character","integer","integer","character","numeric","integer","numeric","numeric")
cols_hk <- c(rep("integer",3),"character","numeric",rep("integer",2),rep("numeric",2))
names(cols_hk) <- c("t","origin_hk","consig_hk","k","vrx_hkd","origin_un","consig_un","usd_per_hkd","vrx_usd")
cols_swiss <- c("integer","integer","character","integer",rep("numeric",2))
# use "character" for k or codes like '9999AA' messes up
tfn <- c('M','rM','X','rX')
names(tfn) <- c('1','4','2','3')

#===================================================================================================================================#
				  
oplog <- paste('logs/', oplog, sep = '')
dinfo <- paste('logs/', dinfo, sep = '')
opcounter <- paste('data/', opcounter, sep = '')
logg <- function(x)mklog(x, path = oplog)
Sys.setenv("AWS_ACCESS_KEY_ID" = keycache$Access_key_ID[keycache$service==usr],
           "AWS_SECRET_ACCESS_KEY" = keycache$Secret_access_key[keycache$service==usr])
if(is.na(Sys.getenv()["AWS_DEFAULT_REGION"]))Sys.setenv("AWS_DEFAULT_REGION" = gsub('.{1}$', '', metadata$availability_zone()))
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
out_names <- c('M-matched', 'X-matched', 'M-orphaned', 'M-lost')
names(out_names) <- c('M', 'X', 'M_orphan', 'M_lost')
tmp <- c('M_p','X_p', names(out_names))
counter$n_pair_match <- matrix(nrow = n_dates, ncol = 10, dimnames = list(dates, paste('n_', tmp, sep = '')))
counter$n_pair_match <- as.data.frame(counter$n_pair_match)

# read in 'treat' function for preliminary data treatments in TREAT module
source("prox-treat.R")

# Prepare for SWISS module
    # Data M_swiss and X_swiss compiled by Joe Spanjers from Swiss source data
      ecycle(M_swiss <- s3read_using(FUN = function(x)fread(x, header=T, na.strings="", colClasses = cols_swiss), 
                                     object = "Swiss_trade_m.csv", bucket = sup_bucket),
             {tmp <- paste('0000', '!', 'loading Swiss M failed', sep = '\t'); logg(tmp); stop(tmp)},
             max_try)
      ecycle(X_swiss <- s3read_using(FUN = function(x)fread(x, header=T, na.strings="", colClasses = cols_swiss), 
                                     object = "Swiss_trade_x.csv", bucket = sup_bucket),
             {tmp <- paste('0000', '!', 'loading Swiss X failed', sep = '\t'); logg(tmp); stop(tmp)}, 
             max_try)
      colnames(M_swiss) <- c("i","j","k","t","v_M_swiss","q_kg_M_swiss")
      colnames(X_swiss) <- c("i","j","k","t","v_X_swiss","q_kg_X_swiss")
      # # keep only records for commodity 710812 which comprises 98% of Swiss trade in non-monetary gold
      #     (NB, monetary gold trade flows should not be included in UN-Comtrade dataset for any country)
      M_swiss <- subset(M_swiss,M_swiss$k==710812)
      X_swiss <- subset(X_swiss,X_swiss$k==710812)
      setkeyv(M_swiss, c('i', 'j', 'k')); setkeyv(X_swiss, c('i', 'j', 'k'))
      # read in "swiss.r" a procedure to make adjustments for each tradeflow matrix
      source("prox-swiss.R")

# Loop by date
for (t in 1:n_dates) {
  year <- dates[t]
  cat("\n","\n","PROCESSING DATA FOR ",as.character(year))

# Start RAW module
    cat("\n","   (1) RAW module")
    cat("\n","        ...reading data file downloaded from UN-COMTRADE...","\n")
    ecycle(save_object(object = paste(year, '.csv.bz2', sep = ''), bucket = in_bucket, file = 'tmp/tmp.csv.bz2', overwrite = TRUE),
		   {logg(paste(year, '!', 'retrieving file failed', sep = '\t')); next}, max_try)
	ecycle(rdata <- fread(cmd = "bzip2 -dc ./tmp/tmp.csv.bz2", header=T, colClasses = cols_UN),
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
    cat("\n","        ...making Swiss adjustments for M...") ; rdata$M  <- swiss(rdata$M,M_swiss,t)
    cat("\n","        ...making Swiss adjustments for X...") ; rdata$X  <- swiss(rdata$X,X_swiss,t)
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

	counter$n_pair_match[t, c('n_M_p', 'n_X_p')] <- as.data.frame(lapply(mirror, nrow))[c('M', 'X')]
    
# Start MATCH module 
    cat("\n","   (6) MATCH module")
	rmatch <- list()
      # Matching M
      cat("\n","        ...matching M data")
      rmatch$M <- subset(mirror$M, mirror$M$v_X>0 & mirror$M$v_M>0)
      rmatch$M_orphan  <- subset(mirror$M,is.na(mirror$M$v_X))
      # Matching X
      cat("\n","        ...matching X data")
      rmatch$X <- subset(mirror$X, mirror$X$v_M>0 & mirror$X$v_X>0)              # should equal rmatch$M from above
      rmatch$M_lost <- subset(mirror$X,is.na(mirror$X$v_M)) # M_lost == X_orphan
  # end MATCH module
      rm(mirror)
	  stopifnot(setequal(names(rmatch),names(out_names)))
	  # update counts   
	  cat("\n","   (7) updating counts")
	  counter$n_pair_match[t, paste('n_', names(rmatch), sep = '')] <- as.data.frame(lapply(rmatch, nrow))
	  # verify M==X
	  if(counter$n_pair_match$n_M==counter$n_pair_match$n_X){
	    tmp <- colnames(rmatch$X)
	    colnames(rmatch$X)[match(c("hs_rpt","hs_ptn","i","j"), tmp)] <- c("hs_ptn","hs_rpt","j","i")
	    tmp <- duplicated(rbind(rmatch$M, rmatch$X))
	    if(sum(tmp)!=counter$n_pair_match$n_M)logg(paste(year, '!', 'MX nrow not identical', sep = '\t'))
	    else{logg(paste(year, '|', 'identical', sep = '\t')); rmatch$X <-NULL}
	  }else logg(paste(year, '!', 'MX not identical', sep = '\t'))
	  
	  # uploading results
	  tmp <- paste(tag, year, out_names[names(rmatch)], sep = '-'); tmp <- paste('tmp/', tmp, '.csv.bz2', sep = '')
	  names(tmp) <- names(out_names[names(rmatch)])
	  for(i in names(rmatch)){
	  ecycle(write.csv(rmatch[[i]], file = bzfile(tmp[i]),row.names=FALSE,na=""), 
             ecycle(s3write_using(rmatch[[i]], FUN = function(x, y)write.csv(x, file=bzfile(y), row.names = FALSE),
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
    rm(rmatch)
} # end t loop
#
put_object(oplog, basename(oplog), bucket = out_bucket)
put_object(opcounter, basename(opcounter), bucket = out_bucket)
# system('sudo shutdown')
