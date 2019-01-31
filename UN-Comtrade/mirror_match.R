# 1. automatically download Comtrade data that is newer than our existing data from the UN;
# 2. decompress and subset the raw data to only keep aggregation level 6 and remove country names and commodity names;
# 3. compress the results and send them to AWS S3;
# 4. record the information of downloaded data;
# 5. send a progress report to Google Drive;
# 6. shutdown system after completion.

rm(list=ls()) # clean up environment
library("aws.s3")
library('aws.ec2metadata')
library('jsonlite')
library('googledrive')
library("data.table")

#=====================================modify the following parameters for each new run==============================================#

usr <- 'aws00' # the user account for using AWS service
years <- 2016:2000 # the years we want to download
hk_years <- 2016:2000 # years HK re-export data is available
out_bucket <- 'gfi-mirror-analysis' # save the results to a S3 bucket called 'gfi-mirror-analysis'
in_bucket <- 'gfi-comtrade' # read in raw data from this bucket
sup_bucket <- 'gfi-supplemental' # supplemental files
tag <- "Comtrade"
counts_DIR <- "Comtrade_Counts"
oplog <- 'mirror_match.log' # progress report file
dinfo <- 'download.log' # file of the information of the downloaded data
max_try <- 10 # the maximum number of attempts for a failed process
keycache <- read.csv('~/vars/accesscodes.csv', header = TRUE, stringsAsFactors = FALSE) # the database of our credentials
googlekey <- '~/vars/googleapi.json' # Google service credentials
gpath <- 'Mirror-Analysis' # the folder name of this project
i_WRITEALL <- 1  # set to 1 if you wish to write intermediate output to disk, but it adds significantly to run time
# column names of the raw data; if it gets changed, we may also need to change the subsetting code.
col_names_UN <- c("classification","year","period","perioddesc","aggregatelevel","isleafcode","tradeflowcode",
                  "tradeflow","reportercode","reporteriso","partnercode","partneriso",
                  "commoditycode","qtyunitcode","qtyunit","qty","netweightkg","tradevalueus","flag")

#===================================================================================================================================#
				  
oplog <- paste('logs/', oplog, sep = '')
dinfo <- paste('logs/', dinfo, sep = '')
logg <- function(x){
  txt <- paste(Sys.time(), Sys.timezone(), x, sep = '\t')
  cat(paste(txt,'\n', sep=''), file = oplog, append = TRUE)
}
Sys.setenv("AWS_ACCESS_KEY_ID" = keycache$Access_key_ID[keycache$service==usr],
           "AWS_SECRET_ACCESS_KEY" = keycache$Secret_access_key[keycache$service==usr])
if(is.na(Sys.getenv()["AWS_DEFAULT_REGION"]))Sys.setenv("AWS_DEFAULT_REGION" = gsub('.{1}$', '', metadata$availability_zone()))
options(stringsAsFactors= FALSE)
cat(NULL, file = oplog, append = FALSE)


# Set up counters to collect for multiple years
n_dates <- length(years)
n_RAW <-  data.frame(Year = dates, M = rep(0,n_dates),X = rep(0,n_dates),rX = rep(0,n_dates),rM = rep(0,n_dates))
n_TREAT <-  data.frame(Year = dates, M = rep(0,n_dates),X = rep(0,n_dates),rX = rep(0,n_dates),rM = rep(0,n_dates))
n_SWISS <-  data.frame(Year = dates, M = rep(0,n_dates),X = rep(0,n_dates),rX = rep(0,n_dates),rM = rep(0,n_dates))
n_MATCH_M <-  data.frame(Year = dates, Paired = rep(0,n_dates), Matched = rep(0,n_dates),Orphaned = rep(0,n_dates),Lost = rep(0,n_dates))
v_MATCH_X <- n_MATCH_X <- v_MATCH_M <-  n_MATCH_M
n_M_matched_q <- data.frame(Year= dates,
                            Total = rep(0,n_dates),
                            Matched = rep(0,n_dates),
                            Number_of_items = rep(0,n_dates),
                            No_quantity = rep(0,n_dates),
                            Area_sq_meters = rep(0,n_dates),
                            Electrical_energy_thou_kw = rep(0,n_dates),
                            Length_meters = rep(0,n_dates),
                            Number_of_pairs = rep(0,n_dates),
                            Volume_liters = rep(0,n_dates),
                            Weight_kg = rep(0,n_dates),
                            Number_of_items_thou = rep(0,n_dates),
                            Number_of_pkgs = rep(0,n_dates),
                            UNKNOWN_11 = rep(0,n_dates),
                            Volume_cubic_meters = rep(0,n_dates),
                            Weight_carats = rep(0,n_dates))
v_X_matched_q <- n_X_matched_q <- v_M_matched_q <- n_M_matched_q

# Loop by date
for (t in 1:n_dates) {
  year <- dates[t]
  cat("\n","\n","PROCESSING DATA FOR ",as.character(year))

# Start RAW module
    cat("\n","   (1) RAW module")
    cat("\n","        ...reading data file downloaded from UN-COMTRADE...","\n")
    raw_data <- s3read_using(FUN = function(x)read.csv(xzfile(x), header=TRUE), 
                             object = paste(year, '.csv.xz', sep = ''), bucket = in_bucket)
    if(!identical(colnames(raw_data),col_names_UN)){
      logg(paste(year, '!', 'colnames mismatching', sep = '\t'))
      next
      }
    cat("\n","        ...eliminating redundant columns...")
    raw_data <- raw_data[,c("tradeflowcode","classification","reportercode","partnercode","commoditycode","qtyunitcode",
                            "qty","netweightkg","tradevalueus")]
    colnames(raw_data) <- c("tf","hs","i","j","k","q_code","q","q_kg","v")
    raw_data <- raw_data[,c("tf","hs","i","j","k","v","q_code","q","q_kg")]
    cat("\n","        ...parsing M...")
    M <-subset(raw_data,raw_data$tf==1) ; M <- M[,c("hs","i","j","k","v","q_code","q","q_kg")]
    colnames(M) <- c("hs","i","j","k","v_M","q_code_M","q_M","q_kg_M")
    n_RAW$M[t] <- dim(M)[1]
    cat("\n","        ...parsing X...")
    X<-subset(raw_data,raw_data$tf==2) ; X <- X[,c("hs","i","j","k","v","q_code","q","q_kg")]
    colnames(X) <- c("hs","i","j","k","v_X","q_code_X","q_X","q_kg_X")
    n_RAW$X[t] <- dim(X)[1]
    cat("\n","        ...parsing rX...")
    rX<-subset(raw_data,raw_data$tf==3) ; rX <- rX[,c("hs","i","j","k","v","q_code","q","q_kg")]
    colnames(rX) <- c("hs","i","j","k","v_rX","q_code_rX","q_rX","q_kg_rX")
    n_RAW$rX[t] <- dim(rX)[1]
    cat("\n","        ...parsing rM...")
    rM<-subset(raw_data,raw_data$tf==4) ; rM <- rM[,c("hs","i","j","k","v","q_code","q","q_kg")]
    colnames(rM) <- c("hs","i","j","k","v_rM","q_code_rM","q_rM","q_kg_rM")
    n_RAW$rM[t] <- dim(rM)[1]
    remove(raw_data)
#   End RAW module

#   Start TREAT module 
    # read in function to process preliminary data treatments
    source("prox-treat.R",sep="")
    cat("\n","   (2) TREAT module")
    cat("\n","        ...treating M...") ; M  <- treat(M,t)
    cat("\n","        ...treating X...") ; X  <- treat(X,t)
    cat("\n","        ...treating rX...") ; rX <- treat(rX,t)
    cat("\n","        ...treating rM...") ; rM <- treat(rM,t)
      
    # counts
      n_TREAT$M[t] <- dim(M)[1]
      n_TREAT$X[t] <- dim(X)[1]
      n_TREAT$rX[t] <- dim(rX)[1]
      n_TREAT$rM[t] <- dim(rM)[1]
      # write
      if(i_WRITEALL) {
          cat("\n","        ...writing treated flows to disk...")
          cat("\n","           ...M...") ; fwrite(M,file="tmp/M.csv",sep=",",col.names=TRUE,row.names=FALSE,na="")
          cat("\n","           ...X...") ; fwrite(X,file="tmp/X.csv",sep=",",col.names=TRUE,row.names=FALSE,na="")
          cat("\n","           ...rX...") ; fwrite(rX,file="tmp/rX.csv",sep=",",col.names=TRUE,row.names=FALSE,na="")
          cat("\n","           ...rM...") ; fwrite(rM,file="tmp/rM.csv",sep=",",col.names=TRUE,row.names=FALSE,na="") 
      }
      logg(paste(year, ':', 'treated', sep = '\t'))

    #   End TREAT module
 
# Start SWISS module
      # Data M_swiss and X_swiss compiled by Joe Spanjers from Swiss source data and stored in swiss_DIR as CSV files
      M_swiss <- s3read_using(FUN = function(x)read.csv(x, header=TRUE, na.strings=""), 
                              object = "Swiss_trade_m.csv", bucket = sup_bucket)
      X_swiss <- s3read_using(FUN = function(x)read.csv(x, header=TRUE, na.strings=""), 
                              object = "Swiss_trade_x.csv", bucket = sup_bucket)
      colnames(M_swiss) <- c("i","j","k","t","v_M_swiss","q_kg_M_swiss")
      colnames(X_swiss) <- c("i","j","k","t","v_X_swiss","q_kg_X_swiss")
      # # keep only records for commodity 710812 which comprises 98% of Swiss trade in non-monetary gold
      #     (NB, monetary gold trade flows should not be included in UN-Comtrade dataset for any country)
      M_swiss <- subset(M_swiss,M_swiss$k==710812)
      X_swiss <- subset(X_swiss,X_swiss$k==710812)
      # read in "swiss.r" a procedure to make adjustments for each tradeflow matrix
      colnames(M_swiss) <- c("i","j","k","t","v_M_swiss","q_kg_M_swiss")
      colnames(X_swiss) <- c("i","j","k","t","v_X_swiss","q_kg_X_swiss")
      source("prox-swiss.R")
    cat("\n","   (3) SWISS module")
    cat("\n","        ...making Swiss adjustments for M...") ; M  <- swiss(M,M_swiss,t)
    cat("\n","        ...making Swiss adjustments for X...") ; X  <- swiss(X,X_swiss,t)
   # counts
    n_SWISS$M[t] <- dim(M)[1]
    n_SWISS$X[t] <- dim(X)[1]
    n_SWISS$rX[t] <- dim(rX)[1]
    n_SWISS$rM[t] <- dim(rM)[1]
      
    # write
      if(i_WRITEALL) {
        cat("\n","        ...writing Swiss-adjusted flows to disk...")
          cat("\n","           ...M...") ; fwrite(M,file="tmp/M.csv",sep=",",col.names=TRUE,row.names=FALSE,na="")
          cat("\n","           ...X...") ; fwrite(X,file="tmp/X.csv",sep=",",col.names=TRUE,row.names=FALSE,na="")
      }
    logg(paste(year, ':', 'swiss-adjusted', sep = '\t'))
    rm(M_swiss, X_swiss)
    # End SWISS module
       
#   Start PAIR module 
    cat("\n","   (4) PAIR module")
    
    # Pair M
    cat("\n","        ...pairing M data")
    cols_M <- c("hs","i","j","k","v_M","q_code_M","q_M","q_kg_M")
    cols_X <- c("hs","j","i","k","v_X","q_code_X","q_X","q_kg_X")
    cols_rX <- c("hs","j","i","k","v_rX","q_code_rX","q_rX","q_kg_rX")
    cols_rM <- c("hs","i","j","k","v_rM","q_code_rM","q_rM","q_kg_rM")
    M_paired <- subset(M,M$v_M>0) ; colnames(M_paired) <- cols_M
    z_X <- X  ; colnames(z_X) <- cols_X
    z_rX <- rX  ; colnames(z_rX) <- cols_rX
    z_rM <- rM  ; colnames(z_rM) <- cols_rM
    #
    cat("\n","            ...M to X") ;  M_paired <- merge(x=M_paired,y=z_X,by = c("i","j","k"), all.x=TRUE)
    colnames(M_paired) <- c("i","j","k","hs_rpt","v_M","q_code_M","q_M","q_kg_M","hs_ptn","v_X","q_code_X","q_X","q_kg_X")
    cat("\n","            ...M to rX") ; M_paired <- merge(x=M_paired,y=z_rX,by = c("i","j","k"), all.x=TRUE)
    M_paired <- M_paired[,c("i","j","k","hs_rpt","v_M","q_code_M","q_M","q_kg_M","hs_ptn","v_X","q_code_X","q_X","q_kg_X","v_rX")]
    cat("\n","            ...M to rM") ; M_paired <- merge(x=M_paired,y=z_rM,by = c("i","j","k"), all.x=TRUE)
    M_paired <- M_paired[,c("hs_rpt","hs_ptn","i","j","k","v_M","v_X","v_rX","v_rM","q_M","q_X","q_kg_M","q_kg_X","q_code_M","q_code_X")]
    #
    cat("\n","        ...writing unadjusted paired M data")
        fwrite(M_paired,"tmp/M.csv",sep=",",col.names=TRUE,row.names=FALSE,na="")
    logg(paste(year, ':', 'M-paired', sep = '\t'))
  
    # Pair X
    cat("\n","        ...pairing X data")
    cols_M <- c("hs","j","i","k","v_M","q_code_M","q_M","q_kg_M")
    cols_X <- c("hs","i","j","k","v_X","q_code_X","q_X","q_kg_X")
    cols_rX <- c("hs","i","j","k","v_rX","q_code_rX","q_rX","q_kg_rX")
    cols_rM <- c("hs","j","i","k","v_rM","q_code_rM","q_rM","q_kg_rM")
    X_paired <- subset(X,X$v_X>0) ; colnames(X_paired) <- cols_X
    z_M <- M  ; colnames(z_M) <- cols_M
    z_rX <- rX  ; colnames(z_rX) <- cols_rX
    z_rM <- rM  ; colnames(z_rM) <- cols_rM
      #
    cat("\n","            ...X to M") ;  X_paired <- merge(x=X_paired,y=z_M,by = c("i","j","k"), all.x=TRUE)
    colnames(X_paired) <- c("i","j","k","hs_rpt","v_X","q_code_X","q_X","q_kg_X","hs_ptn","v_M","q_code_M","q_M","q_kg_M")
    cat("\n","            ...X to rX") ; X_paired <- merge(x=X_paired,y=z_rX,by = c("i","j","k"), all.x=TRUE)
    X_paired <- X_paired[,c("i","j","k","hs_rpt","v_X","q_code_X","q_X","q_kg_X","hs_ptn","v_M","q_code_M","q_M","q_kg_M","v_rX")]
    cat("\n","            ...X to rM") ; X_paired <- merge(x=X_paired,y=z_rM,by = c("i","j","k"), all.x=TRUE)
    X_paired <- X_paired[,c("hs_rpt","hs_ptn","i","j","k","v_M","v_X","v_rX","v_rM","q_M","q_X","q_kg_M","q_kg_X","q_code_M","q_code_X")]
    #
    #
    cat("\n","        ...writing unadjusted paired X data")
      fwrite(X_paired,"tmp/X.csv",sep=",",col.names=TRUE,row.names=FALSE,na="")
    logg(paste(year, ':', 'X-paired', sep = '\t'))
    rm(z_M, z_X, z_rX, z_rM)
    # end PAIR module

# Start Hong Kong module
    cat("\n","   (5) Hong Kong module","\n")
    # Data hkrx compiled by Joe Spanjers from Hong Kong sources and stored  in hk_DIR as CSV file
    nms_hk <- c("t","k","origin_hk","consig_hk","vrx_hkd","origin_un","consig_un","usd_per_hkd","vrx_usd")
    cls_hk <- c(rep("integer",4),"numeric",rep("integer",2),rep("numeric",2))
    rx_hk <- s3read_using(FUN = function(x)read.csv(unz(x, 'hkrx.csv'), header=TRUE, na.strings=""), 
                          object = "HK_trade_rx.zip", bucket = sup_bucket)
    colnames(rx_hk) <- nms_hk
    rx_hk <- rx_hk[,c("t","origin_un","consig_un","k","vrx_usd")]
    colnames(rx_hk) <- c("t","i","j","k","v_rx_hk")
    # operate on X-paired and M-paired and then redo the matching etc
    if (year %in% hk_years) {
        hk <- subset(rx_hk,rx_hk$t==year)
        hk <- hk[,c("i","j","k","v_rx_hk")]
        # adjusting M variables
        temp <- M_paired
        colnames(hk) <- c("j","i","k","v_rx_hk")
        M_paired <- merge(x=M_paired,y=hk,by=c("i","j","k"),all.x=TRUE)
        hk_344 <- hk[,c("i","k","v_rx_hk")]
        colnames(hk_344) <- c("i","k","adj_344")
        hk_344 <- aggregate(hk_344$adj_344,list(hk_344$i,hk_344$k),sum) ; colnames(hk_344) <- c("i","k","adj_344")
        M_paired <- merge(x=M_paired,y=hk_344,by=c("i","k"),all.x=TRUE)
        M_paired[is.na(M_paired$v_rx_hk),"v_rx_hk"] <- 0
        M_paired[is.na(M_paired$adj_344),"adj_344"] <- 0
        temp <- M_paired
        M_paired[!M_paired$j==344,"v_M"] <- M_paired[!M_paired$j==344,"v_M"] - M_paired[!M_paired$j==344,"v_rx_hk"]
        M_paired[ M_paired$j==344,"v_M"] <- M_paired[ M_paired$j==344,"v_M"] + M_paired[ M_paired$j==344,"adj_344"]
        M_paired <- M_paired[,c("hs_rpt","hs_ptn","i","j","k","v_M","v_X","v_rX","v_rM","q_M","q_X","q_kg_M","q_kg_X","q_code_M","q_code_X")]
        junk <- subset(M_paired,M_paired$v_M<0)
        M_paired[M_paired$v_M<0,"v_M"] <- temp[M_paired$v_M<0,"v_M"]  ;   # undo the adjustment for negative values
        M_paired[M_paired$i==752,"v_M"] <- temp[M_paired$i==752,"v_M"]  ; # undo the adjustment for Sweden  (OECD[2016], p. 19)
        M_paired[M_paired$i==348,"v_M"] <- temp[M_paired$i==348,"v_M"]  ; # undo the adjustment for Hungary (OECD[2016], p. 19)
        fwrite(junk,file="M-not HK adjusted.csv",col.names=TRUE,row.names = FALSE,sep=",",na = "")
        # adjusting X variables
        colnames(hk) <- c("i","j","k","v_rx_hk")
        X_paired <- merge(x=X_paired,y=hk,by=c("i","j","k"),all.x=TRUE)
        hk_344 <- hk[,c("j","k","v_rx_hk")]
        colnames(hk_344) <- c("j","k","adj_344")
        hk_344 <- aggregate(hk_344$adj_344,list(hk_344$j,hk_344$k),sum) ; colnames(hk_344) <- c("j","k","adj_344")
        X_paired <- merge(x=X_paired,y=hk_344,by=c("j","k"),all.x=TRUE)
        X_paired[is.na(X_paired$v_rx_hk),"v_rx_hk"] <- 0
        X_paired[is.na(X_paired$adj_344),"adj_344"] <- 0
        temp <- X_paired
        X_paired[!X_paired$i==344,"v_M"] <- X_paired[!X_paired$i==344,"v_M"] - X_paired[!X_paired$i==344,"v_rx_hk"]
        X_paired[ X_paired$i==344,"v_M"] <- X_paired[ X_paired$i==344,"v_M"] + X_paired[ X_paired$i==344,"adj_344"]
        X_paired <- X_paired[,c("hs_rpt","hs_ptn","i","j","k","v_X","v_M","v_rM","v_rX","q_X","q_M","q_kg_X","q_kg_M","q_code_X","q_code_M")]
        junk <- subset(X_paired,X_paired$v_M<0)
        X_paired[X_paired$v_M<0,"v_M"] <- temp[X_paired$v_M<0,"v_M"]  ; # undo the adjustment for negative values
        X_paired[X_paired$j==752,"v_M"] <- temp[X_paired$j==752,"v_M"]  ; # undo the adjustment for Sweden  (OECD[2016], p. 19)
        X_paired[X_paired$j==348,"v_M"] <- temp[X_paired$j==348,"v_M"]  ; # undo the adjustment for Hungary (OECD[2016], p. 19)
        fwrite(junk,file="X-not HK adjusted.csv",col.names = TRUE,row.names = FALSE,sep=",",na = "")
        remove(temp,junk)
    }
    cat("\n","        ...writing adjusted paired M data") ; fwrite(M_paired,"tmp/M.csv",sep=",",col.names=TRUE,row.names=FALSE,na="")
    logg(paste(year, ':', 'M-HK-adjusted', sep = '\t'))
    cat("\n","        ...writing adjusted paired X data") ; fwrite(X_paired,"tmp/X.csv",sep=",",col.names=TRUE,row.names=FALSE,na="")
    logg(paste(year, ':', 'X-HK-adjusted', sep = '\t'))
    # end Hong Kong module

# Start MATCH module 
    cat("\n","   (6) MATCH module")
 
      # Matching M
      cat("\n","        ...matching M data")
      M_matched <- subset(M_paired,M_paired$v_X>0)
      M_orphan  <- subset(M_paired,is.na(M_paired$v_X))
      cat("\n","        ...matching M quantity data")
      M_matched_q <- subset(M_matched,M_matched$q_code_M==M_matched$q_code_X)
      M_matched_q <- subset(M_matched_q, ((M_matched_q$q_M>0)&(M_matched_q$q_X>0)))
          
      # Matching X
      cat("\n","        ...matching X data")
      X_matched <- subset(X_paired,X_paired$v_M>0)              # should equal M_matched from above
      X_orphan  <- subset(X_paired,is.na(X_paired$v_M))
      cat("\n","        ...matching X quantity data")
      X_matched_q <- subset(X_matched,X_matched$q_code_X==X_matched$q_code_M)
      X_matched_q <- subset(X_matched_q, ((X_matched_q$q_M>0)&(X_matched_q$q_X>0)))
      #
      M_lost <- X_orphan
      X_lost <- M_orphan
         #
      cat("\n","        ...writing matched M data")
      s3write_using(M_matched, FUN = function(x, y)write.csv(x, file=xzfile(y), row.names = FALSE),
                    bucket = out_bucket,
                    object = paste(tag, '-M-matched-', year, '.csv.xz', sep = ''))
        fwrite(M_matched,"M-matched.csv",sep=",",col.names=TRUE,row.names=FALSE,na="")
        fwrite(M_matched_q,"M-matched-q.csv",sep=",",col.names=TRUE,row.names=FALSE,na="")
        cat("\n","        ...writing matched X data")
        fwrite(X_matched,"X-matched.csv",sep=",",col.names=TRUE,row.names=FALSE,na="")
        fwrite(X_matched_q,"X-matched-q.csv",sep=",",col.names=TRUE,row.names=FALSE,na="")
        cat("\n","        ...writing lost X & M data")
        fwrite(M_orphan,"M-orphaned.csv",sep=",",col.names=TRUE,row.names=FALSE,na="")
        fwrite(X_orphan,"X-orphaned.csv",sep=",",col.names=TRUE,row.names=FALSE,na="")
        fwrite(X_lost,"X-lost.csv",sep=",",col.names=TRUE,row.names=FALSE,na="")
        fwrite(M_lost,"M-lost.csv",sep=",",col.names=TRUE,row.names=FALSE,na="")
# end MATCH module

# update counts   
      cat("\n","   (7) updating counts")
      n_MATCH_M$Paired[t] <- dim(M_paired)[1]
      n_MATCH_M$Matched[t] <- dim(M_matched)[1]
      n_MATCH_M$Orphaned[t] <- dim(M_orphan)[1]
      n_MATCH_M$Lost[t] <- dim(M_lost)[1]
      #
      n_MATCH_X$Paired[t] <- dim(X_paired)[1]
      n_MATCH_X$Matched[t] <- dim(X_matched)[1]
      n_MATCH_X$Orphaned[t] <- dim(X_orphan)[1]
      n_MATCH_X$Lost[t] <- dim(X_lost)[1]
      #
      v_MATCH_M$Paired[t] <- sum(M_paired$v_M,na.rm=TRUE)
      v_MATCH_M$Matched[t] <- sum(M_matched$v_M,na.rm=TRUE)
      v_MATCH_M$Orphaned[t] <- sum(M_orphan$v_M,na.rm=TRUE)
      v_MATCH_M$Lost[t] <- sum(M_lost$v_X,na.rm=TRUE)
      #
      v_MATCH_X$Paired[t] <- sum(X_paired$v_X,na.rm=TRUE)
      v_MATCH_X$Matched[t] <- sum(X_matched$v_X,na.rm=TRUE)
      v_MATCH_X$Orphaned[t] <- sum(X_orphan$v_X,na.rm=TRUE)
      v_MATCH_X$Lost[t] <- sum(X_lost$v_M,na.rm=TRUE)
      #
      n_M_matched_q[t,2] <- dim(M_matched)[1]
      n_M_matched_q[t,3] <- dim(M_matched_q)[1]
      n_M_matched_q[t,4] <- sum(M_matched_q$q_code_M==5,na.rm=TRUE) 
      n_M_matched_q[t,5] <- sum(M_matched_q$q_code_M==1,na.rm=TRUE) 
      n_M_matched_q[t,6] <- sum(M_matched_q$q_code_M==2,na.rm=TRUE) 
      n_M_matched_q[t,7] <- sum(M_matched_q$q_code_M==3,na.rm=TRUE) 
      n_M_matched_q[t,8] <- sum(M_matched_q$q_code_M==4,na.rm=TRUE) 
      n_M_matched_q[t,9] <- sum(M_matched_q$q_code_M==6,na.rm=TRUE) 
      n_M_matched_q[t,10] <- sum(M_matched_q$q_code_M==7,na.rm=TRUE) 
      n_M_matched_q[t,11] <- sum(M_matched_q$q_code_M==8,na.rm=TRUE) 
      n_M_matched_q[t,12] <- sum(M_matched_q$q_code_M==9,na.rm=TRUE) 
      n_M_matched_q[t,13] <- sum(M_matched_q$q_code_M==10,na.rm=TRUE) 
      n_M_matched_q[t,14] <- sum(M_matched_q$q_code_M==11,na.rm=TRUE) 
      n_M_matched_q[t,15] <- sum(M_matched_q$q_code_M==12,na.rm=TRUE) 
      n_M_matched_q[t,16] <- sum(M_matched_q$q_code_M==13,na.rm=TRUE)
      #
      v_M_matched_q[t,2] <- sum(M_matched$v_M,na.rm=TRUE)
      v_M_matched_q[t,3] <- sum(M_matched_q$v_M,na.rm=TRUE)
      temp<- subset(M_matched_q,M_matched_q$q_code_M==5); v_M_matched_q[t,4] <- sum(temp$v_M,na.rm=TRUE) 
      temp<- subset(M_matched_q,M_matched_q$q_code_M==1); v_M_matched_q[t,5] <- sum(temp$v_M,na.rm=TRUE) 
      temp<- subset(M_matched_q,M_matched_q$q_code_M==2); v_M_matched_q[t,6] <- sum(temp$v_M,na.rm=TRUE) 
      temp<- subset(M_matched_q,M_matched_q$q_code_M==3); v_M_matched_q[t,7] <- sum(temp$v_M,na.rm=TRUE) 
      temp<- subset(M_matched_q,M_matched_q$q_code_M==4); v_M_matched_q[t,8] <- sum(temp$v_M,na.rm=TRUE) 
      temp<- subset(M_matched_q,M_matched_q$q_code_M==6); v_M_matched_q[t,9] <- sum(temp$v_M,na.rm=TRUE) 
      temp<- subset(M_matched_q,M_matched_q$q_code_M==7); v_M_matched_q[t,10] <- sum(temp$v_M,na.rm=TRUE) 
      temp<- subset(M_matched_q,M_matched_q$q_code_M==8); v_M_matched_q[t,11] <- sum(temp$v_M,na.rm=TRUE) 
      temp<- subset(M_matched_q,M_matched_q$q_code_M==9); v_M_matched_q[t,12] <- sum(temp$v_M,na.rm=TRUE) 
      temp<- subset(M_matched_q,M_matched_q$q_code_M==10); v_M_matched_q[t,13] <- sum(temp$v_M,na.rm=TRUE) 
      temp<- subset(M_matched_q,M_matched_q$q_code_M==11); v_M_matched_q[t,14] <- sum(temp$v_M,na.rm=TRUE) 
      temp<- subset(M_matched_q,M_matched_q$q_code_M==12); v_M_matched_q[t,15] <- sum(temp$v_M,na.rm=TRUE) 
      temp<- subset(M_matched_q,M_matched_q$q_code_M==13); v_M_matched_q[t,16] <- sum(temp$v_M,na.rm=TRUE) 
      #
      n_X_matched_q[t,2] <- dim(X_matched)[1]
      n_X_matched_q[t,3] <- dim(X_matched_q)[1]
      n_X_matched_q[t,4] <- sum(X_matched_q$q_code_X==5,na.rm=TRUE) 
      n_X_matched_q[t,5] <- sum(X_matched_q$q_code_X==2,na.rm=TRUE) 
      n_X_matched_q[t,6] <- sum(X_matched_q$q_code_X==3,na.rm=TRUE) 
      n_X_matched_q[t,7] <- sum(X_matched_q$q_code_X==4,na.rm=TRUE) 
      n_X_matched_q[t,8] <- sum(X_matched_q$q_code_X==5,na.rm=TRUE) 
      n_X_matched_q[t,9] <- sum(X_matched_q$q_code_X==6,na.rm=TRUE) 
      n_X_matched_q[t,10] <- sum(X_matched_q$q_code_X==7,na.rm=TRUE) 
      n_X_matched_q[t,11] <- sum(X_matched_q$q_code_X==8,na.rm=TRUE) 
      n_X_matched_q[t,12] <- sum(X_matched_q$q_code_X==9,na.rm=TRUE) 
      n_X_matched_q[t,13] <- sum(X_matched_q$q_code_X==10,na.rm=TRUE) 
      n_X_matched_q[t,14] <- sum(X_matched_q$q_code_X==11,na.rm=TRUE) 
      n_X_matched_q[t,15] <- sum(X_matched_q$q_code_X==12,na.rm=TRUE) 
      n_X_matched_q[t,16] <- sum(X_matched_q$q_code_X==13,na.rm=TRUE)
      #
      v_X_matched_q[t,2] <- sum(X_matched$v_X,na.rm=TRUE)
      v_X_matched_q[t,3] <- sum(X_matched_q$v_X,na.rm=TRUE)
      temp<- subset(X_matched_q,X_matched_q$q_code_X==5); v_X_matched_q[t,4] <- sum(temp$v_X,na.rm=TRUE) 
      temp<- subset(X_matched_q,X_matched_q$q_code_X==1); v_X_matched_q[t,5] <- sum(temp$v_X,na.rm=TRUE) 
      temp<- subset(X_matched_q,X_matched_q$q_code_X==2); v_X_matched_q[t,6] <- sum(temp$v_X,na.rm=TRUE) 
      temp<- subset(X_matched_q,X_matched_q$q_code_X==3); v_X_matched_q[t,7] <- sum(temp$v_X,na.rm=TRUE) 
      temp<- subset(X_matched_q,X_matched_q$q_code_X==4); v_X_matched_q[t,8] <- sum(temp$v_X,na.rm=TRUE) 
      temp<- subset(X_matched_q,X_matched_q$q_code_X==6); v_X_matched_q[t,9] <- sum(temp$v_X,na.rm=TRUE) 
      temp<- subset(X_matched_q,X_matched_q$q_code_X==7); v_X_matched_q[t,10] <- sum(temp$v_X,na.rm=TRUE) 
      temp<- subset(X_matched_q,X_matched_q$q_code_X==8); v_X_matched_q[t,11] <- sum(temp$v_X,na.rm=TRUE) 
      temp<- subset(X_matched_q,X_matched_q$q_code_X==9); v_X_matched_q[t,12] <- sum(temp$v_X,na.rm=TRUE) 
      temp<- subset(X_matched_q,X_matched_q$q_code_X==10); v_X_matched_q[t,13] <- sum(temp$v_X,na.rm=TRUE) 
      temp<- subset(X_matched_q,X_matched_q$q_code_X==11); v_X_matched_q[t,14] <- sum(temp$v_X,na.rm=TRUE) 
      temp<- subset(X_matched_q,X_matched_q$q_code_X==12); v_X_matched_q[t,15] <- sum(temp$v_X,na.rm=TRUE) 
      temp<- subset(X_matched_q,X_matched_q$q_code_X==13); v_X_matched_q[t,16] <- sum(temp$v_X,na.rm=TRUE) 
    
    #   End  module
    # cleanup some
    remove(M)
    remove(X)
    remove(rX)
    remove(rM)
    remove(M_paired)
    remove(M_matched)
    remove(M_orphan)
    remove(M_lost)
    remove(M_matched_q)
    remove(X_paired)
    remove(X_matched)
    remove(X_orphan)
    remove(X_lost)
    remove(X_matched_q)
    remove(temp)
} # end t loop
#
setwd(counts_DIR)
cat("\n","\n","...Writing out final M & X counts","\n")
if (auto_dates) {yr_suffix <- ".csv"} else {yr_suffix <- paste("-special.csv",sep="")}
write.csv(n_RAW,file=paste(tag,"-counts-record-RAW",yr_suffix,sep=""),row.names=FALSE,na="")
write.csv(n_TREAT,file=paste(tag,"-counts-record-TREAT",yr_suffix,sep=""),row.names=FALSE,na="")
write.csv(n_SWISS,file=paste(tag,"-counts-record-SWISS",yr_suffix,sep=""),row.names=FALSE,na="")
write.csv(n_MATCH_M,file=paste(tag,"-counts-record-MATCH_M",yr_suffix,sep=""),row.names=FALSE,na="")
write.csv(v_MATCH_M,file=paste(tag,"-counts-value-MATCH_M",yr_suffix,sep=""),row.names=FALSE,na="")
write.csv(n_MATCH_X,file=paste(tag,"-counts-record-MATCH_X",yr_suffix,sep=""),row.names=FALSE,na="")
write.csv(v_MATCH_X,file=paste(tag,"-counts-value-MATCH_X",yr_suffix,sep=""),row.names=FALSE,na="")
write.csv(n_M_matched_q,file=paste(tag,"-counts-record-MATCH_M_q",yr_suffix,sep=""),row.names=FALSE,na="")
write.csv(n_X_matched_q,file=paste(tag,"-counts-record-MATCH_X_q",yr_suffix,sep=""),row.names=FALSE,na="")
write.csv(v_M_matched_q,file=paste(tag,"-counts-value-MATCH_M_q",yr_suffix,sep=""),row.names=FALSE,na="")
write.csv(v_X_matched_q,file=paste(tag,"-counts-value-MATCH_X_q",yr_suffix,sep=""),row.names=FALSE,na="")
#
setwd(init_DIR)
