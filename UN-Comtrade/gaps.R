rm(list=ls()) # clean up environment
pkgs <- c('aws.s3', 'sparklyr', 'batchscr', 'dplyr', 'remotes')
for(i in pkgs)library(i, character.only = T)
install_github("sherrisherry/GFI-Cloud", subdir="pkg"); library(pkg)

#=====================================modify the following parameters for each new run==============================================#

usr <- 'aws00' # the user account for using AWS service
years <- 2016:2001 # the years we want to download
out_bucket <- 'gfi-work' # save the results to this S3 bucket
oplog <- 'gaps.log' # progress report file
outfile <- 'data/summ_prd.csv'
keycache <- read.csv('~/vars/accesscodes.csv', header = TRUE, stringsAsFactors = FALSE) # the database of our credentials
nload <- 3.5 # years for a worker
max_try <- 10L # the maximum number of attempts for a failed process
spark_home <- '/home/gfi/spark/spark-2.3.2-bin-hadoop2.7' # SPARK_HOME isn't required with 'local' master
master_node <- 'spark://ip-172-31-91-141.ec2.internal:7077'
cols_pdict <- rep('integer',5)
names(cols_pdict) <- c('t', 'i', 'j', 'd_dev_i', 'd_dev_j') # 'k' makes return value of spark_apply too large (thus too io consuming)

#===================================================================================================================================#
				  
oplog <- paste('/efs/logs', oplog, sep = '/')
logg <- function(x)mklog(x, path = oplog)
ec2env(keycache,usr); Sys.setenv('SPARK_HOME' = spark_home)
npar <- ceiling(length(years)/nload)
cat('Time\tZone\tYear\tMark\tStatus\n', file = oplog, append = FALSE)
conf <- spark_config()
conf$sparklyr.apply.env.AWS_ACCESS_KEY_ID <- Sys.getenv('AWS_ACCESS_KEY_ID')
conf$sparklyr.apply.env.AWS_SECRET_ACCESS_KEY <- Sys.getenv('AWS_SECRET_ACCESS_KEY')
conf$sparklyr.apply.env.AWS_DEFAULT_REGION <- Sys.getenv('AWS_DEFAULT_REGION')
conf$sparklyr.apply.env.TMPDIR <- '/home/gfi/temp'
conf$sparklyr.apply.env.max_try <- as.character(max_try)
conf$spark.executor.memory <- "11GB" # the memory to request from each executor, workers having less memory are not used
conf$spark.executor.instances <- npar
conf$spark.dynamicAllocation.enabled <- "false"

sc <- spark_connect(master= master_node, config = conf)

dist_codes <- function(years, cols){
  pkgs <- c('aws.s3', 'stats', 'cleandata', 'batchscr', 'pkg')
  for(i in pkgs){# spark starts a plain R session so settings taken care by RStudio need to be addressed.
    if(!require(i, character.only = T))install.packages(i, repos = "http://cran.us.r-project.org")
    library(i, character.only = T)}
  #==============================================================================================#
  
  cty <- NULL # set to NULL to select all countries within GFI's consideration; or exp. c(231, 404, 800)
  cifob_model <- 'cifob_model.rds.bz2'
  tag <- 'Comtrade'
  yrs_model <- 2016:2001 # the years for cifob_model
  in_dir <- '/efs/unct'
  cols_in <- c('character', rep("integer",3),rep("numeric",7),rep("integer",11)) 
  names(cols_in) <- c('k','t',"i","j","v_M","v_X","q_M","q_X", 'ln_distw', 'ln_distw_squared', 'ln_uvmdn', "q_code_M","q_code_X",
                      'd_fob', 'd_contig', 'd_conti', 'd_rta', 'd_landlocked_i', 'd_landlocked_j', 'd_dev_i', 'd_dev_j', 'd_hs_diff')
  cols_out <- c("t","j","i","k","v_M","v_X","q_M","q_X","q_code_M","d_dev_i","d_dev_j", 'v_M_fob', 'a_wt', 'gap_wtd')
  cols_model <- c('ln_distw', 'ln_distw_squared', 'ln_uvmdn', 'd_contig', 'd_conti', 'd_rta', 'd_landlocked_i', 'd_landlocked_j', 'd_dev_i', 'd_dev_j', 'd_hs_diff')
  cols_model <- append(cols_model, paste('d', yrs_model[-1], sep = '_'))
  
  #==============================================================================================#
  max_try <- as.integer(Sys.getenv('max_try'))
  out_bucket <- Sys.getenv('out_bucket')
  oplog <- Sys.getenv('oplog')
  logg <- function(x)mklog(x, path = oplog)
  options(stringsAsFactors= FALSE)
  years <- years[,1]; cols <- cols[,1]
  
  ecycle(save_object(object = cifob_model, bucket = out_bucket, file = 'tmp/tmp.csv.bz2', overwrite = TRUE),
         {logg(paste('0000','!', 'retrieving file failed', sep = '\t')); stop()}, max_try)
  ecycle(cifob_model <- readRDS(pipe("bzip2 -dkc ./tmp/tmp.csv.bz2")),
         {logg(paste('0000','!', 'loading file failed', sep = '\t')); stop()}, max_try)
  logg(paste('0000',':', 'loaded model', sep = '\t'))
  unlink('tmp/tmp.csv.bz2')
  
  if(is.null(cty))cty <- gfi_cty(logf = logg)
  
  predicts <- list()
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
    tinv$v_M_fob <- 0; tinv <- split(tinv, tinv$'d_fob')
    tinv$'1'$'v_M_fob' <- tinv$'1'$'v_M'
    pdict <- predict(cifob_model, tinv$'0'[, cols_model],type='response')
    logg(paste(year, ':', 'applied model', sep = '\t'))
    pdict <- exp(pdict)
    tinv$'0'$"v_M_fob" <- tinv$'0'$"v_M" / pdict
    # pdict <- cbind(tinv$'0'[, cols], pdict)
	# may use SSEtotal=SSEbetween+SSEwithin for stdev; median + cnt for global median.
	pdict <- aggregate(pdict, as.list(tinv$'0'[, cols]), function(x)c(max=max(x),min=min(x),sum=sum(x),cnt=length(x)))
	tinv <- do.call(rbind, tinv)
    tinv[(tinv$v_M_fob>tinv$v_M),"v_M_fob"] <- tinv[(tinv$v_M_fob>tinv$v_M),"v_M"]# DEFAULT: when fob>cif set FOB = CIF
    # calculate weights & gaps
    tinv$a_wt <- 1
    tinv[(tinv$q_M < tinv$q_X),"a_wt"] <- 1. - ((tinv[(tinv$q_M < tinv$q_X),"q_X"]- tinv[(tinv$q_M < tinv$q_X),"q_M"])/tinv[(tinv$q_M < tinv$q_X),"q_X"])
    tinv[(tinv$q_M >= tinv$q_X),"a_wt"] <- 1. - ((tinv[(tinv$q_M >= tinv$q_X),"q_M"]- tinv[(tinv$q_M >= tinv$q_X),"q_X"])/tinv[(tinv$q_M >= tinv$q_X),"q_M"])
    tinv$gap <- tinv$v_M_fob - tinv$v_X
    tinv$gap_wtd <- tinv$gap * tinv$a_wt
    logg(paste(year, ':', 'weighted gaps', sep = '\t'))
    file_out <- paste('tmp/', paste(tag, year, 'gaps.csv.bz2', sep = '-'), sep = '')
    ecycle(write.csv(tinv[, cols_out], file = bzfile(file_out),row.names=F,na=""), 
           ecycle(s3write_using(tinv[, cols_out], FUN = function(x, y)write.csv(x, file=bzfile(y), row.names = FALSE),
                                bucket = out_bucket, object = basename(file_out)),
                  logg(paste(year, '!', paste('uploading', basename(file_out), 'failed', sep = ' '), sep = '\t')), max_try), 
           max_try,
           ecycle(put_object(file_out, basename(file_out), bucket = out_bucket), 
                  logg(paste(year, '!', paste('uploading', basename(file_out), 'failed', sep = ' '), sep = '\t')),
                  max_try,
                  {logg(paste(year, '|', paste('uploaded', basename(file_out), sep = ' '), sep = '\t')); unlink(file_out)}))
	predicts[[as.character(year)]] <- pdict
    rm(tinv)
  }
  rm(cifob_model)
  return(do.call(rbind, predicts))
}

logg(paste('0000', '|', 'cluster started', sep = '\t'))
tbl_yrs <- sdf_copy_to(sc, data.frame(years), repartition = npar)
predicts <- spark_apply(tbl_yrs, dist_codes, packages = c('pkg', 'batchscr'), 
                        context = data.frame(names(cols_pdict), stringsAsFactors = F), 
                        memory = T, name = 'pred_gaps', rdd = T, 
                        columns = append(cols_pdict, c(max_prd = 'numeric', min_prd = 'numeric', sum_prd = 'numeric', cnt_prd = 'integer')), 
                        env = list(out_bucket = out_bucket, oplog = oplog))
summ_prd <- list()
summ_prd[[1]] <- predicts %>% group_by(t, d_dev_i, d_dev_j) %>% summarize(max = max(max_prd), min = min(min_prd), mean = sum(sum_prd)/sum(cnt_prd)) %>% sdf_collect()
summ_prd[[2]] <- predicts %>% group_by(t, d_dev_i) %>% summarize(max = max(max_prd), min = min(min_prd), mean = sum(sum_prd)/sum(cnt_prd)) %>% sdf_collect()
summ_prd[[2]][, 'd_dev_j'] <- NA
summ_prd[[3]] <- predicts %>% group_by(t, d_dev_j) %>% summarize(max = max(max_prd), min = min(min_prd), mean = sum(sum_prd)/sum(cnt_prd)) %>% sdf_collect()
summ_prd[[3]][, 'd_dev_i'] <- NA
summ_prd[[4]] <- predicts %>% group_by(t) %>% summarize(max = max(max_prd), min = min(min_prd), mean = sum(sum_prd)/sum(cnt_prd)) %>% sdf_collect()
summ_prd[[4]][, c('d_dev_i', 'd_dev_j')] <- NA
summ_prd[[5]] <- predicts %>% group_by(d_dev_i, d_dev_j) %>% summarize(max = max(max_prd), min = min(min_prd), mean = sum(sum_prd)/sum(cnt_prd)) %>% sdf_collect()
summ_prd[[5]][, 't'] <- NA
summ_prd[[6]] <- predicts %>% group_by(d_dev_i) %>% summarize(max = max(max_prd), min = min(min_prd), mean = sum(sum_prd)/sum(cnt_prd)) %>% sdf_collect()
summ_prd[[6]][, c('t', 'd_dev_j')] <- NA
summ_prd[[7]] <- predicts %>% group_by(d_dev_j) %>% summarize(max = max(max_prd), min = min(min_prd), mean = sum(sum_prd)/sum(cnt_prd)) %>% sdf_collect()
summ_prd[[7]][, c('t', 'd_dev_i')] <- NA
summ_prd[[8]] <- predicts %>% summarize(max = max(max_prd), min = min(min_prd), mean = sum(sum_prd)/sum(cnt_prd)) %>% sdf_collect()
summ_prd[[8]][, c('t', 'd_dev_i', 'd_dev_j')] <- NA
logg(paste('0000', '|', 'summarized predictions', sep = '\t'))
spark_disconnect(sc)
write.csv(do.call(rbind, summ_prd), outfile, row.names = F)
put_object(oplog, basename(oplog), bucket = out_bucket)
put_object(outfile, basename(outfile), bucket = out_bucket)
