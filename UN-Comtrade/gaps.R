rm(list=ls()) # clean up environment
pkgs <- c('aws.s3', 'aws.ec2metadata', 'scripting', 'cleandata', 'remotes')
for(i in pkgs)library(i, character.only = T)
install_github("sherrisherry/GFI-Cloud", subdir="pkg")

#=====================================modify the following parameters for each new run==============================================#

usr <- 'aws00' # the user account for using AWS service
years <- 2016:2001 # the years we want to download
yrs_model <- 2016:2001 # the years for cifob_model
out_bucket <- 'gfi-work' # save the results to this S3 bucket
in_bucket <- 'gfi-work' # read in raw data from this bucket
sup_bucket <- 'gfi-supplemental' # supplemental files
oplog <- 'gaps.log' # progress report file
max_try <- 10 # the maximum number of attempts for a failed process
keycache <- read.csv('~/vars/accesscodes.csv', header = TRUE, stringsAsFactors = FALSE) # the database of our credentials
cty <- NULL # set to NULL to select all countries within GFI's consideration; or exp. c(231, 404, 800)
cifob_model <- 'cifob_model.rds.bz2'
tag <- "Comtrade"
cols_in <- c('character', rep("integer",3),rep("numeric",7),rep("integer",11)) 
names(cols_in) <- c('k','t',"i","j","v_M","v_X","q_M","q_X", 'ln_distw', 'ln_distw_squared', 'ln_uvmdn', "q_code_M","q_code_X",
                    'd_fob', 'd_contig', 'd_conti', 'd_rta', 'd_landlocked_i', 'd_landlocked_j', 'd_dev_i', 'd_dev_j', 'd_hs_diff')
cols_out <- c("t","j","i","k","v_M","v_X","q_M","q_X","q_code_M","d_dev_i","d_dev_j", 'v_M_fob', 'a_wt', 'gap_wtd')
cols_model <- c('ln_distw', 'ln_distw_squared', 'ln_uvmdn', 'd_contig', 'd_conti', 'd_rta', 'd_landlocked_i', 'd_landlocked_j', 'd_dev_i', 'd_dev_j', 'd_hs_diff')
cols_model <- append(cols_model, paste('d', years[-1], sep = '_'))
#===================================================================================================================================#
				  
oplog <- file.path('logs', oplog)
logg <- function(x)mklog(x, path = oplog)
ec2env(keycache,usr)
options(stringsAsFactors= FALSE)
cat('Time\tZone\tYear\tMark\tStatus\n', file = oplog, append = FALSE)

ecycle(save_object(object = cifob_model, bucket = out_bucket, file = 'tmp/tmp.csv.bz2', overwrite = TRUE),
       {logg(paste('0000','!', 'retrieving file failed', sep = '\t')); stop()}, max_try)
ecycle(cifob_model <- readRDS(pipe("bzip2 -dkc ./tmp/tmp.csv.bz2")),
       {logg(paste('0000','!', 'loading file failed', sep = '\t')); stop()}, max_try)
logg(paste('0000',':', 'loaded model', sep = '\t'))
unlink('tmp/tmp.csv.bz2')

if(is.null(cty))cty <- gfi_cty(logf = logg)

predicts <- list()
for(year in years){
  ecycle(save_object(object = paste(tag, year,"input.csv.bz2",sep="-"), bucket = in_bucket, file = 'tmp/tmp.csv.bz2', overwrite = TRUE),
         {logg(paste(year, '!', 'retrieving file failed', sep = '\t')); stop()}, max_try)
  ecycle(m_in <- read.csv(bzfile('tmp/tmp.csv.bz2'), colClasses=cols_in, na.strings="", header = T),
         {logg(paste(year, '!', 'loading file failed', sep = '\t')); stop()}, max_try)
  logg(paste(year, ':', 'loaded data', sep = '\t'))
  unlink('tmp/tmp.csv.bz2')
  m_in <- subset(m_in, m_in$i %in% cty | m_in$j %in% cty) # subset cty
  logg(paste(year, ':', 'sub_cty', sep = '\t'))
  logg(paste(year, '#', nrow(m_in), sep = '\t'))
  m_in$d <- factor(m_in$t, levels = yrs_model); tmp <- encode_onehot(m_in[,'d', drop=FALSE], drop1st = T)
  m_in$d <- NULL; m_in <- cbind(m_in, tmp)
  logg(paste(year, ':', 'prepared data', sep = '\t'))
  m_in$v_M_fob <- 0 
  m_in[m_in$d_fob == 1,'v_M_fob'] <- m_in[m_in$d_fob == 1,'v_M']
  pred <- predict(cifob_model, m_in[m_in$d_fob == 0, cols_model],type='response')
  logg(paste(year, ':', 'applied model', sep = '\t'))
  pred <- exp(pred)
  m_in[(m_in$d_fob == 0),"v_M_fob"] <- m_in[(m_in$d_fob == 0),"v_M"] / pred 
  m_in[(m_in$v_M_fob>m_in$v_M),"v_M_fob"] <- m_in[(m_in$v_M_fob>m_in$v_M),"v_M"]# DEFAULT: when fob>cif set FOB = CIF
  # calculate weights & gaps
  cat("\n"," ...calculating weights & gaps","\n")
  m_in$a_wt <- 1
  m_in[(m_in$q_M < m_in$q_X),"a_wt"] <- 1. - ((m_in[(m_in$q_M < m_in$q_X),"q_X"]- m_in[(m_in$q_M < m_in$q_X),"q_M"])/m_in[(m_in$q_M < m_in$q_X),"q_X"])
  m_in[(m_in$q_M >= m_in$q_X),"a_wt"] <- 1. - ((m_in[(m_in$q_M >= m_in$q_X),"q_M"]- m_in[(m_in$q_M >= m_in$q_X),"q_X"])/m_in[(m_in$q_M >= m_in$q_X),"q_M"])
  m_in$gap <- m_in$v_M_fob - m_in$v_X
  m_in$gap_wtd <- m_in$gap * m_in$a_wt
  logg(paste(year, ':', 'weighted gaps', sep = '\t'))
  file_out <- paste('tmp/', paste(tag, year, 'gaps.csv.bz2', sep = '-'), sep = '')
  ecycle(write.csv(m_in[, cols_out], file = bzfile(file_out),row.names=FALSE,na=""), 
         ecycle(s3write_using(m_in[, cols_out], FUN = function(x, y)write.csv(x, file=bzfile(y), row.names = FALSE),
                              bucket = out_bucket, object = basename(file_out)),
                logg(paste(year, '!', paste('uploading', basename(file_out), 'failed', sep = ' '), sep = '\t')), max_try), 
         max_try,
         ecycle(put_object(file_out, basename(file_out), bucket = out_bucket), 
                logg(paste(year, '!', paste('uploading', basename(file_out), 'failed', sep = ' '), sep = '\t')),
                max_try,
                {logg(paste(year, '|', paste('uploaded', basename(file_out), sep = ' '), sep = '\t')); unlink(file_out)}))
  predicts[[year]] <- pred
  rm(m_in)
}
rm(cifob_model)
predicts <- unlist(predicts)
capture.output(summary(predicts), file= "data/Stats_Fitted_Exp_Values_Full.txt")
logg(paste('0000', '|', 'summarized predictions', sep = '\t'))
put_object(oplog, basename(oplog), bucket = out_bucket)
