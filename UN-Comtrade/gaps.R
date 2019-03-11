rm(list=ls()) # clean up environment
pkgs <- c('aws.s3', 'aws.ec2metadata', 'scripting', 'cleandata', 'data.table')
for(i in pkgs)library(i, character.only = T)

#=====================================modify the following parameters for each new run==============================================#

usr <- 'aws00' # the user account for using AWS service
years <- 2016:2001 # the years we want to download
out_bucket <- 'gfi-work' # save the results to a S3 bucket called 'gfi-mirror-analysis'
in_bucket <- 'gfi-work' # read in raw data from this bucket
oplog <- 'gaps.log' # progress report file
max_try <- 10 # the maximum number of attempts for a failed process
keycache <- read.csv('~/vars/accesscodes.csv', header = TRUE, stringsAsFactors = FALSE) # the database of our credentials
cty <- NULL # set to NULL to select all countries within GFI's consideration; or exp. c(231, 404, 800)
cifob_model <- 'cifob_model.rds.bz2'
cols_in <- c(rep("integer",3),rep("character",3),rep("numeric",4),rep("integer",5), "numeric", rep("integer",5)) 
names(cols_in) <- c("t","j","i","hs_rpt","hs_ptn","k","v_M","v_X","q_M","q_X","q_code_M","q_code_X","d_fob","d_dev_i","d_dev_j",
                    "distw","d_landlocked_j","d_landlocked_i","d_contig","d_conti","d_rta")
cols_out <- c("t","j","i","hs_rpt","hs_ptn","k","v_rX","v_rM","v_M","v_X","q_M","q_X","q_kg_M","q_kg_X","q_code_M","q_code_X","d_dev_i","d_dev_j",
              'uvmdn', 'v_M_fob', 'a_wt', 'gap', 'gap_wtd')
cols_model <- c('ln_distw', 'ln_distw_squared', 'ln_uvmdn', 'd_contig', 'd_conti', 'd_rta', 'd_landlocked_i', 'd_landlocked_j', 'd_dev_i', 'd_dev_j', 'd_hs_diff')
cols_model <- append(cols_model, paste('d', years[-1], sep = '_'))
#===================================================================================================================================#
				  
oplog <- paste('logs/', oplog, sep = '')
logg <- function(x)mklog(x, path = oplog)
Sys.setenv("AWS_ACCESS_KEY_ID" = keycache$Access_key_ID[keycache$service==usr],
           "AWS_SECRET_ACCESS_KEY" = keycache$Secret_access_key[keycache$service==usr])
if(is.na(Sys.getenv()["AWS_DEFAULT_REGION"]))Sys.setenv("AWS_DEFAULT_REGION" = gsub('.{1}$', '', metadata$availability_zone()))
options(stringsAsFactors= FALSE)
cat('Time\tZone\tYear\tMark\tStatus\n', file = oplog, append = FALSE)

ecycle(save_object(object = file_inout, bucket = in_bucket, file = 'tmp/tmp.csv.bz2', overwrite = TRUE),
       {logg(paste('!', 'retrieving file failed', sep = '\t')); stop()}, max_try)
ecycle(m_in <- read.csv(bzfile('tmp/tmp.csv.bz2'), colClasses=cols_in, na.strings="", header = T),
       {logg(paste('!', 'loading file failed', sep = '\t')); stop()}, max_try)
logg(paste(':', 'loaded data', sep = '\t'))
unlink('tmp/tmp.csv.bz2')
# this also removes countries outside GFI consideration because of d_dev
m_in <- m_in[complete.cases(m_in[, -match(c("v_rX","v_rM"), colnames(m_in))]),]
m_in <- subset(m_in,m_in$q_code_M == m_in$q_code_X)  # just to be sure

m_in$p_M <- m_in$v_M / m_in$q_M
tmp <- aggregate(m_in$p_M,list(m_in$k),median, na.rm=TRUE) ; colnames(tmp) <- c("k","uvmdn")
m_in <- merge(x=m_in,y=tmp,by=c("k"),all.x=TRUE)
m_in$ln_distw <- log(m_in$distw)
m_in$ln_distw_squared <- m_in$ln_distw * m_in$ln_distw
m_in$ln_uvmdn <- log(m_in$uvmdn)
m_in$d_hs_diff <- as.integer(m_in$hs_ptn!=m_in$hs_rpt)
m_in$d <- factor(m_in$t, levels = years); tmp <- encode_onehot(m_in[,'d', drop=FALSE], drop1st = T)
m_in$d <- NULL; m_in <- cbind(m_in, tmp)
logg(paste(':', 'prepared data', sep = '\t'))
ecycle(save_object(object = cifob_model, bucket = in_bucket, file = 'tmp/tmp.csv.bz2', overwrite = TRUE),
		{logg(paste('!', 'retrieving file failed', sep = '\t')); stop()}, max_try)
ecycle(cifob_model <- readRDS(pipe("bzip2 -dkc ./tmp/tmp.csv.bz2")),
		{logg(paste('!', 'loading file failed', sep = '\t')); stop()}, max_try)
logg(paste(':', 'loaded model', sep = '\t'))
unlink('tmp/tmp.csv.bz2')

m_in$v_M_fob <- 0 
m_in[m_in$d_fob == 1,'v_M_fob'] <- m_in[m_in$d_fob == 1,'v_M']
m_in[(m_in$d_fob == 0),"v_M_fob"] <- predict(cifob_model, m_in[m_in$d_fob == 0, cols_model],type='response')
logg(paste(':', 'applied model', sep = '\t'))
rm(cifob_Model)
m_in[(m_in$d_fob == 0),"v_M_fob"] <- exp(m_in[(m_in$d_fob == 0),"v_M_fob"])
m_in[(m_in$d_fob == 0),"v_M_fob"] <- m_in[(m_in$d_fob == 0),"v_M"] / m_in[(m_in$d_fob == 0),"v_M_fob"] 
m_in[(m_in$v_M_fob>m_in$v_M),"v_M_fob"] <- m_in[(m_in$v_M_fob>m_in$v_M),"v_M"]# DEFAULT: when fob>cif set FOB = CIF
#
tmp <- colnames(m_in); colnames(m_in)[match('d', tmp)] <- 't'
# calculate weights & gaps
cat("\n"," ...calculating weights & gaps","\n")
m_in$a_wt <- 1
m_in[(m_in$q_M < m_in$q_X),"a_wt"] <- 1. - ((m_in[(m_in$q_M < m_in$q_X),"q_X"]- m_in[(m_in$q_M < m_in$q_X),"q_M"])/m_in[(m_in$q_M < m_in$q_X),"q_X"])
m_in[(m_in$q_M >= m_in$q_X),"a_wt"] <- 1. - ((m_in[(m_in$q_M >= m_in$q_X),"q_M"]- m_in[(m_in$q_M >= m_in$q_X),"q_X"])/m_in[(m_in$q_M >= m_in$q_X),"q_M"])
m_in$gap <- m_in$v_M_fob - m_in$v_X
m_in$gap_wtd <- m_in$gap * m_in$a_wt
logg(paste(':', 'weighted gaps', sep = '\t'))
m_in <- m_in[, cols_out]
cat("\n","   ...writing output","\n")
file_inout <- paste('tmp/', basename(file_inout), '.csv.bz2', sep = '')
ecycle(write.csv(m_in, file = bzfile(file_inout),row.names=FALSE,na=""), 
       ecycle(s3write_using(m_in, FUN = function(x, y)write.csv(x, file=bzfile(y), row.names = FALSE),
                            bucket = out_bucket, object = basename(file_inout)),
              logg(paste(year, '!', paste('uploading', basename(file_inout), 'failed', sep = ' '), sep = '\t')), max_try), 
       max_try,
       ecycle(put_object(file_inout, basename(file_inout), bucket = out_bucket), 
              logg(paste(year, '!', paste('uploading', basename(file_inout), 'failed', sep = ' '), sep = '\t')),
              max_try,
              {logg(paste(year, '|', paste('uploaded', basename(file_inout), sep = ' '), sep = '\t')); unlink(file_inout)}))

put_object(oplog, basename(oplog), bucket = out_bucket)
