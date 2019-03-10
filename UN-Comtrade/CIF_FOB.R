# build model to estimate gap between CIF and FOB
# produce cif-fob model coefficients: Margins Model Coefficients.csv

rm(list=ls()) # clean up environment
pkgs <- c('aws.s3', 'aws.ec2metadata', 'stats', 'scripting', 'cleandata')
for(i in pkgs)library(i, character.only = T)

#=====================================modify the following parameters for each new run==============================================#

usr <- 'aws00' # the user account for using AWS service
years <- 2016:2001 # the years we want to download
out_bucket <- 'gfi-work' # save the results to a S3 bucket called 'gfi-mirror-analysis'
in_bucket <- 'gfi-mirror-analysis' # read in raw data from this bucket
sup_bucket <- 'gfi-supplemental' # supplemental files
oplog <- 'CIF_FOB.log' # progress report file
max_try <- 10 # the maximum number of attempts for a failed process
keycache <- read.csv('~/vars/accesscodes.csv', header = TRUE, stringsAsFactors = FALSE) # the database of our credentials
# parameter spex
q_crit    <- 0.025  # threshold for quantity-based exclusions
p_crit_hi <- 1.75   # upper threshhold for price-based exclusions
p_crit_lo <- 0.75   # lower threshhold for price-based exclusions
#
cols_in <- c(rep("integer",3),rep("character",3),rep("NULL",2),rep("numeric",6),rep("integer",5), "numeric", rep("integer",5)) 
names(cols_in) <- c("t","j","i","hs_rpt","hs_ptn","k","v_rX","v_rM","v_M","v_X","q_M","q_X","q_kg_M","q_kg_X","q_code_M","q_code_X","d_fob","d_dev_i","d_dev_j",
                    "distw","d_landlocked_j","d_landlocked_i","d_contig","d_conti","d_rta")
cols_model <- c('ln_v_margin', 'ln_distw', 'ln_distw_squared', 'ln_uvmdn', 'd_contig', 'd_conti', 'd_rta', 'd_landlocked_i', 'd_landlocked_j', 'd_dev_i', 'd_dev_j', 'd_hs_diff')
cols_model <- append(cols_model, paste('d', years[-1], sep = '_'))
#===================================================================================================================================#
				  
oplog <- paste('logs/', oplog, sep = '')
logg <- function(x)mklog(x, path = oplog)
Sys.setenv("AWS_ACCESS_KEY_ID" = keycache$Access_key_ID[keycache$service==usr],
           "AWS_SECRET_ACCESS_KEY" = keycache$Secret_access_key[keycache$service==usr])
if(is.na(Sys.getenv()["AWS_DEFAULT_REGION"]))Sys.setenv("AWS_DEFAULT_REGION" = gsub('.{1}$', '', metadata$availability_zone()))
options(stringsAsFactors= FALSE)
cat('Time\tZone\tYear\tMark\tStatus\n', file = oplog, append = FALSE)

row_count <- data.frame(Year = years); row_count$raw <- NA; row_count$nofob <- NA; row_count$nona <- NA; row_count$inlim <- NA

model_train <- list()
for (year in years) {
    obj_nm <- paste("Comtrade",year,"input.csv.bz2",sep="-")
    cat("\n","...reading input for",year,"\n")
ecycle(save_object(object = obj_nm, bucket = in_bucket, file = 'tmp/tmp.csv.bz2', overwrite = TRUE),
		   {logg(paste(year, '!', 'retrieving file failed', sep = '\t')); next}, max_try)
ecycle(x_in <- read.csv(pipe("bzip2 -dkc ./tmp/tmp.csv.bz2"), header=T, colClasses = cols_in),
		   {logg(paste(year, '!', 'loading file failed', sep = '\t')); next}, max_try,
		   cond = is.data.frame(x_in))
	logg(paste(year, ':', 'opened', sep = '\t'))
	unlink('tmp/tmp.csv.bz2')
	row_count$raw[row_count$Year==year] <- nrow(x_in)
    # exclude :(1) records with different quantity units (should be none)
    #          (2) records where the differences in quantities exceeds q_crit
    #          (3) the ratio between import unit values and export unit values exceeds 2 or is less than 1
    cat("   ...processing...")
    x_in <- subset(x_in, d_fob == 0, -match('d_fob',colnames(x_in))) # this handles NAs
    row_count$nofob[row_count$Year==year] <- nrow(x_in)
    x_in <- x_in[complete.cases(x_in),]
    row_count$nona[row_count$Year==year] <- nrow(x_in)
    x_in <- subset(x_in,x_in$q_code_M == x_in$q_code_X)
    x_in <- subset(x_in,(abs(x_in$q_M - x_in$q_X) / x_in$q_M) < q_crit)
    x_in$p_M <- x_in$v_M / x_in$q_M
    x_in$p_X <- x_in$v_X / x_in$q_X
    x_in <- subset(x_in, (x_in$p_M / x_in$p_X) < p_crit_hi)
    x_in <- subset(x_in, (x_in$p_M / x_in$p_X) > p_crit_lo)
    logg(paste(year, ':', 'subseted', sep = '\t'))
    tmp <- aggregate(x_in$p_M,list(x_in$k),median, na.rm=TRUE) ; colnames(tmp) <- c("k","uvmdn")
    x_in <- merge(x=x_in,y=tmp,by=c("k"),all.x=TRUE)
    row_count$inlim[row_count$Year==year] <- nrow(x_in)
    x_in$ln_v_margin <- log(x_in$v_M/x_in$v_X)
    x_in$ln_distw <- log(x_in$distw)
    x_in$ln_distw_squared <- x_in$ln_distw * x_in$ln_distw
    x_in$ln_uvmdn <- log(x_in$uvmdn)
    x_in$d_hs_diff <- as.integer(x_in$hs_ptn!=x_in$hs_rpt)
    x_in$d <- factor(x_in$t, levels = years); tmp <- encode_onehot(x_in[,'d', drop=FALSE], drop1st = T)
    x_in$d <- NULL; x_in <- cbind(x_in, tmp)
    x_in <- x_in[, cols_model]
    # this eleminates countries outside GFI focus because of NAs in d_dev
	model_train[[as.character(year)]] <- na.omit(x_in)
	logg(paste(year, '|', 'processed', sep = '\t'))
}

rm(x_in)

model_train <- do.call('rbind', model_train)

bak <- 'data/process_cifob_train.csv'
ecycle(write.csv(row_count, file = bzfile(bak),row.names=FALSE,na=""), 
       ecycle(s3write_using(row_count, FUN = function(x, y)write.csv(x, file=bzfile(y), row.names = FALSE),
                            bucket = out_bucket, object = basename(bak)),
              logg(paste('0000', '!', 'uploading train set process failed', sep = '\t')), max_try), 
       max_try,
       ecycle(put_object(bak, basename(bak), bucket = out_bucket), 
              logg(paste('0000', '!', 'uploading train set process failed', sep = '\t')),
              max_try,
              logg(paste('0000', '|', 'uploaded train set process', sep = '\t'))))

rm(row_count)

# start modeling
model_lm <- paste(cols_model[1], paste(cols_model[-1], collapse = '+'), sep = '~')
# model_lm: ln_v_margin ~ ln_distw + ln_distw_squared + ln_uvmdn + d_contig + d_conti + d_rta + d_landlocked_i + d_landlocked_j
#                         + d_dev_i + d_dev_j + d_hs_diff + d_2001 + ... + d_2016
cifob_model <- lm(formula = model_lm, data= model_train)
logg(paste('0000', ':', 'trained model', sep = '\t'))
bak <- 'data/cifob_model.rds.bz2'
ecycle(saveRDS(cifob_model, bzfile(bak), ascii = F), 
       ecycle(s3write_using(cifob_model, FUN = function(x, y)saveRDS(x, file=bzfile(y), ascii = F, compress = T),
                            bucket = out_bucket, object = basename(bak)),
              logg(paste('0000', '!', 'uploading margins_model failed', sep = '\t')), max_try), 
       max_try,
       ecycle(put_object(bak, basename(bak), bucket = out_bucket), 
              logg(paste('0000', '!', 'uploading margins_model failed', sep = '\t')),
              max_try,
              {logg(paste('0000', '|', 'uploaded margins_model', sep = '\t')); unlink(bak)}))
cifob_fitted <- exp(cifob_model$fitted.values)
capture.output(summary(cifob_fitted), file= "data/Stats_Fitted_Values.txt")
capture.output(summary(cifob_model), file= "data/cifob_Regression_Summary.txt")
# collect coefficients
model_row_names <- c("Intercept", cols_model[-1])
model_coeffs <- data.frame(Coefficients = rep(NA, length(model_row_names)))
rownames(model_coeffs)<-model_row_names
model_coeffs[,"Coefficients"] <- coef(margins_model)
write.csv(model_coeffs,file="data/cifob_Model_Coefficients.csv")
logg(paste('0000', '|', 'saved stats', sep = '\t'))

put_object(oplog, basename(oplog), bucket = out_bucket)

rm(list=ls())
