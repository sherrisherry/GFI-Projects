# build model to estimate gap between CIF and FOB
# produce cif-fob model coefficients: Margins Model Coefficients.csv

rm(list=ls()) # clean up environment
pkgs <- c('aws.s3', 'aws.ec2metadata', 'stats', 'batchscr', 'cleandata', 'remotes')
for(i in pkgs)library(i, character.only = T)
install_github("sherrisherry/GFI-Cloud", subdir="pkg"); library(pkg)

#=====================================modify the following parameters for each new run==============================================#

usr <- 'aws00' # the user account for using AWS service
years <- 2016:2001 # the years we want to download
out_bucket <- 'gfi-work' # save the results to this S3 bucket
in_dir <- '/efs/unct' # read in raw data from this bucket
oplog <- 'cifob_model.log' # progress report file
max_try <- 10 # the maximum number of attempts for a failed process
tag <- "Comtrade"
keycache <- read.csv('~/vars/accesscodes.csv', header = TRUE, stringsAsFactors = FALSE) # the database of our credentials
# parameter spex
q_crit    <- 0.025  # threshold for quantity-based exclusions
p_crit_hi <- 1.75   # upper threshhold for price-based exclusions
p_crit_lo <- 0.75   # lower threshhold for price-based exclusions
#
cols_in <- c('character', rep("integer",3),rep("numeric",7),rep("integer",11)) 
names(cols_in) <- c('k','t',"i","j","v_M","v_X","q_M","q_X", 'ln_distw', 'ln_distw_squared', 'ln_uvmdn', "q_code_M","q_code_X",
                    'd_fob', 'd_contig', 'd_conti', 'd_rta', 'd_landlocked_i', 'd_landlocked_j', 'd_dev_i', 'd_dev_j', 'd_hs_diff')
cols_model <- c('ln_v_margin', 'ln_distw', 'ln_distw_squared', 'ln_uvmdn', 'd_contig', 'd_conti', 'd_rta', 'd_landlocked_i', 'd_landlocked_j', 'd_dev_i', 'd_dev_j', 'd_hs_diff')
cols_model <- append(cols_model, paste('d', years[-1], sep = '_'))
#===================================================================================================================================#
				  
oplog <- file.path('logs', oplog)
logg <- function(x)mklog(x, path = oplog)
ec2env(keycache,usr)
options(stringsAsFactors= FALSE)
cat('Time\tZone\tYear\tMark\tStatus\n', file = oplog, append = FALSE)

model_train <- list()
for (year in years) {
  ecycle(in_x <- read.csv(bzfile(file.path(in_dir, paste(tag, year,"input.csv.bz2",sep="-"))), header=T, colClasses = cols_in),
         {logg(paste(year, '!', 'loading file failed', sep = '\t')); next}, max_try,
         cond = is.data.frame(in_x))
	logg(paste(year, '#', paste('opened', nrow(in_x), sep = ':'), sep = '\t'))
    # exclude :(1) records with different quantity units (should be none)
    #          (2) records where the differences in quantities exceeds q_crit
    #          (3) the ratio between import unit values and export unit values exceeds 2 or is less than 1
    in_x <- subset(in_x, d_fob == 0, -match('d_fob',colnames(in_x))) # this handles NAs
    logg(paste(year, '#', paste('m_cif', nrow(in_x), sep = ':'), sep = '\t'))
    in_x <- subset(in_x,(abs(in_x$q_M - in_x$q_X) / in_x$q_M) < q_crit)
    in_x$p_M <- in_x$v_M / in_x$q_M
    in_x$p_X <- in_x$v_X / in_x$q_X
    in_x <- subset(in_x, (in_x$p_M / in_x$p_X) < p_crit_hi)
    in_x <- subset(in_x, (in_x$p_M / in_x$p_X) > p_crit_lo)
    logg(paste(year, '#', paste('inlim', nrow(in_x), sep = ':'), sep = '\t'))
    in_x$ln_v_margin <- log(in_x$v_M/in_x$v_X) # target
    in_x$d <- factor(in_x$t, levels = years); tmp <- encode_onehot(in_x[,'d', drop=FALSE], drop1st = T)
    in_x$d <- NULL; in_x <- cbind(in_x, tmp)
	model_train[[as.character(year)]] <- in_x[, cols_model]
	logg(paste(year, '|', 'processed', sep = '\t'))
}

rm(in_x)

model_train <- do.call('rbind', model_train)

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
model_coeffs[,"Coefficients"] <- coef(cifob_model)
write.csv(model_coeffs,file="data/cifob_Model_Coefficients.csv")
logg(paste('0000', '|', 'saved stats', sep = '\t'))

put_object(oplog, basename(oplog), bucket = out_bucket)

rm(list=ls())
