rm(list=ls()) # clean up environment
pkgs <- c('aws.s3', 'aws.ec2metadata', 'stats', 'scripting')
for(i in pkgs)library(i, character.only = T)

#=====================================modify the following parameters for each new run==============================================#

usr <- 'aws00' # the user account for using AWS service
dates <- 2016:2001 # the years we want to download
out_bucket <- 'gfi-mirror-analysis' # save the results to a S3 bucket called 'gfi-mirror-analysis'
in_bucket <- 'gfi-mirror-analysis' # read in raw data from this bucket
sup_bucket <- 'gfi-supplemental' # supplemental files
tag <- "Comtrade"
oplog <- 'tm_cty.log' # progress report file
max_try <- 10 # the maximum number of attempts for a failed process
keycache <- read.csv('~/vars/accesscodes.csv', header = TRUE, stringsAsFactors = FALSE) # the database of our credentials

cty <- c()
cols_in <- c(rep('integer', 6),'character', rep('numeric',7))
names(cols_in) <- c("t","j","i","q_code_M","d_dev_i","d_dev_j","k",
                    "v_M","v_X","q_M","q_X",'v_M_fob', 'a_wt', 'gap_wtd')
cols_out <- c('t',"k","i","j","v_M","v_X","q_M","q_X","q_code_M","q_code_X", 'ln_distw', 'ln_distw_squared', 'ln_uvmdn',
              'd_fob', 'd_contig', 'd_conti', 'd_rta', 'd_landlocked_i', 'd_landlocked_j', 'd_dev_i', 'd_dev_j', 'd_hs_diff')

#===================================================================================================================================#

oplog <- paste('logs/', oplog, sep = '')
logg <- function(x)mklog(x, path = oplog)
Sys.setenv("AWS_ACCESS_KEY_ID" = keycache$Access_key_ID[keycache$service==usr],
           "AWS_SECRET_ACCESS_KEY" = keycache$Secret_access_key[keycache$service==usr])
if(is.na(Sys.getenv()["AWS_DEFAULT_REGION"]))Sys.setenv("AWS_DEFAULT_REGION" = gsub('.{1}$', '', metadata$availability_zone()))
options(stringsAsFactors= FALSE)
cat('Time\tZone\tYear\tMark\tStatus\n', file = oplog, append = FALSE)

options(stringsAsFactors= FALSE)
aggk <- read.csv('data/M-Gaps.csv', colClasses = cols_in)
input <- subset(input, input$i %in% cty | input$j %in% cty)
