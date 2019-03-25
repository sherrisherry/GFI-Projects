pkgs <- c('aws.s3', 'aws.ec2metadata', 'XML', 'remotes')
for(i in pkgs)library(i, character.only = T)
install_github("sherrisherry/GFI-Cloud", subdir="pkg")
usr <- 'aws00' # the user account for using AWS service
keycache <- read.csv('~/vars/accesscodes.csv', header = TRUE, stringsAsFactors = FALSE)
out_bucket <- 'gfi-comtrade'

ec2env(keycache,usr)

download.file('https://comtrade.un.org/ws/refs/getExplanatoryNotes.aspx?so=2', 'data/unct_pubnote.xml')
# download reporting information from comtrade publication notes
# if download fails, check https://comtrade.un.org/db/mr/daExpNoteDetail.aspx?y=all
options(stringsAsFactors = F)
# all cols are in char
df <- xmlToDataFrame(xmlParse('data/unct_pubnote.xml'))
unique(df$partner)
# some items should have white spaces trimmed, sapply returns matrix
df <- as.data.frame(sapply(df, trimws))
df[sapply(df,function(x){x %in% c('', 'N/A')})] <- NA
df <- df[df$flow==1 & df$valuation=='FOB', c("year","reporter")]
colnames(df) <- c('t','i')
obj_nm <- 'data/mfob.csv'
write.csv(df,obj_nm, row.names=F)
put_object(obj_nm, basename(obj_nm), bucket = out_bucket)
