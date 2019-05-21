# normalize legacy HK re-export data 'hkrx.csv' compiled by Joe Spanjers from Hong Kong sources
# the results are uploaded to bucket 'gfi-supplemental'

legacy <- read.csv('hkrx.csv', stringsAsFactors = F, header = T)
legacy$usd_per_hkd <- NULL
colnames(legacy)[8] <- 'vrx_un'
# legacy$k <- sprintf('%06d', legacy$k)
legacy$k <- as.character(legacy$k)
legacy[nchar(legacy$k)<6, 'k'] <- paste('0', legacy[nchar(legacy$k)<6, 'k'], sep = '')
sum(nchar(legacy$k)!=6)
y <- unique(legacy$t)
for(i in y){
  write.csv(legacy[legacy$t == i, ], file = bzfile(paste('data/HK', i, 'rx.csv.bz2', sep = '_')), row.names = F, na='')
}
