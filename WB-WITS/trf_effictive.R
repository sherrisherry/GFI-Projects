options(stringsAsFactors = FALSE)
trains <- read.csv('data/train.csv'); wto <- read.csv('data/wto.csv')
cols <- c("Trade.Year","Reporter","Partner","Product","Trade.Source","Weighted.Average")
names(cols) <- c('t', 'wb_code_i', 'wb_code_j', 'k', 'source', 'trf_wtd')
trains <- trains[, cols]; wto <- wto[, cols]
trains <- na.omit(trains); wto <- na.omit(wto)
trf <- merge(x=trains, y=wto, by=c("Trade.Year","Reporter","Partner","Product"))
if(sum(trf$Weighted.Average.x!=trf$Weighted.Average.y)!=0)stop('overlap not equal')
trf <- rbind(trains, wto)
trf <- trf[!duplicated(trf[, -match('Trade.Source', colnames(trf))]), ]
colnames(trf) <- names(cols)
write.csv(trf, 'data/trf.csv', row.names = F)
