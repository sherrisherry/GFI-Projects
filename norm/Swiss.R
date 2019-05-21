a <- read.csv('Swiss_trade_m.csv', stringsAsFactors = F)
b <- read.csv('Swiss_trade_x.csv', stringsAsFactors = F)
unique(a$k)
unique(b$k)
colnames(a) <- sub('_M', '', colnames(a))
colnames(b) <- sub('_X', '', colnames(b))
unique(a$i)
unique(b$i)
colnames(a)[1]<-'mx'
colnames(b)[1]<-'mx'
a$mx <- 'm'
b$mx <- 'x'
c <- rbind(a,b)
summary(c)
write.csv(c, 'Swiss_mx_71-72.csv', row.names = F)
