cols_eia <- rep("integer",4)
names(cols_eia) <- c("t","i","j","value")
eia <- read.csv('EIA.csv', stringsAsFactors = F, colClasses = cols_eia)
colnames(eia)[4] <- 'd_rta'
sum(!duplicated(eia[eia$t %in% 2000:2016,2:3])) # total 37442 pairs of countries; this dataset changes very slowly as no change in 17 years.
sum(!duplicated(eia[eia$t %in% 1992:2016,2:3])) # last change happened in 1993
write.csv(eia[eia$t<2013,], 'EIA.csv', row.names = F) # adjust to original available years
