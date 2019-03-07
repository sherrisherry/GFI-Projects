# 10 Year tm
cols_in <- c(rep('integer', 5),'character',rep('NULL',11), rep('numeric',6))
names(cols_in) <- c("t","j","i","d_dev_i","d_dev_j","k","hs_rpt","hs_ptn","v_rX","v_rM","q_M","q_X","q_kg_M","q_kg_X",
             "q_code_M","q_code_X",'uvmdn',"v_M","v_X", 'v_M_fob', 'a_wt', 'gap', 'gap_wtd')
options(stringsAsFactors= FALSE)
aggk <- read.csv('data/M-Gaps.csv', colClasses = cols_in)
aggk <- aggk[aggk$d_dev_i+aggk$d_dev_j<2 & aggk$d_dev_i+aggk$d_dev_j>0, ]
aggk$factor <- ifelse(aggk$gap_wtd > 0, 'p', 'n'); aggk$factor[aggk$gap_wtd==0] <- 'x'
aggk <- aggregate(aggk[,c('v_M_fob','v_X','gap_wtd')],
                 list(aggk$t,aggk$i,aggk$j,aggk$d_dev_i,aggk$factor),sum, na.rm=TRUE)
colnames(aggk) <- c('t','i','j','mx','f','v_i','v_j','gap_wtd')
aggk$mx <- ifelse(aggk$mx==1, 'm', 'x')
aggk <- split(aggk, aggk$mx)
tmp <- c(p='mo', n='mu', x='ng'); aggk$m$f <- tmp[aggk$m$f]
tmp <- c(p='xu', n='xo', x='ng'); aggk$x$f <- tmp[aggk$x$f]
tmp <- colnames(aggk$x)
colnames(aggk$x)[match(c('i','j','v_i','v_j'),tmp)] <- c('j','i','v_j','v_i')
aggk <- do.call(rbind, aggk)
write.csv(aggk,file='data/flow_agged_k.csv',row.names=F)

# M <- tmp[tmp$d_dev_i==1, ]
# 
# colnames(M) <- c('t','i','j','v_i','v_j','gap_wtd')
# X <- tmp[tmp$d_dev_j==1, ]
# X <- aggregate(X[,c('v_M_fob','v_X','gap_wtd')],list(X$t,X$i,X$j),sum, na.rm=TRUE)
# colnames(X) <- c('t','j','i','v_j','v_i','gap_wtd')
# write.csv(M,file='data/M_agg.csv',row.names=F)
