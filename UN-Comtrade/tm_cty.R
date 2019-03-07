cty <- c()
cols_in <- c(rep('integer', 5),'character',rep('NULL',11), rep('numeric',6))
names(cols_in) <- c("t","j","i","d_dev_i","d_dev_j","k","hs_rpt","hs_ptn","v_rX","v_rM","q_M","q_X","q_kg_M","q_kg_X",
                    "q_code_M","q_code_X",'uvmdn',"v_M","v_X", 'v_M_fob', 'a_wt', 'gap', 'gap_wtd')
options(stringsAsFactors= FALSE)
aggk <- read.csv('data/M-Gaps.csv', colClasses = cols_in)
tmp <- subset(tmp, tmp$i %in% countries | tmp$j %in% countries)
