library("RODBC", lib.loc="C:/Program Files/R/R-3.0.2/library")
scratch <- odbcConnect("Dscratch")
griffin <- odbcConnect("Dgriffin")
reporting <- odbcConnect("Dreporting")
msr <- odbcConnect("MSR_analytics")

# PUll order level data 
OL <- sqlFetch(msr,'dbo.MSR_OL_1_6221') 
VL <- sqlFetch(msr,'dbo.MSR_VL_1_6221') 
str(OL)

# Pull only orders that were placed within certain X day from first touch point 
OL1 <- subset(OL, days_from_touch < 6)

# Summarize the information by visitor id . Takes a lot of time (3 mins for 400K orders). So think about having group by in sql. 
OLS <- ddply(OL1, c("Test_type_id","visitor_id","shopper_key","test_id","test_sub_id"), function(df)c(max(df$order_count),sum(df$booking_usd), sum(df$GM_usd)))

# Pull the group by using sql query. Really fast. Takes < 10s. So explore if combining tables in SQL is faster than doing it in R
feature_id = 6221
Test_type_id = 1 
days_to_consider = 7 
table_name = c("dbo.MSR_OL",toString(Test_type_id),toString(feature_id))
table_name <- paste(table_name, collapse ="_")
SubQ <- paste("select Test_type_id, visitor_id, shopper_key, test_id, test_sub_id, control_test, Test_name, test_sub_name ,max(order_count) as order_count, sum(booking_usd) as booking_usd, sum(GM_usd) as GM_usd from", table_name," where days_from_touch <= ",toString(days_to_consider), " group by Test_type_id, visitor_id, shopper_key, test_id, test_sub_id, control_test, Test_name, test_sub_name")
OLS2 <- sqlQuery(msr,SubQ)

# Flat outliers and cap the bookings 
## Outlier % should be passed in the function , but for time i will just use some number 
quantiles <- quantile( OLS2$booking_usd, c(.001, .99 ) )
OLS2$booking_usd[OLS2$booking_usd > quantiles[2]] <- quantiles[2]

# Merge Visitor table and order capped table
# Ok, need to remove few fields from OLS2, so we dont have repeat columns in CL 
CL <- merge(x= VL,y = OLS2, by = "visitor_id", all.x = TRUE)

# Pull CR % for different feature id. Need to do this in a loop for each feature id or may be there is another way. 
## for the time being see what you can do ignoring fv_id 
str(CL)

## Pull only control Fv_id 
CL1 <- subset(CL, Test_sub_id = 25241)

## Can i summarize certain metrics for the control fv_id 
CL3 <- data.frame(Control_count = c(dim(CL1)[1])
                  , Control_ordering_visitor = c(sum(CL1$order_count[!is.na(CL1$order_count)] > 0))
                  , Control_AOV = c(mean(CL1$booking_usd[!is.na(CL1$booking_usd)]))
                  , Control_AOV_var = c(var(CL1$booking_usd[!is.na(CL1$booking_usd)]))
                  )
CL3


# can I summarize without a loop approach. Yes, this works!! Time to go home. 
CL4 <- ddply(CL, c("Test_sub_id"), function(df)c(Visitor_count = sum(df$visitor_id >0),Ordering_visitor_count = sum(df$order_count[!is.na(df$order_count)] > 0),AOV = mean(df$booking_usd[!is.na(df$booking_usd)]) ,booking_var = var(df$booking_usd[!is.na(df$booking_usd)]) ))
CL4

# Now to create a loop - Abandoning this approach, as there is a way to pull what i want without using loops
C <- unique(CL$Test_sub_id[CL$control_test.x == 'C']) # Pulls distinct control feature value id 
T <- unique(CL$Test_sub_id[CL$control_test.x == 'T']) # pulls distinct test feature value id 




# How to write into table 
x <- data.frame(foo=1:4,bar=c(T,T,T,F))
sqlSave(scratch, x, "DEV_table_from_R2", rownames = FALSE, verbose=T, fast=T, append=T)