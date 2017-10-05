require(data.table)

getSavedData <- function () {
	#uncrunched data saved with saveRDS(data, file="data.rds")
	cat("Loading dataframe...")
	data <- readRDS("data.rds")
	cat("\u2713\n")
	cat("Crunching data...")
	data <- crunchData(data)
	cat("\u2713\n")
	cat("Result:\n")
	summaryData(data)
	return (data)
}

crunchData <- function(dataframe) {
	data <- as.data.table(dataframe) #Create data.table

	#convert data types
	data[, budget:=as.numeric(budget)]
	data[, bid:=as.numeric(bid)]
	data[, reach_estimate:=as.numeric(reach_estimate)]
	data[, spend:=as.numeric(spend)]
	data[, impressions:=as.numeric(impressions)]
	data[, clicks:=as.numeric(clicks)]
	data[, conversions:=as.numeric(conversions)]

	#Filter data
	data <- data[spend >= 100] #Filter out rows that have spend less than 100
	data <- data[bid > 0] #Filter out rows that have zero/NA in bid
	setkey(data, group_id, date, spend)
	data <- unique(data) #Remove duplicate adsets with still unique ids (some random fuckery in db)
	data[, id:=paste(group_id, adset_id, bid, sep="")] #Create unique ids for adsets having different bids
	setkey(data, id, date)
	data <- unique(data) # Remove all duplicate keys (some of the data is messed up and different ad sets have same ids)
	data[, days_of_data:=.N, by=id]
	data <- data[days_of_data >= 30] # Exclude ad sets with less than 30 days of data
	data[, adsets_in_campaign:=.N, by=.(group_id, date)]
	data <- data[adsets_in_campaign > 1] #Filter out day rows when campaign has only one ad set
	data <- data[adsets_in_campaign < 100] #Filter out day rows when campaign has over 100 ad sets
	data[, days_of_data_campaign:=length(unique(date)), by=.(group_id)]
	data <- data[days_of_data_campaign >= 30] # Exclude campaigns with less than 30 days of data

	#add conversion rates
	data[, impressions_cr:=impressions/spend]
	data[, clicks_cr:=clicks/spend]
	data[, conversions_cr:=conversions/spend]

	return (data)
}

summaryData <- function(dataTable) {		
	cat("Rows: ")		
	cat(dataTable[, .N])		
	cat("\n")		
		
	cat("Campaigns: ")		
	cat(dataTable[, uniqueN(group_id)])		
	cat("\n")		
		
	cat("Adsets: ")		
	cat(dataTable[, uniqueN(id)])		
	cat("\n")		
 }