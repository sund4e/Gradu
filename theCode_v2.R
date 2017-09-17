# From terminal run ssh -L 63333:postgresql4.smartly.io:5432 suvi@app99.smartly.io
# where 63333 is the port you want to use for the tunnel & 5432 is the port server listens to, and 10.125.0.46 is the host
# -L tells that you do local port forwarding, i.e. forward the trafic from your local port 63333 to 10.125.0.46:5432
# dev.smartly.io is the ssh host you use to connect

require("RPostgreSQL")
require(data.table)
require(ggplot2)
require(plyr)
require(dplyr)
require(rmongodb)
require(rlist)
require(rbenchmark)

getData <- function(){
	# create a connection 
	# loads the PostgreSQL driver
	drv <- dbDriver("PostgreSQL")

	# creates a connection to the postgres database
	# note that "con" will be used later in each connection to the database
	con <- dbConnect(drv, dbname = "smartly_production",
	                 host = "localhost", port = 63333,
	                 user = "bi", password = "")

	#Save the end and start days of months
	dates.start <- seq(as.Date("2016-03-01"),length=10,by="months")
	dates.end <- seq(as.Date("2016-04-01"),length=10,by="months")-1

	print("getting data")
	# get the data
	for (month in 1:length(dates.start)) {
		start <- dates.start[month]
		end <- dates.end[month]
		cat(paste("Dates", start, "-", end, sep=" "))
		data.month <- dbGetQuery(con, statement = paste(
			"SELECT 
		    DATE,
		    group_id,
		    adset->>'id' adset_id,
		    adset->>'budget' budget,
		    adset->>'bid_amount' bid,
		    adset->>'bid_is_autobid' autobid,
		    adset->>'reachestimate' reach_estimate,
		    adset->>'bid_billing_event' billing_event,
		    adset->>'goal' optimization_goal,
		    adset#>'{spent_ts,0}' spend,
		    adset#>'{impressions_ts,0}' impressions,
		    adset#>'{link_clicks_ts,0}' clicks,
	    	adset#>'{conversions_ts,0}' conversions
			FROM buster.pba_input_data_view_by_date
			CROSS JOIN json_array_elements(input_data_json::json) adset
			WHERE 
		    DATE BETWEEN '", start, "' AND '", end, "'", sep=""
		))

		#Add up all data to data.all variable
		if (exists("data.all")) {
			data.all <- rbind(data.all, data.month)
		} else {
			data.all <- data.month
		}

		cat(" ", "\u2713")
		cat("\n")
	}
	#save the dataframe: 
	filename <- paste("data", Sys.time(),".RData", sep="")
	save(data.all, file = filename)
	cat("Data saved to file: ", filename, "\n")

	#close connection and return data
	cat("Closing connection... ")
	cat(dbDisconnect(con))
	return(data.all)
}

#-----------------------
runCode <- function() {
	data <- getSavedData()
	data.distributions <- getRewardDistributions(data)
	data.adsets <- data[, .(spend = 0), by = .(id, group_id)] #for temp table in getReturnForAlgorthm

	col <- "impressioins"
	data.campaigns <- getSimulatedCampaigns(data.distributions, "impressions")
	# save(data.campaigns, file = "data030917.RData")
	# load("data030917.RData")

	data.optimal <- getReturnForAlgorithm(data.campaigns, optimalAllocation, "optimal", data.adsets)

	# test <- getTestData(data.campaigns)
	# output <- getReturnForAlgorithm2(test, optimalAllocation2, "test", data.adsets)

}

# get test data for two campaigns:
getTestData <- function(data.campaigns) {
	test <- data.campaigns[group_id %in% c("5527905fd1a561f72d8b456c","583d0bf17bff8500408b4567")]
	# returns <- returns[date %in% c(as.Date("2016-03-31"),as.Date("2016-12-27"))]
	return(test)
}

#--------------------

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
	data <- data[spend > 100] #Filter out rows that have spend less than 100
	data <- data[bid > 0] #Filter out rows that have zero/NA in bid
	data <- data[, id:=paste(adset_id, bid, sep="")] #Create unique ids for adsets having different bids
	data <- data[, adsets_in_campaign:=.N, by=.(group_id, date)][adsets_in_campaign > 1] #Filter out day rows when campaign has only one ad set
	data <- data[, days_of_data:=.N, by=id][days_of_data >= 30] # Exclude ad sets with less than 30 days of data



	#convert data types
	data <- data[, budget:=as.numeric(budget)]
	data <- data[, bid:=as.numeric(bid)]
	data <- data[, reach_estimate:=as.numeric(reach_estimate)]
	data <- data[, spend:=as.numeric(spend)]
	data <- data[, impressions:=as.numeric(impressions)]
	data <- data[, clicks:=as.numeric(clicks)]
	data <- data[, conversions:=as.numeric(conversions)]

	#add conversion rates
	data <- data[, impressions_cr:=impressions/budget]
	data <- data[, clicks_cr:=clicks/budget]
	data <- data[, conversions_cr:=conversions/budget]

	return (data)
}

getRewardDistributions <- function(dataTable) {
	data <- copy(dataTable)
	cat("Creating reward distributions... ")
	#Each ad set has an own row, columns contain vectors of real conversion rates
	data.distributions <- data[, .(
		dist_impressions = .(impressions_cr), 
		dist_clicks = .(clicks_cr), 
		dist_conversions = .(conversions_cr)
	), by=id]
	cat("\u2713\n")

	cat("Combining distributions to data... ")
	setkey(data, id)
	setkey(data.distributions, id)
	data.combined <- data[data.distributions]
	cat("\u2713\n")

	return (data.combined)
}

#CREATING SAMPLED REWARDS-----------------------
# Converts data on campaign level with r column containing data for each ad set
# Columns for adsetData: 
# r = return for the date
# r_history = returns before the date
# weight = initial weight column with equal allocations

getSimulatedCampaigns <- function(data, conversion_column) {
	data.returns <- generateSamples(data, conversion_column)
	data.history <- addHistoryColumn(data.returns)
	data.campaigns <- getCampaignData(data.history)
	data.ready <- getEqualAllocation(data.campaigns)
	return (data.ready)
}

generateSamples <- function (data, column) {
	cat("Creating sampled observations... ")
	col_dist <- paste("dist_", column, sep="")
	data <- data[, .(r = as.numeric(lapply(get(col_dist), sampleObservations))), by=.(date, id, group_id)]
	cat("\u2713\n")
	return(data)
}

sampleObservations	<- function(observations) {
	sample(observations, size = 1, replace=TRUE)
}

addHistoryColumn <- function(adsetData) {
	cat("Adding history column for samples... ")
	data <- copy(adsetData)
	setkey(data, date)
	data[, 
		r_history := list(mapply(getHistory, .(.SD), date)), 
		by=.(id), 
		.SDcols=c("r", "id", "date")
	]
	cat("\u2713\n")
	return (data)
}

getHistory <- function(adsetData, day) {
	return (adsetData[day > date, r])
}

getCampaignData <- function(adsetData) {
	cat("Creating rows for campaigns... ")
	campaigns <- adsetData[, .(
			r = .(.SD)
		),
		by=.(group_id, date), 
		.SDcols=c("id", "r", "r_history")
	]
	cat("\u2713\n")
	return(campaigns)
}

getEqualAllocation <- function(campaignData) {
	cat("Adding starting allocations... ")
	data <- copy(campaignData)
	setkey(data, group_id, date)
	data[, r := .(lapply(r, addEqualAllocation))]
	cat("\u2713\n")
	return(data)
}

# Takes in dataTable with each row representing an ad set
addEqualAllocation <- function(dataTable) {
	data <- copy(dataTable)
	w <- 1/data[, .N]
	data[, weight := w]
	return(data)
}

#----------------------------------------------------



getOptimalAllocation <- function (data) {
	

}

# output <- getReturnForAlgorithm(test, optimalAllocation2, "test")

getReturnForAlgorithm <- function (dataTable, algorithm, name, spendData) {
	data <- copy(dataTable)
	setkey(data, group_id, date)
	rows <- data[, .N]
	budget <- 100

	# Temp table for preserving spend history
	spendData[, spend := 0]
	setkey(spendData, id)

	cat("Allocating budget with algorithm:", name, "\n")
	cat("0 % ...")

	for(i in 1:rows) {
		#Set allocations
		adsetData <- data[i, r][[1]]
		setkey(adsetData, id)
		algorithm(adsetData)

		#Set spend
		getSpend <- function (spend, adset_id, budget) {
			weight <- adsetData[id == adset_id, weight]
			return (spend + weight * budget)
		}
		spendData[id %in% c(adsetData$id), spend := mapply(getSpend, spend, id, budget)]

		#Set updated adset data to campaign row
		set(data, i, "r", list(list(adsetData)))

		#Log progress to console
		if(i %% (rows / 10)  == 0 ) {
			cat(i/rows * 100, "% ... ");
		}
	}

	cat("\nCalculating returns...")
	columnName <- paste("r", name, sep="_")
	data[, (columnName) := lapply(r, getReturn)]
	cat(" \u2713\n")
	return (data)
}

# Calculates total return from the adset table
getReturn <- function(adsetData) {
	return (sum(adsetData$weight * adsetData$r))
}

optimalAllocation <- function (adsetData) {
	adsets.old <- adsetData[adsetData[, lapply(r_history, length) != 0]]

	if(adsets.old[, .N] != 0) {
		allocableWeight <- adsets.old[, .N]/adsetData[, .N]
		max <- adsets.old[, max(r)]
		getWeight <- function(r) {
			return (if(r==max) allocableWeight else 0)
		}

		adsetData[id %in% c(adsets.old$id), weight := as.numeric(lapply(r, getWeight))]
	}
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

summaryData.sim <-function (dataTable) {
	#Check numer of observation in array of column impressions
	samples[, lapply(impressions, function(x) length(x)), by=id]

	#Get first in list of lists
	samples[, lapply(.SD, function(x) list(do.call(list.zip, x))), by=campaign_id, .SDcols=c("impressions", "clicks", "conversions")][1,clicks][[1]][[1]]


	samples[, lapply(.SD, function(x) length(x)), by=campaign_id, .SDcols=c("impressions")]
}

#Return only adsets that have observations for more than n days
getDataWithAtLeastNDays <- function(dataTable, days) {
	data <- dataTable
	data <- data[, days_of_data:=.N, by=id][days_of_data >= days]
	return (data)
}

histogram <- function(data, column) {
	qplot(data[, column], geom="histogram")
	ggplot(data=data, aes(data[, days_of_data])) + geom_histogram(breaks=seq(30, 360, by = 30), col="red", fill="green", alpha = .2)
	ggplot(test, aes(conversions_cr)) + geom_histogram()
}

countTime <- function() {
	system.time({
		samples[, max_impression := lapply(impressions, function(x) max(x))]
	})
}