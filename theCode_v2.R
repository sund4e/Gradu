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
	data.adsets <- data[, .(optimal = 0), by = .(id, group_id)] #for temp table in getReturnForAlgorthm

	col <- "impressioins"
	data.campaigns <- getSimulatedCampaigns(data.distributions, "impressions")
	# save(data.campaigns, file = "data030917.RData")
	# load("data030917.RData")

	data.optimal <- getReturns(data.campaigns, data.adsets)

	#Adset test-----------------
	spendData <- data[, .(optimal = 0), by = .(id, group_id)]
	data.adsets <- getSimulatedAdsets(data.distributions, "impressions")

	#----------------------------

	# test <- getTestData(data.campaigns)
	# output <- getReturnForAlgorithm(test, optimalAllocation, "test", data.adsets)

}

# get test data for two campaigns:
getTestData <- function(data.campaigns) {
	test <- data.campaigns[group_id %in% c("5527905fd1a561f72d8b456c","5628e7e858e7abf6308b456c")]
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

	# Remove all duplicate keys 
	# (some of the data is messed up and different ad sets have same ids)
	setkey(data, id, group_id, date)
	data <- unique(data)

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

#------------------------------------------------
#ADSETS
getDay <- function(date, campaign.rows) {
	campaign.rows[date == date, 1:.N, by=date]
}

getSimulatedAdsets <- function(data, conversion_column) {
	data.returns <- generateSamples(data, conversion_column)
	cat("Add column for campaign day... ")
	days <- data.returns[, .(date = unique(date)), by = .(group_id)]
	days[, day := 1:.N, by = .(group_id)]

	setkey(data.returns, group_id, date, id)
	setkey(days, group_id, date)
	data.returns <- data.returns[days]

	cat("\u2713\n")
	return (data.returns)
}
# output <- calculateReturns(data.adsets)
# output[group_id=="55fbd93458e7ab426a8b4567"][day==303]
calculateReturns <- function (dataTable) {
	data <- copy(dataTable)
	setkey(data, group_id, day, id)
	days <- max(data$day)
	budget <- 100

	#As default, set equal allocations for all ad sets
	data[, equal := 1/.N, by=.(group_id, day)]

	cat("Calculating returns with algorithms \n")
	for(i in 1:5) { #two campaign have 303 days
		history <- data[day > 1]
		setkey(history, group_id, day, id)
		# adsets.old <- data[day == i & id %in% history$id]
		# data[day == i, r_max := max(r), by=.(group_id)]

		data[
			day == i & id %in% history$id, 
			optimal := mapply(getOptimalWeight, .SD$r, .SD[, max(r)], .SD[, sum(equal)]), 
			by=.(group_id), 
			.SDcols=c("id", "r", "equal")
		]

		data[
			day == i & id %in% history$id, 
			greedy := mapply(getGreedyWeight, .(.SD), .(history), .SD[, sum(equal)]), 
			by=.(group_id), 
			.SDcols=c("id", "r", "equal")
		]
		
		
		# data[day == i, allocation := mapply(getAllocation, .(.SD), .(history)), by=.(group_id), .SDcols=c("id", "r")]
		
		#Log progress to console
		if(i %% 100  == 0 ) {
			cat("(", ceiling(i/days * 100), "% ) \n", sep="");
		} else {
			cat(".")
		}
	}

	cat("(100%) \n")
	return (data)
}

getOptimalWeight <- function(returns, max, allocableWeight) {
	return (sapply(returns, function(r) {
		return (if(r==max) allocableWeight else 0)
	}))
}

getGreedyWeight <- function(adsets, history, allocableWeight) {
	averages <- history[id %in% adsets$id, mean(r)]
	max <- max(averages)
	getOptimalWeight(averages, max, allocableWeight)
}

getAllocation <- function (adsets, history) {
	print(history)
	print(adsets)
	return (adsets$r)
}

#----------------------------------------------------

getReturns <- function (dataTable, spendData) {
	cat()
	data <- copy(dataTable)
	setkey(data, group_id, date)
	rows <- data[, .N]
	budget <- 100

	# data[i, `:=` (
	# 		optimal = 0,
	# 		greedy = 0,
	# 		egreedy.05 = 0,
	# 		egreedy.01 = 0,
	# 		decreasing.egreedy.1 = 0,
	# 		decreasing.egreedy.10 = 0
	# 	)]

	# Temp table for preserving spend history
	spendData[, `:=` (
		optimal = 0,
		greedy = 0,
		egreedy.05 = 0,
		egreedy.01 = 0,
		decreasing.egreedy.1 = 0,
		decreasing.egreedy.10 = 0
	)]
	setkey(spendData, id)

	cat("Calculating returns with algorithms \n")
	for(i in 1:rows) {
		#Set allocations
		adsetData <- copy(data[i, r][[1]])
		setkey(adsetData, id)
		getAllocations(adsetData, spendData)
		updateSpend(adsetData, spendData, budget)

		#Set returns
		getReturn <- function(column) {
			return (sum(adsetData[, get(column)] * adsetData$r))
		}

		set(data, i=i, j="optimal", value=getReturn('optimal'))
		set(data, i=i, j="greedy", value=getReturn('greedy'))
		set(data, i=i, j="egreedy.05", value=getReturn('egreedy.05'))
		set(data, i=i, j="egreedy.01", value=getReturn('egreedy.01'))
		set(data, i=i, j="decreasing.egreedy.1", value=getReturn('decreasing.egreedy.1'))
		set(data, i=i, j="decreasing.egreedy.10", value=getReturn('decreasing.egreedy.10'))

		#Log progress to console
		if(i %% 1000  == 0 ) {
			cat("(", ceiling(i/rows * 100), "% ) \n", sep="");
		} else if(i %% 10  == 0 ) {
			cat(".")
		}
	}

	cat("(100%) \n")
	return (data)
}

#Testing
testing <- function (data.adsets, test) {
	spendData <- copy(data.adsets)
	spendData[, `:=` (
		optimal = 0,
		greedy = 0,
		egreedy.05 = 0,
		egreedy.01 = 0,
		decreasing.egreedy.1 = 0,
		decreasing.egreedy.10 = 0
	)]
	adsetData <- copy(test[250,r][[1]])

	getAllocations(adsetData, spendData)
	# updateSpend2(adsetData, spendData, 100)

	benchmark(
		updateSpend(adsetData, spendData, 100), 
		updateSpend2(adsetData, spendData, 100), 
		order="elapsed", 
		replications=100
	)
	# spendData[group_id == '5628e7e858e7abf6308b456c']	
}

updateSpend <- function (adsetData, spendData, budget) {
	rows <- adsetData[, .N]
	for (i in 1:rows) {
		adset_id <- adsetData[i, id]
		set(spendData, i=which(spendData[["id"]] == adset_id), j="optimal", value=(adsetData[i, optimal] * budget))
		set(spendData, i=which(spendData[["id"]] == adset_id), j="greedy", value=(adsetData[i, greedy] * budget))
		set(spendData, i=which(spendData[["id"]] == adset_id), j="egreedy.05", value=(adsetData[i, egreedy.05] * budget))
		set(spendData, i=which(spendData[["id"]] == adset_id), j="egreedy.01", value=(adsetData[i, egreedy.05] * budget))
		set(spendData, i=which(spendData[["id"]] == adset_id), j="decreasing.egreedy.1", value=(adsetData[i, decreasing.egreedy.1] * budget))
		set(spendData, i=which(spendData[["id"]] == adset_id), j="decreasing.egreedy.10", value=(adsetData[i, decreasing.egreedy.1] * budget))
	}
}

getAllocations <- function (adsetData, spendData) {
	#Set equal allocations
	equal <- 1/adsetData[, .N]
	set(adsetData, i=NULL, j="optimal", value=equal)
	set(adsetData, i=NULL, j="greedy", value=equal)
	set(adsetData, i=NULL, j="egreedy.05", value=equal)
	set(adsetData, i=NULL, j="egreedy.01", value=equal)
	set(adsetData, i=NULL, j="decreasing.egreedy.1", value=equal)
	set(adsetData, i=NULL, j="decreasing.egreedy.10", value=equal)

	adsetData[, r_avrg := sapply(r_history, mean)]
	adsets.old <- adsetData[adsetData[, sapply(r_history, length) != 0]]
	n = adsets.old[, .N]

	if(adsets.old[, .N] != 0) {
		allocableWeight <- adsets.old[, .N]/adsetData[, .N]
		max <- adsets.old[, max(r)]
		max_avrg <- adsets.old[, max(r_avrg)]
		epsilon05 = 0.5
		epsilon01 = 0.1
		c1 = 1
		c10 = 10
		t = adsets.old[, max(sapply(r_history, length))]

		# Compute confidence bounds for UCB
		# adsetData[, r_ucb := mapply(getUCB, r_avrg, id, spendData)] 

		getOptimalWeight <- function(r) {
			return (if(r==max) allocableWeight else 0)
		}

		getGreedyWeight <- function(r_avrg) {
			return (if(r_avrg==max_avrg) allocableWeight else 0)
		}

		getEpsilonGreedyWeight <- function(r_avrg, epsilon) {
			exploration.weight = epsilon * allocableWeight / (n)
			exploitation.weight = {1 - epsilon} * allocableWeight + exploration.weight
			return (if(r_avrg==max_avrg) exploitation.weight else exploration.weight)
		}

		getDecreasingEpsilonGreedyWeight <- function(r_avrg, constant) {
			epsilon <- min(constant/t, 1)
			return (getEpsilonGreedyWeight(r_avrg, epsilon))
		}

		adsetData[id %in% c(adsets.old$id), `:=` (
			optimal = sapply(r, getOptimalWeight),
			greedy = sapply(r_avrg, getGreedyWeight),
			egreedy.05 = sapply(r_avrg, getEpsilonGreedyWeight, e=epsilon05),
			egreedy.01 = sapply(r_avrg, getEpsilonGreedyWeight, e=epsilon01),
			decreasing.egreedy.1 = sapply(r_avrg, getDecreasingEpsilonGreedyWeight, c=c1),
			decreasing.egreedy.10 = sapply(r_avrg, getDecreasingEpsilonGreedyWeight, c=c10)
		)]
	}
}

getUCB <- function (r, id, spendData) {
	adset_spend <- spendData[id, spend]
	campaign_spend <- 
	return(r + sqrt((2 * ln(campaign_spend))/adset_spend))
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

helpers <- function() {
	# Number of unique dates in each campaign
	data.adsets[, length(unique(date)), by= .(group_id)]
}

#Testing
testUpdateSpend <- function (data.adsets, test) {
	spendData <- data.adsets
	spendData[, `:=` (
		optimal = 0,
		greedy = 0,
		egreedy.05 = 0,
		egreedy.01 = 0,
		decreasing.egreedy.1 = 0,
		decreasing.egreedy.10 = 0
	)]
	adsetData <- test[250,r][[1]]
	benchmark(
		updateSpend(adsetData, spendData, 100), 
		updateSpend2(adsetData, spendData, 100), 
		order="elapsed", 
		replications=100
	)
	# spendData[group_id == '5628e7e858e7abf6308b456c']	
}

updateSpend2 <- function (adsetData, spendData, budget) {
	getSpend <- function (spend, column, adset_id, budget) {
		weight <- adsetData[id == adset_id, get(column)]
		return (spend + weight * budget)
	}
	spendData[id %in% c(adsetData$id), `:=`(
		optimal = mapply(getSpend, optimal, 'optimal', id, budget),
		greedy = mapply(getSpend, greedy, 'greedy', id, budget),
		egreedy.05 = mapply(getSpend, egreedy.05, 'egreedy.05', id, budget),
		egreedy.01 = mapply(getSpend, egreedy.01, 'egreedy.01', id, budget),
		decreasing.egreedy.1 = mapply(getSpend, decreasing.egreedy.1, 'decreasing.egreedy.1', id, budget),
		decreasing.egreedy.10 = mapply(getSpend, decreasing.egreedy.10, 'decreasing.egreedy.10', id, budget)
	)]
}