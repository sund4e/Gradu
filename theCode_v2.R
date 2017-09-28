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
require(RcppRoll)

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

# data.returns <- generateSamples(data.distributions, "impressions")
# data.ready <- getRunningDays(data.returns)
getSimulatedAdsets <- function(data, conversion_column) {
	data.returns <- generateSamples(data, conversion_column)
	data.ready <- getRunningDays(data.returns)
	return (data.ready)
}

getRunningDays <- function(data) {
	cat("Add columns for running days... ")
	setkey(data, group_id, date, id)
	days.campaign <- data[, .(date = unique(date)), by = .(group_id)]
	days.campaign[, day.campaign := 1:.N, by = .(group_id)]
	setkey(days.campaign, group_id, date)
	data.combined <- data[days.campaign]
	
	setkey(data, id, date)
	days.adset <- data[, .(date = date, day.adset = 1:.N), by = .(group_id, id)]
	setkey(days.adset, group_id, date, id)
	data.combined <- data.combined[days.adset]

	cat("\u2713\n")
	return (data.combined)
}

# data.adsets <- getSimulatedAdsets(data.distributions, "impressions")
# output <- calculateReturns(data.adsets)
# output <- calculateReturns(data.adsets[group_id == "5527905fd1a561f72d8b456c"])
# output[group_id=="55fbd93458e7ab426a8b4567"][day==303]

#system.time({output <- calculateReturns(data.adsets)})


# System times:
# With optimal only: 26.951
# With greedy algorithms: 190.895 NEW: 3.489
# With UCB: 5062.102 NEW: 59.642

# Two first days:
# With all: 95.900

# Without default columns && UCB: 12.127

# Just optimal, one campaign with 50 reps
# Old: 11.872
# new: 25.447

# Just optimal, whole data, 5 days
# Old: 28.058
# new: 11.606

calculateReturns <- function (dataTable) {
	data <- copy(dataTable)
	# setkey(data, group_id, day.campaign, id)
	days <- max(data$day.campaign)
	budget <- 100
	epsilon05 = 0.5
	epsilon01 = 0.1
	c1 = 1
	c10 = 10

	cat("Adding default columns... ")
	setkey(data, day.adset)
	data[, w.equal := 1/.N, by=.(group_id, day.campaign)]
	data[, r.avrg := getAverageReturn(.SD)]
	data[!.(1), n.allocable := .N, by=.(group_id, date)]
	data[, w.allocable := getAllocableWeight(.SD)]
	data[, r.max := getMaxForDay(.SD, 'r')]
	data[, r.avrg.max := getMaxForDay(.SD, 'r.avrg')]

	data[, budget := budget]
	data[, ln.spend := log(budget * (day.campaign-1))]
	data[, spend.adset := getAdsetSpend(.SD, 'w.equal')]
	cat("\u2713\n")

	cat("Calculating returns for greedy algorithms... ")
	setkey(data, day.adset)
	data[, optimal := getGreedyWeight(.SD, 'r')]
	data[, greedy := getGreedyWeight(.SD, 'r.avrg')]
	data[, egreedy.01 := getEpsilonGreedyWeight(.SD, epsilon01)]
	data[, egreedy.05 := getEpsilonGreedyWeight(.SD, epsilon05)]
	data[, egreedy.decreasing.1 := getDecreasingEpsilonGreedyWeight(.SD, c1)]
	data[, egreedy.decreasing.10 := getDecreasingEpsilonGreedyWeight(.SD, c10)]
	cat("\u2713\n")

	data[, ucb := w.equal]
	setkey(data, day.campaign)

	cat("Calculating returns for UCB... \n")
	for(i in 1:days) { #two campaign have 303 days
		data[.(i), ucb := getUCBWeight(.SD)]
		data[, spend.adset := getAdsetSpend(.SD, 'ucb')]
		
		#Log progress to console
		if(i %% 100  == 0 ) {
			cat("(", ceiling(i/days * 100), "% ) \n", sep="");
		} else {
			cat(".")
		}
	}

	setkey(data, day.campaign)

	cat("(100%) \n")
	print(data)
	return (data)
}

# Get the average return befor the date
getAverageReturn <- function (data) {
  temp <- copy(data)
  temp[, r.cumulative := cumsum(r), by=.(id)]
  temp[, r.avrg := ((r.cumulative - r) / (day.adset - 1))]
  return(temp[, r.avrg])
}

# Get the weight of adsets havign history, i.e. non new adsets
# Assumes that day.adset is set as key
getAllocableWeight <- function (data) {
  temp <- copy(data)
  temp[, w.allocable := 0]
  temp[!.(1), w.allocable := sum(w.equal), by=.(group_id, date)]
  return(temp[, w.allocable])
}

# Get the maximum adset return for each day in each campaign (ignoring new adsets)
# Assumes that day.adset is set as key
getMaxForDay <- function (data, column) {
  temp <- copy(data)
  temp[, max := 0]
  temp[!.(1), max := max(get(column)), by=.(group_id, date)]
  return(temp[, max])
}

# Get the maximum adset return for each day in each campaign (ignoring new adsets)
# Assumes that day.adset is set as key
getAdsetSpend <- function (data, weightColumn) {
  temp <- copy(data)
  temp[, spend := get(weightColumn) * budget]
  temp[, spend.cumulative := cumsum(spend), by=.(id)]
  return(temp[, spend.cumulative - spend])
}

#Allocation algorithms ------------------

getGreedyWeight <- function(data, column) {
	max.column = paste(column, 'max', sep='.')
	temp <- copy(data)
  temp[get(column) == get(max.column), weight := w.allocable]
  temp[get(column) != get(max.column), weight := 0]
  temp[get(max.column) == 0, weight := w.equal]
  return(temp[, weight])
}

addWeight <- function(data, column, column.true, column.false) {
	max.column = paste(column, 'max', sep='.')
  data[get(column) == get(max.column), weight := get(column.true)]
  data[get(column) != get(max.column), weight := get(column.false)]
  data[get(max.column) == 0, weight := w.equal]
}

getEpsilonGreedyWeight <- function(data, epsilon) {
	temp <- copy(data)
	temp[, exploration.weight := epsilon * (w.allocable / n.allocable)]
	temp[, exploitation.weight := {1 - epsilon} * w.allocable + exploration.weight]
	addWeight(temp, 'r.avrg', 'exploitation.weight', 'exploration.weight')
	return (temp[, weight])
}

getDecreasingEpsilonGreedyWeight <- function(data, constant) {
	temp <- copy(data)
	temp[, c := constant]
	temp[, epsilon := constant/day.campaign]
	temp[epsilon > 1, epsilon := 1]
	temp[, exploration.weight := epsilon * (w.allocable / n.allocable)]
	temp[, exploitation.weight := {1 - epsilon} * w.allocable + exploration.weight]
	addWeight(temp, 'r.avrg', 'exploitation.weight', 'exploration.weight')
	return (temp[, weight])
}

getUCBWeight <- function(data) {
	temp <- copy(data)
	temp[, ci := 0]
	temp[!.(1), ci := sqrt((2 * ln.spend)/spend.adset)]
	temp[, `:=` (
		ucb = r.avrg + ci,
		lcb = r.avrg - ci
	)]
	temp[, lcb.max := max(lcb), by=.(group_id, day.campaign)]
	temp[, surviving := ucb > lcb.max]
	temp[surviving == TRUE, n.surviving := .N, by=.(group_id, day.campaign)]
	temp[surviving == TRUE, weight := w.allocable/n.surviving]
	temp[surviving == FALSE, weight := 0]
	temp[.(1), weight := w.equal]
	return (temp[, weight])
}

#----------------------------------------------------

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
		# set(spendData, i=which(spendData[["id"]] == adset_id), j="egreedy.05", value=(adsetData[i, egreedy.05] * budget))
		# set(spendData, i=which(spendData[["id"]] == adset_id), j="egreedy.01", value=(adsetData[i, egreedy.05] * budget))
		# set(spendData, i=which(spendData[["id"]] == adset_id), j="decreasing.egreedy.1", value=(adsetData[i, decreasing.egreedy.1] * budget))
		# set(spendData, i=which(spendData[["id"]] == adset_id), j="decreasing.egreedy.10", value=(adsetData[i, decreasing.egreedy.1] * budget))
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