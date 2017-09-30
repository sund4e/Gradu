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
require(cumstats)

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

#Run code -----------------------
runCode <- function() {
	data <- getSavedData()
	data.distributions <- getRewardDistributions(data)
	data.adsets <- getSimulatedAdsets(data.distributions, "impressions")
	data.simulated <- calculateReturns(data.adsets)
	data.campaigns <- getRegret(data.simulated)

	#Sequential experiment---
	data.sequential <- getSequentialData(data, "impressions_cr")
	data.returns <- calculateReturns(data.sequential)
	data.result <- getRegret(data.returns)
	data.result.avrg = data.result[, lapply(.SD, mean), by=group_id]

	summary(data.result)
	
	

	# test <- getTestData(data.campaigns)
	# output <- getReturnForAlgorithm(test, optimalAllocation, "test", data.adsets)
	# data.adsets <- getSimulatedAdsets(data.distributions, "impressions")
# output <- calculateReturns(data.adsets)
# output <- calculateReturns(data.adsets[group_id == "5527905fd1a561f72d8b456c"])
# output[group_id=="55fbd93458e7ab426a8b4567"][day==303]

#system.time({output <- calculateReturns(data.adsets)})
#data.campaigns <- getRegret(data.output)

	#Investigate
	# data.returns[group_id == "560bb3cff789b8e5698b4567"][day.campaign == 124]
	# UCBs and Thompson not working
	# Fix greedy algorithm = don't add to average unless budget allocated
	# Would Beta distribution work better for thompson

	#TO TEST:
	# no thompson sampling as negative regret
	# no UCB for campaign is 0 
	# no softmix or max 0

}

#Data generation--------------------

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
	data <- data[spend > 100] #Filter out rows that have spend less than 100
	data <- data[bid > 0] #Filter out rows that have zero/NA in bid
	setkey(data, group_id, date, spend, impressions, clicks, conversions)
	data <- unique(data) #Remove duplicate adsets with still unique ids (some random fuckery in db)
	data <- data[, id:=paste(adset_id, bid, sep="")] #Create unique ids for adsets having different bids
	setkey(data, id, group_id, date)
	data <- unique(data) # Remove all duplicate keys (some of the data is messed up and different ad sets have same ids)
	data <- data[, adsets_in_campaign:=.N, by=.(group_id, date)][adsets_in_campaign > 1] #Filter out day rows when campaign has only one ad set
	data <- data[, days_of_data:=.N, by=id][days_of_data >= 30] # Exclude ad sets with less than 30 days of data

	#add conversion rates
	data[, impressions_cr:=impressions/spend]
	data[, clicks_cr:=clicks/spend]
	data[, conversions_cr:=conversions/spend]

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

#Sequential data ----
getSequentialData <- function(data, conversion_column) {
	data.sequential <- data[, .(
		date = date,
		group_id = group_id,
		id = adset_id,
		r = get(conversion_column)
	)]

	data.sequential = getRunningDays(data.sequential)
	return(data.sequential)
}

#Sample creation-----------------------
getSimulatedAdsets <- function(data, conversion_column) {
	data.returns <- generateSamples(data, conversion_column)
	data.ready <- getRunningDays(data.returns)
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

#Simulation ----------------------------------------------
calculateReturns <- function (dataTable) {
	data <- copy(dataTable)
	# setkey(data, group_id, day.campaign, id)
	days <- max(data$day.campaign)
	budget <- 100
	epsilon05 = 0.5
	epsilon01 = 0.1
	c1 = 1
	c10 = 10
	tau25 = 25
	tau50 = 50

	cat("Adding default columns... ")
	setkey(data, day.adset)
	data[, w.equal := 1/.N, by=.(group_id, day.campaign)]
	data[, r.avrg := getAverageReturn(.SD)]
	data[!.(1), n.allocable := .N, by=.(group_id, date)]
	data[, w.allocable := getAllocableWeight(.SD)]
	data[, r.max := getMaxForDay(.SD, 'r')]
	data[, r.avrg.max := getMaxForDay(.SD, 'r.avrg')]
	data[, budget := budget]
	data[, spend.equal := getAdsetSpend(.SD, 'w.equal')]

	#Init columns for UCB
	data[, ln.spend := log(budget * (day.campaign-1))]
	data[, ln.time := log(day.campaign)]
	data[, r.variance := getReturnVariance(.SD)]
	data[, spend.ucb := 0]
	data[, spend.ucb.tuned := 0]

	#Init columns for Thopson Sampling
	data[, spend.thompson := 0]
	data[, r.count := 0]
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

	cat("Calculating returns for probability matching... ")
	setkey(data, day.adset)
	data[, softmax.25 := getSoftMaxWeight(.SD, tau25)]
	data[, softmax.50 := getSoftMaxWeight(.SD, tau50)]
	data[, softmix.25 := getSoftMixWeight(.SD, tau25)]
	data[, softmix.50 := getSoftMixWeight(.SD, tau50)]
	cat("\u2713\n")

	cat("Calculating returns for UCB and Thompson... \n")
	data[, ucb := w.equal]
	data[, ucb.tuned := w.equal]
	data[, thompson := w.equal]
	for(i in 1:days) {
		setkey(data, id, day.adset)
		data[, `:=` (
			temp.ucb = sum(spend.ucb),
			temp.ucb.tuned = sum(spend.ucb.tuned),
			temp.thompson = sum(spend.thompson),
			temp.r.count = sum(r.count)
		), by = .(id)]

		setkey(data, day.campaign)
		data[.(i), `:=` (
			spend.ucb = temp.ucb,
			spend.ucb.tuned = temp.ucb.tuned,
			spend.thompson = temp.thompson,
			r.count = temp.r.count
		)]

		setkey(data, day.campaign, id)
		data[.(i), ucb := getUCBWeight(.SD)]
		data[.(i), ucb.tuned := getUCBTunedWeight(.SD)]
		data[.(i), thompson := getThompsonWeight(.SD)]

		#Update running numbers
		setkey(data, day.campaign)
		data[.(i), `:=` (
			spend.ucb = temp.ucb + ucb * budget,
			spend.ucb.tuned = temp.ucb.tuned + ucb.tuned * budget,
			spend.thompson = temp.thompson + thompson * budget,
			r.count = temp.r.count + r * (thompson * budget)
		)]
		
		# Log progress to console
		if(i %% 100  == 0 ) {
			cat("(", floor(i/days * 100), "% ) \n", sep="");
		} else {
			cat(".")
		}
	}

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
# Assumes that day.adset is set as key (adset observaitons sorted by day)
getAdsetSpend <- function (data, weightColumn) {
  temp <- copy(data)
  temp[, spend := get(weightColumn) * budget]
  temp[, spend.cumulative := cumsum(spend), by=.(id)]
  return(temp[, spend.cumulative - spend])
}

# Get the maximum adset return for each day in each campaign (ignoring new adsets)
# Assumes that day.adset is set as key
getReturnVariance <- function (data) {
  temp <- copy(data)
  temp[, variance := cumvar(r), by=.(id)]
  temp[, variance.lag := shift(variance, 1), by=.(id)]
  return(temp[, variance.lag])
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

getProbabilityWeights <- function(data) {
	data[!.(1), exp := exp(r.avrg/temperature)]
	data[!.(1), exp.sum := sum(exp), by=.(group_id, day.campaign)]
  data[!.(1), weight := w.allocable * exp/exp.sum]
  data[.(1), weight := w.equal]
	return(data[, weight])
}

getSoftMaxWeight <- function(data, tau) {
	temp <- copy(data)
  temp[, temperature := tau]
  return(getProbabilityWeights(temp))
}

getSoftMixWeight <- function(data, tau) {
	temp <- copy(data)
	temp[, temperature := tau * log(day.campaign)/day.campaign]
  return(getProbabilityWeights(temp))
}

getWeightWithCI <- function(data) {
	temp <- copy(data)
	temp[, `:=` (
		ucb = r.avrg + ci,
		lcb = r.avrg - ci
	)]
	temp[, lcb.max := max(lcb), by=.(group_id, day.campaign)]
	temp[, surviving := ucb >= lcb.max]
	temp[surviving == TRUE, n.surviving := .N, by=.(group_id, day.campaign)]
	temp[surviving == TRUE, weight := w.allocable/n.surviving]
	temp[surviving == FALSE, weight := 0]
	return (temp[, weight])
}

getUCBWeight <- function(data) {
	temp <- copy(data)
	setkey(temp, day.adset)
	temp[, ci := 0]
	temp[!.(1), ci := sqrt((2 * ln.spend)/spend.ucb)]
	temp[!.(1), weight := getWeightWithCI(.SD)]
	temp[.(1), weight := w.equal]
	setkey(temp, day.campaign, id)
	return (temp[, weight])
}

getTunedConfidenceInterval <- function(data) {
	temp <- copy(data)
	temp[, ci.variance := sqrt((2 * ln.spend)/spend.ucb.tuned)]
	temp[, V := r.variance + ci.variance]
	temp[, multiplier := 1/4]
	temp[V < multiplier, multiplier := V]
	temp[, ci := sqrt((ln.time/spend.ucb.tuned)*multiplier)]
	return(temp[, ci])
}

getUCBTunedWeight <- function(data) {
	temp <- copy(data)
	setkey(temp, day.adset)
	temp[, ci := 0]
	temp[!.(1, 2), ci := getTunedConfidenceInterval(.SD)]
	temp[!.(1, 2), weight := getWeightWithCI(.SD)]
	temp[.(1, 2), weight := w.equal]
	setkey(temp, day.campaign, id)
	return (temp[, weight])
}

sampleBest <- function(data, rep) {
	temp <- copy(data)
	temp[, best.count := 0]
	for(i in 1:rep) {
		temp[, r.sample := rgamma(1, shape=r.count, scale=spend.thompson), by=.(id)]
		temp[, max := max(r.sample), by=.(group_id, day.campaign)]
		temp[r.sample == max, best.count := best.count + 1]
	}
	return (temp[, best.count])
}

getThompsonWeight <- function(data) {
	temp <- copy(data)
	setkey(temp, day.adset)
	rep <- 100
	temp[!.(1), best.count := sampleBest(.SD, rep)]
	temp[!.(1), weight := best.count / rep * w.allocable]
	temp[.(1), weight := w.equal]
	setkey(temp, day.campaign, id)
	return (temp[, weight])
}

#Getting regret ------------------------------
getRegret <- function (data) {
	data.campaigns = data[, .(
		r.optimal = sum(optimal * r),
		r.equal = sum(w.equal * r),
		r.greedy = sum(greedy * r),
		r.egreedy.01 = sum(egreedy.01 * r),
		r.egreedy.05 = sum(egreedy.05 * r),
		r.egreedy.decreasing.1 = sum(egreedy.decreasing.1 * r),
		r.egreedy.decreasing.10 = sum(egreedy.decreasing.10 * r),
		r.softmax.25 = sum(softmax.25 * r),
		r.softmax.50 = sum(softmax.50 * r),
		r.softmix.25 = sum(softmix.25 * r),
		r.softmix.50 = sum(softmix.50 * r),
		r.ucb = sum(ucb * r),
		r.ucb.tuned = sum(ucb.tuned * r),
		r.thompson = sum(thompson * r)
	), by = .(group_id, day.campaign)]

	data.campaigns[, `:=` (
		regret.equal = r.optimal - r.equal,
		regret.greedy = r.optimal - r.greedy,
		regret.egreedy.01 = r.optimal - r.egreedy.01,
		regret.egreedy.05 = r.optimal - r.egreedy.05,
		regret.egreedy.decreasing.1 = r.optimal - r.egreedy.decreasing.1,
		regret.egreedy.decreasing.10 = r.optimal - r.egreedy.decreasing.10,
		regret.softmax.25 = r.optimal - r.softmax.25,
		regret.softmax.50 = r.optimal - r.softmax.50,
		regret.softmix.25 = r.optimal - r.softmix.25,
		regret.softmix.50 = r.optimal - r.softmix.50,
		regret.ucb = r.optimal - r.ucb,
		regret.ucb.tuned = r.optimal - r.ucb.tuned,
		regret.thompson = r.optimal - r.thompson
	)]

	return(data.campaigns)
}

#Some random crap----------------------------------------------------

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