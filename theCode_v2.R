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

	col <- "impressioins"
	data.campaigns <- getSimulatedCampaigns(data.distributions, "impressions")
	# save(data.campaigns, file = "data030917.RData")

	addAllocations(data.returns, optimalAlgorithm, "optimal")

	# get test data for two campaigns: 
	# test <- data.distributions[group_id %in% c("5527905fd1a561f72d8b456c","583d0bf17bff8500408b4567")]
	# returns <- generateSamples(test, "impressions")
	# returns <- returns[date %in% c(as.Date("2016-03-31"),as.Date("2016-12-27"))]
	# returns.history <- addHistoryColumn(returns)
	# campaigns <- getCampaignData(returns.history)
	# test <- getEqualAllocation(campaigns)

	# Simulations

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

# output <- getReturnForAlgorithm(test, optimalAllocation, "test")

getReturnForAlgorithm <- function (dataTable, algorithm, columnName) {
	col = paste("r", columnName, sep="_")
	data <- copy(dataTable)
	data[, (columnName) := r]
	setkey(data, group_id, date)
	rows <- data[, .N]
	budget <- 100
	# pb <- txtProgressBar(min = 1, max = rows, style = 3)

	# for(i in 1:rows) {
	for(i in 1:5) {
		adsetData <- data[i, r][[1]]
		cat("Row: ", i, " date: ", as.character(data[i, date]), "\n")
		# print("adsetData");
		# print(adsetData);
		# print(length(unlist(adsetData[, r_history])))

		if (length(unlist(adsetData[, r_history])) == 0) {
			# Set inital allocations as equal allocation
			adsetData[, spend := weight * budget]
			adsetData[, total_spend := spend]
		} else {
			#Get return table for previous date
			id <- data[i, group_id]
			date.row <- data[i, date]
			adsetData.previous <- data[group_id == id][date < date.row][.N, r][[1]]

			#Calculate allocations
			algorithm(adsetData, adsetData.previous)
			adsetData[, spend := weight * budget]
			history <- adsetData.previous[, total_spend]
			adsetData[, total_spend := spend + history]
		}
		print(adsetData[, .(id, r, weight, spend, total_spend)])
		set(data, i, "r", list(list(adsetData)))

		# algorithm(data[i, r], data[date == prev, r])
		# set(data, i, "r", algorithm(data[i, r], data[prev, r]))
		# set(data, i, "r", as.data.table(algorithm(data[i, r], data[prev, r])))
		# setTxtProgressBar(pb, i)
	}

	# close(pb)
	return (data)
}

getAllocableWeight <- function(adsetData, adsets.old) {
	adsets.new <- adsetData[, .N] - length(adsets.old)
	weight.newAdsets <- adsetData[id %in% adsets.new, sum(weight)]
	print(1 - weight.newAdsets)
	return (1 - weight.newAdsets)
}

optimalAllocation <- function (adsetData, adsetData.previous) {
	adsets.old <- adsetData[id %in% adsetData.previous[, id], id]
	print(adsets.old)
	max <- adsetData[, max(r)]
	allocable.weight <- getAllocableWeight(adsetData, adsets.old)
	getWeight <- function(r) {
		return (if(r==max) allocable.weight else as.numeric(0))
	}
	adsetData[id %in% adsets.old, weight := as.numeric(lapply(r, getWeight))]
}

addAllocations <- function (data, algorithm, columnName) {
	setkey(data, date, group_id)
	col = paste("r", columnName, sep="_")
	data[, (col) := mapply(function(data, day, campaign, adset) {
		algorithm(data, day, campaign, adset)
	}, data=.(.SD), day=date, campaign=group_id, adset=id)]
}

optimalAlgorithm <- function (data, day, campaign, adset) {
	if (data[.(day, campaign, adset), unlist(r)] == data[.(day, campaign), max(unlist(r))]) {
		return (1)
	}
	return(0)
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

# generateSamples <- function (originalData) {
# 	data <- copy(originalData)
# 	cat("Creating simulated observations... \n")
# 	#Replace oservations with simulated data
# 	columns <- c("impressions", "clicks", "conversions")
# 	rows = data[,.N]
# 	pb <- txtProgressBar(min = 1, max = rows, style = 3)

# 	for (row in 1:rows) {
# 		# cat(row,"...")
# 		for (column in 1:length(columns)) {
# 			observations <- unname(unlist(data[row, columns[column], with=FALSE]))
# 			simulations <- sample(observations, size = length(observations), replace=TRUE)
# 			data[row, columns[column] := .(.(simulations))]
# 			setTxtProgressBar(pb, row)
# 		}
# 	}

# 	close(pb)
# 	cat("Simulated observations created\n")
# 	return(data)
# }

# sampleObservations	<- function(impressions, clicks, conversions) {
# 	return(list(
# 		sample(impressions, size = length(impressions), replace=TRUE),
# 		sample(clicks, size = length(clicks), replace=TRUE),
# 		sample(conversions, size = length(conversions), replace=TRUE)
# 	))
# }

# combineSamples <- function (sampleList) {
# 	list <- sampleList
# 	lengths <- lapply(sampleList, length)
# 	lengths.unique <- list.cases(lengths) #Gets all unique values oredred from small to large 

# 	if(lenght(lengths.unique) > 1) {
# 		combinedList <- list()
# 		startIndex <- 1
# 		for (endIndex in lengths.unique) {
# 			splitList <- lapply(list, function(x) list.subset(x, c(startIndex, endIndex))) #get all observations 
# 			combinedList <- list.append(combinedList, do.call(list.zip, splitList))
# 			indexesToRemove <-  
# 		}
# 	}

# 	return(list(do.call(list.zip, list)))

# }

# combineSamples <- function (sampleList) {
# 	print("Combining...")
# 	for(i in 1:length(sampleList)) {
# 		print(sampleList[[i]][[1]])
# 	}

# 	sampleList.lengths <- lapply(sampleList, length) 

# 	#If some adsets don't have all dates, remove them
# 	if(length(list.cases(sampleList.lengths)) > 1) {
# 		cat("\nSubsetting...")
# 		cat("\ncahnging: ", length(sampleList)) 
# 		sampleList <- list.subset(sampleList, grep(max(unlist(sampleList.lengths)), sampleList.lengths))
# 		cat("\nto: ", length(sampleList))
# 	}
# 	r <- do.call(list.zip, sampleList)
# 	cat("\nReturning:")
# 	print(r[[1]][[1]])
# 	return(list(do.call(list.zip, sampleList)))
# }

# testi <- function (ergument) {
# 	ergument <- "jovain"
# }

#OLD SIMULATION DATA
#----------------------
# getSimulationData <- function(dataTable) {
# 	data.real <- getRewardPerBudget(dataTable)
# 	data.sampled <- generateSamples(data.real)
# 	data.simulation <- getCampaignData(data.sampled)
# 	return(data.simulation)
# }

# getRewardPerBudget <- function(dataTable) {
# 	data <- copy(dataTable)
# 	cat("Creating reward distributions... ")
# 	#Each ad set has an own row, columns contain vectors of real conversion rates
# 	simulationData <- data[, .(
# 		dist_impressions = .(impressions_cr), 
# 		dist_clicks = .(clicks_cr), 
# 		dist_conversions = .(conversions_cr), 
# 		dist_campaign_id = tail(group_id, 1) #Data set has some adsets with multiple campaign ids ????
# 	), by=id]
# 	cat("\u2713\n")
# 	return (simulationData)
# }

# generateSamples <- function (originalData) {
# 	data <- copy(originalData)
# 	cat("Creating sampled observations... ")
# 	#Replace oservations with simulated data
# 	data[, ':=' (
# 		sim_impressions = mapply(function(x) sampleObservations(x), x=dist_impressions), 
# 		sim_clicks = mapply(function(y) sampleObservations(y), y=dist_clicks),
# 		sim_conversions = mapply(function(z) sampleObservations(z), z=dist_conversions)
# 	)]
# 	cat("\u2713\n")
# 	return(data)
# }

# sampleObservations	<- function(observations) {
# 	sample(observations, size = length(observations), replace=TRUE)
# }

# #Input "sampleList" lists samples for each adset: list(list(samples), list(samples), ...)
# combineSamples <- function (sampleList) {
# 	sampleList.lengths <- lapply(sampleList, length) 
	
# 	if(length(list.cases(sampleList.lengths)) > 1) {
# 		sampleList <- list.subset(sampleList, grep(max(unlist(sampleList.lengths)), sampleList.lengths)) #If some adsets don't have all dates, remove them
# 	}
# 	return(list(do.call(list.zip, sampleList)))
# }


# getCampaignData <- function(adsetData) {
# 	data <- copy(adsetData)
# 	cat("Creating rows for campaigns... ")
# 	data <- data[, 
# 		lapply(.SD, combineSamples), 
# 		by=campaign_id, 
# 		.SDcols=c("impressions", "clicks", "conversions")
# 	]
# 	cat("\u2713\n")
# 	return(data)
# }

# generateSamples <- function (originalData) {
# 	data <- copy(originalData)
# 	cat("Creating sampled observations... ")
# 	#Replace oservations with simulated data
# 	data[, ':=' (
# 			sim_impressions = mapply(function(x) sampleObservations(x), x=dist_impressions), 
# 			sim_clicks = mapply(function(y) sampleObservations(y), y=dist_clicks),
# 			sim_conversions = mapply(function(z) sampleObservations(z), z=dist_conversions)
# 		)
# 	}]
# 	cat("\u2713\n")
# 	return(data)
# }

# addAllocations_3 <- function (data, algorithm, columnName) {
# 	col = paste("r", columnName, sep="_")
# 	# data[, .(w = .(lapply(.SD, optimalAlgorithm))), by=.(date, group_id), .SDcols=c("date", "group_id", "id")]
# 	# data[, .(w = mapply(optimalAlgorithm(.SD, x, y, z), x=date, y=group_id, z=id))]
# 	rows = data[,.N]
# 	print(rows)
# 	pb <- txtProgressBar(min = 0, max = rows, style = 3)
# 	data[, (col) := mapply(function(data, day, campaign, adset, row) {
# 		# cat(row)
# 		# cat("\n")
# 		setTxtProgressBar(pb, row)
# 		algorithm(data, day, campaign, adset)
# 	}, data=.(.SD), day=date, campaign=group_id, adset=id, row=.I)]

# 	close(pb)
# }

# getCampaignData <- function(adsetData) {
# 	data <- copy(adsetData)
# 	cat("Creating rows for campaigns... ")
# 	data <- data[, 
# 		.(r = mapply(
# 			function(data) {combineSamples(data)}, 
# 	 		data=.(.SD))
# 		), 
# 		by=.(group_id, date), 
# 		.SDcols=c("r")
# 	]
# 	cat("\u2713\n")
# 	return(data)
# }

# #Input "sampleList" lists samples for each adset: list(list(samples), list(samples), ...)
# combineSamples <- function (sampleList) {
# 	print("---")
# 	print(do.call(list.zip, sampleList))
# 	return (list(do.call(list.zip, sampleList)))
# }

#GET CAMPAIGNS:
# data <- data[, .(
# 		r = mapply(combineSamples, .(.SD[, r])),
# 		# r_history =  mapply(combineSamples, .(.SD[, r_history]))
# 	),
# 	by=.(group_id, date), 
# 	.SDcols=c("r", "r_history")
# ]

# combineHistory <- function (sampleList) {
# 	list <- lapply(sampleList, as.list)
# 	return (list)
# }

#GET ALLOCATIONS
# addAllocations <- function (data, algorithm, columnName) {
# 	setkey(data, date, group_id, id)
# 	col = paste("r", columnName, sep="_")
# 	data[, (col) := mapply(function(data, day, campaign, adset) {
# 		algorithm(data, day, campaign, adset)
# 	}, data=.(.SD), day=date, campaign=group_id, adset=id)]
# }

# optimalAlgorithm <- function (data, day, campaign, adset) {
# 	if (data[.(day, campaign, adset), unlist(r)] == data[.(day, campaign), max(unlist(r))]) {
# 		return (1)
# 	}
# 	return(0)
# }

#-----------------------