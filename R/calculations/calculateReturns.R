require(data.table)
source("R/calculations/algorithms.R")
source("R/calculations/helpers.R")

calculateReturns <- function(dataTable) {
	budget <- 100
	epsilon05 = 0.5
	epsilon01 = 0.1
	c1 = 1
	c10 = 10
	tau25 = 25
	tau5 = 5
	precision = 2 #digits to which the allocations are rounded

	data <- copy(dataTable)
	data <- getRunningDays(data)
	calculateInitalColumns(data, budget, precision)
	calculateReturnsForStaticAlgorithms(data, precision, epsilon01, epsilon05, tau25, tau5)
	calculateReturnsForDynamicAlgorithms(data, budget, precision, epsilon01, epsilon05, c1, c10, tau25, tau5)
	data[, greedy := getCorrectedGreedy(.SD)]
	return(data)
}

calculateReturnsForStaticAlgorithms <- function(data, precision, epsilon01, epsilon05, tau25, tau5) {
	cat("Calculating optimal returns... ")
	setkey(data, day.campaign, id)
	data[, optimal := getGreedyWeight(.SD, 'r')]
	# data[, optimal := round(optimal, precision)]
	cat("\u2713\n")
}

calculateReturnsForDynamicAlgorithms <- function(data, budget, precision, epsilon01, epsilon05, c1, c10, tau25, tau5) {
	weights = c(
		"greedy",
		"egreedy.01",
		"egreedy.05", 
		"egreedy.decreasing.1", 
		"egreedy.decreasing.10",
		"softmax.25",
		"softmax.5",
		"softmix.25",
		"softmix.5", 
		"ucb",
		"ucb.tuned",
		"thompson"
	)
	returns = paste("r", weights,  sep=".") # return visible for algorithm
	counts = paste("count", weights,  sep=".") # 1 if has allocation, 0 otherwise
	avrgs = paste("avrg", weights,  sep=".") # average return visible for algorithm
	spends = paste("spend", weights,  sep=".") # cumulative spend for algorithm
	conversions = paste("conversions", weights,  sep=".") # cumulative conversions for algorithm
	temps = paste("temp", weights,  sep=".") # temp column
	tempcounts = paste("tempcounts", weights,  sep=".") # running times of allocations

	data[, (weights) := 0]
	data[, (returns) := 0]
	data[, (counts) := 0]

	cat("Calculating returns for algorithms... \n")
	setkey(data, day.campaign, id)
	days <- max(data$day.campaign)
	for(i in 1:days) {
		data[, (temps) := lapply(.SD, sum), by = .(id), .SDcols = returns]
		data[, (tempcounts) := lapply(.SD, sum), by = .(id), .SDcols = counts]
		data[.(i), (avrgs) := .SD / data[.(i), tempcounts, with=FALSE], .SDcols = temps]
		data[, (temps) := lapply(.SD, function(weight) sum(weight * budget)), by = .(id), .SDcols = weights]
		data[.(i), (spends) := .SD, .SDcols = temps]
		data[.(i), (conversions) := .SD * data[.(i), spends, with=FALSE], .SDcols = avrgs]

		data[.(i), `:=` (
			greedy = getGreedyWeight(.SD, "avrg.greedy"),
			egreedy.01 = getEpsilonGreedyWeight(.SD, epsilon01, "avrg.egreedy.01"),
			egreedy.05 = getEpsilonGreedyWeight(.SD, epsilon05, "avrg.egreedy.05"),
			egreedy.decreasing.1 = getDecreasingEpsilonGreedyWeight(.SD, c1, "avrg.egreedy.decreasing.1"),
			egreedy.decreasing.10 = getDecreasingEpsilonGreedyWeight(.SD, c10, "avrg.egreedy.decreasing.10"),
			softmax.25 = getSoftMaxWeight(.SD, tau25, "avrg.softmax.25"),
			softmax.5 = getSoftMaxWeight(.SD, tau5, "avrg.softmax.5"),
			softmix.25 = getSoftMixWeight(.SD, tau25, "avrg.softmix.25"),
			softmix.5 = getSoftMixWeight(.SD, tau5, "avrg.softmix.5"),
			ucb = getUCBWeight(.SD, "avrg.ucb"),
			ucb.tuned = getUCBTunedWeight(.SD, "avrg.ucb.tuned"),
			thompson = getThompsonWeight(.SD)
		)]

		data[.(i), (weights) := .SD, .SDcols = weights]
		#Prevent allocating infinitely small amount
		data[.(i), (counts) := ceiling(round(.SD, precision)), .SDcols = weights] 
		data[.(i), (weights) := .SD * data[.(i), counts, with=FALSE], .SDcols = weights]

		# data[.(i), (counts) := ceiling(.SD), .SDcols = weights]
		data[.(i), (returns) := .SD * data[.(i), r], .SDcols = counts]

		
		# Log progress to console
		if(i %% 100  == 0 ) {
			cat("(", floor(i/days * 100), "% ) \n", sep="");
		} else {
			cat(".")
		}
	}
	cat("(100%) \n")
}
 
calculateInitalColumns <- function(data, budget, precision) {
	cat("Calculating initial columns... ")
	setkey(data, day.adset)
	data[, w.equal := 1/.N, by=.(group_id, day.campaign)]
	# data[, w.equal := round(1/.N, precision), by=.(group_id, day.campaign)]
	data[!.(1), n.allocable := .N, by=.(group_id, date)]
	data[, w.allocable := getAllocableWeight(.SD)]
	# data[, budget := budget]
	data[, r.avrg := getAverageReturn(.SD)]

	#Init columns for UCB
	data[, ln.spend := log(budget * (day.campaign-1))]
	data[, ln.time := log(day.campaign)]
	data[, r.variance := getReturnVariance(.SD)]
	cat("\u2713\n")
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