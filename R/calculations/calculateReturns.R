require(data.table)
source("R/calculations/algorithms.R")
source("R/calculations/helpers.R")

calculateReturns <- function (dataTable) {
	budget <- 100
	epsilon05 = 0.5
	epsilon01 = 0.1
	c1 = 1
	c10 = 10
	tau25 = 25
	tau5 = 5

	data <- copy(dataTable)
  data <- getRunningDays(data)
	data <- setInitialColumns(data, budget)

	cat("Calculating returns for greedy algorithms... ")
	setkey(data, day.adset)
	data[, optimal := getOptimalWeight(.SD, 'r')]
	data[, egreedy.01 := getEpsilonGreedyWeight(.SD, epsilon01)]
	data[, egreedy.05 := getEpsilonGreedyWeight(.SD, epsilon05)]
	data[, egreedy.decreasing.1 := getDecreasingEpsilonGreedyWeight(.SD, c1)]
	data[, egreedy.decreasing.10 := getDecreasingEpsilonGreedyWeight(.SD, c10)]
	cat("\u2713\n")

	cat("Calculating returns for probability matching... ")
	setkey(data, day.adset)
	data[, softmax.25 := getSoftMaxWeight(.SD, tau25)]
	data[, softmax.50 := getSoftMaxWeight(.SD, tau5)]
	data[, softmix.25 := getSoftMixWeight(.SD, tau25)]
	data[, softmix.50 := getSoftMixWeight(.SD, tau5)]
	cat("\u2713\n")

	cat("Calculating returns for UCB and Thompson... \n")
	#Set equal weights as starting point
	data[, ucb := w.equal]
	data[, ucb.tuned := w.equal]
	data[, thompson := w.equal]
	data[, greedy := 0]
	data[.(1), greedy := w.equal]

	#Set weights for consecutive days
	days <- max(data$day.campaign)
	for(i in 1:days) {
		setkey(data, id, day.adset)
		data[, `:=` (
			temp.ucb = sum(spend.ucb),
			temp.ucb.tuned = sum(spend.ucb.tuned),
			temp.thompson = sum(spend.thompson),
			temp.sum.r.count = sum(r.count),
			temp.sum.r.greedy = sum(r.greedy),
			temp.count.greedy = sum(ceiling(greedy))
		), by = .(id)]

		setkey(data, day.campaign)
		data[.(i), `:=` (
			sum.spend.ucb = temp.ucb,
			sum.spend.ucb.tuned = temp.ucb.tuned,
			sum.spend.thompson = temp.thompson,
			sum.r.count = temp.sum.r.count,
			r.avrg.greedy = temp.sum.r.greedy / temp.count.greedy
		)]

		setkey(data, day.campaign, id)
		data[.(i), `:=` (
			ucb = getUCBWeight(.SD),
			ucb.tuned = getUCBTunedWeight(.SD),
			thompson = getThompsonWeight(.SD),
			greedy = getGreedyWeight(.SD)
		)]

		#Update running numbers
		setkey(data, day.campaign)
		data[.(i), `:=` (
			spend.ucb = ucb * budget,
			spend.ucb.tuned = ucb.tuned * budget,
			spend.thompson = thompson * budget,
			r.count = r * (thompson * budget),
			r.greedy = r * ceiling(greedy)
		)]
		
		# Log progress to console
		if(i %% 100  == 0 ) {
			cat("(", floor(i/days * 100), "% ) \n", sep="");
		} else {
			cat(".")
		}
	}

	cat("(100%) \n")

	data[, greedy := getCorrectedGreedy(.SD)]

	print(data)
	return (data)
}

testing <- function(dataTable) {
	budget <- 100
	epsilon05 = 0.5
	epsilon01 = 0.1
	c1 = 1
	c10 = 10
	tau25 = 25
	tau5 = 5

	data <- copy(dataTable)
	data <- getRunningDays(data)
	calculateInitalColumns(data, budget)
	calculateReturnsForStaticAlgorithms(data, epsilon01, epsilon05, tau25, tau5)

	weights = c(
		"greedy", 
		"egreedy.decreasing.1", 
		"egreedy.decreasing.10",
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

	data[, (weights) := 0]
	data[, (returns) := 0]
	data[, (counts) := 0]

	cat("Calculating returns for UCB and Thompson... \n")
	setkey(data, day.campaign, id)
	days <- max(data$day.campaign)
	for(i in 1:days) {
		data[, (temps) := sum(get(returns))/sum(get(counts)), by = .(id)]
		data[.(i), (avrgs) := get(temps)]
		data[, (temps) := sum(get(weights) * budget), by = .(id)]
		data[.(i), (spends) := get(temps)]
		data[.(i), (conversions) := get(avrgs) * get(spends)]

		data[.(i), `:=` (
			greedy = getGreedyWeight(.SD, "avrg.greedy"),
			egreedy.decreasing.1 = getDecreasingEpsilonGreedyWeight(.SD, c1, "avrg.egreedy.decreasing.1"),
			egreedy.decreasing.10 = getDecreasingEpsilonGreedyWeight(.SD, c10, "avrg.egreedy.decreasing.10"),
			softmix.25 = getSoftMixWeight(.SD, tau25, "avrg.softmix.25"),
			softmix.5 = getSoftMixWeight(.SD, tau5, "avrg.softmix.5"),
			ucb = getUCBWeight(.SD, "avrg.ucb"),
			ucb.tuned = getUCBTunedWeight(.SD, "avrg.ucb.tuned"),
			thompson = getThompsonWeight(.SD)
		)]

		data[.(i), (weights) := round(.SD, 2), .SDcols = weights] #Prevent allocating infinitely small amount
		data[.(i), (counts) := ceiling(.SD), .SDcols = weights]
		data[.(i), (returns) := lapply(.SD, "*", data[.(i), r]), .SDcols = weights]

		for(col in seq_along(weights)) {
			set(data, i=)
		}
		
		# Log progress to console
		if(i %% 100  == 0 ) {
			cat("(", floor(i/days * 100), "% ) \n", sep="");
		} else {
			cat(".")
		}
	}
	cat("(100%) \n")

	data[, greedy := getCorrectedGreedy(.SD)]
	return(data)
}

roundWeights <- function(data) {
	rounded <- data[, lapply(.SD, round, digits = 2)]
	return(rounded)
}

calculateReturnsForStaticAlgorithms <- function(data, epsilon01, epsilon05, tau25, tau5) {
	cat("Calculating returns for static algorithms... ")
	setkey(data, day.adset)
	data[, optimal := getOptimalWeight(.SD, 'r')]
	data[, egreedy.01 := getEpsilonGreedyWeight(.SD, epsilon01)]
	data[, egreedy.05 := getEpsilonGreedyWeight(.SD, epsilon05)]
	data[, softmax.25 := getSoftMaxWeight(.SD, tau25)]
	data[, softmax.50 := getSoftMaxWeight(.SD, tau5)]
	cat("\u2713\n")
}

calculateInitalColumns <- function(data, budget) {
	cat("Calculating initial columns... ")
	setkey(data, day.adset)
	data[, w.equal := 1/.N, by=.(group_id, day.campaign)]
	data[!.(1), n.allocable := .N, by=.(group_id, date)]
	data[, w.allocable := getAllocableWeight(.SD)]
	data[, budget := budget]
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

setInitialColumns <- function (dataTable, budget) {
  data <- dataTable
	cat("Adding default columns... ")
	setkey(data, day.adset)
	data[, w.equal := 1/.N, by=.(group_id, day.campaign)]
	data[, r.avrg := getAverageReturn(.SD)]
	data[!.(1), n.allocable := .N, by=.(group_id, date)]
	data[, w.allocable := getAllocableWeight(.SD)]
	data[, r.avrg.max := getMaxForDay(.SD, 'r.avrg')]
	data[, budget := budget]

	#Init columns for UCB
	data[, ln.spend := log(budget * (day.campaign-1))]
	data[, ln.time := log(day.campaign)]
	data[, r.variance := getReturnVariance(.SD)]
	data[, spend.ucb := 0]
	data[, spend.ucb.tuned := 0]

	#Init columns for Thopson Sampling
	data[, spend.thompson := 0]
	data[, r.count := 0]

	#Init columns for greedy
	data[, r.greedy := 0]
	data[, r.avrg.greedy := 0]
	cat("\u2713\n")
  return(data)
}