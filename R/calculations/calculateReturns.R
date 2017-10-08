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
	tau50 = 50

	data <- copy(dataTable)
	data <- getRunningDays(data)
	calculateInitalColumns(data, budget)
	calculateOptimalReturn(data)
	calculateReturnsForDynamicAlgorithms(data, budget, epsilon01, epsilon05, c1, c10, tau25, tau50)
	return(data)
}

calculateOptimalReturn <- function(data) {
	cat("Calculating optimal returns... ")
	setkey(data, day.campaign, id)
	data[, optimal := getGreedyWeight(.SD, 'r')]
	cat("\u2713\n")
}

calculateReturnsForDynamicAlgorithms <- function(data, budget, epsilon01, epsilon05, c1, c10, tau25, tau50) {
	weights = c(
		"greedy",
		"egreedy.01",
		"egreedy.05", 
		"egreedy.decreasing.1", 
		"egreedy.decreasing.10",
		"softmax.25",
		"softmax.50",
		"softmix.25",
		"softmix.50", 
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
			softmax.50 = getSoftMaxWeight(.SD, tau50, "avrg.softmax.50"),
			softmix.25 = getSoftMixWeight(.SD, tau25, "avrg.softmix.25"),
			softmix.50 = getSoftMixWeight(.SD, tau50, "avrg.softmix.50"),
			ucb = getUCBWeight(.SD, "avrg.ucb"),
			ucb.tuned = getUCBTunedWeight(.SD, "avrg.ucb.tuned"),
			thompson = getThompsonWeight(.SD)
		)]

		data[.(i), (weights) := .SD, .SDcols = weights]
		#Prevent allocating infinitely small amount, minimum allocation is unit of budget
		data[.(i), (counts) := ceiling(round(.SD, 2)), .SDcols = weights] 
		data[.(i), (weights) := .SD * data[.(i), counts, with=FALSE], .SDcols = weights]

		data[.(i), (returns) := .SD * data[.(i), r], .SDcols = counts]

		
		# Log progress to console
		if(i %% 100  == 0 ) {
			cat("(", floor(i/days * 100), "% ) \n", sep="");
		} else {
			cat(".")
		}
	}
	cat("(100%) \n")

	# Fix all allocations to equal their proportion of the total allocaion for day
	data[, (temps) := lapply(.SD, sum), by = .(group_id, day.campaign), .SDcols = weights]
	data[, (weights) := .SD / data[, temps, with=FALSE], .SDcols = weights]
}
 
calculateInitalColumns <- function(data, budget) {
	cat("Calculating initial columns... ")
	setkey(data, day.adset)
	data[, equal := 1/.N, by=.(group_id, day.campaign)]
	data[!.(1), n.allocable := .N, by=.(group_id, date)]
	data[, w.allocable := getAllocableWeight(.SD)]
	data[, r.avrg := getAverageReturn(.SD)]

	#Init columns for UCB
	data[, ln.spend := log(budget * (day.campaign-1))]
	data[, ln.time := log(day.campaign)]
	data[, r.variance := getReturnVariance(.SD)]
	cat("\u2713\n")
}

getRunningDays <- function(data) {
	cat("Add columns for running days... ")
	setkey(data, group_id, date)
	days.campaign <- data[, .(date = unique(date)), by = .(group_id)]
	days.campaign[, day.campaign := 1:.N, by = .(group_id)]
	setkey(days.campaign, group_id, date)
	data.combined <- data[days.campaign]
	
	setkey(data, id, date)
	days.adset <- data[, .(date = date, day.adset = 1:.N), by = .(group_id, id)]
	setkey(days.adset, group_id, date, id)
	setkey(data.combined, group_id, date, id)
	data.combined <- data.combined[days.adset]

	cat("\u2713\n")
	return (data.combined)
}