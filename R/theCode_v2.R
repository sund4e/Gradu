require(data.table)
require(ggplot2)
require(rbenchmark)
library(pryr)
library(profvis)
# source(here("R", "dataHandling", "getSavedData.R"), chdir=T)
source("R/dataHandling/getSavedData.R")
source("R/dataHandling/getSimulatedData.R")
source("R/dataHandling/getSequentialData.R")
source("R/calculations/calculateReturns.R")

algorithms = c("equal", "greedy", "egreedy.01", "egreedy.05", 
	"egreedy.decreasing.1", "egreedy.decreasing.10", "softmax.25", 
	"softmax.50", "softmix.25", "softmix.50", "ucb", 
	"ucb.tuned", "thompson")
columns = c("optimal", algorithms)
columnNames = c("r", "regret", "relative")
returns = c("r.optimal", paste('r', algorithms, sep="."))
regrets = paste('regret', algorithms, sep=".")
relative = paste('relative', algorithms, sep=".")

#Run code -----------------------
runCode <- function() {
	data <- getSavedData()

	#getSequential data
	impressions.sequential <- getSequentialData(data, "impressions_cr")
	clicks.sequential <- getSequentialData(data, "clicks_cr")
	conversions.sequential <- getSequentialData(data, "conversions_cr")

	#Get allocations
	impressions.weights <- calculateReturns(impressions.sequential)
	clicks.weights <- calculateReturns(clicks.sequential)
	conversions.weights <- calculateReturns(conversions.sequential)

	#Data
	data.plot <- getCombinedPlotData(impressions.weights, clicks.weights, conversions.weights)


	impressions.result <- getRegret(impressions.weights)
	impressions.plot <- getPlotData(impressions.result)


	clicks.result <- getRelativeRegret(clicks.weights)
	conversions.result <- getRelativeRegret(impressions.weights)

	impressions.summary <- getSummaryTable(impressions.result)
	impressions.plot <- getPlotData(impressions.result)
	impressions.time <- getTimeData(impressions.result)

	clicks.summary <- getSummaryTable(clicks.result)
	clicks.plot <- getPlotData(clicks.result)
	clicks.time <- getTimeData(clicks.result)

	conversions.summary <- getSummaryTable(conversions.result)
	conversions.plot <- getPlotData(conversions.result)
	conversions.time <- getTimeData(conversions.result)

	#Plots
	#Boxplot
	ggplot(impressions.plot, aes(x = algorithm, y = relative)) + geom_boxplot(outlier.shape = NA) + coord_flip()
	
	#Lineplot
	ggplot(impressions.time, aes(x = day.campaign, y = value, color = algorithm)) + geom_line()
	ggplot(impressions.time, aes(x = day.campaign, y = cumulative, color = algorithm)) + geom_line() + coord_cartesian(ylim=c(0, 100000))
	ggplot(impressions.time, aes(x = day.campaign, y = cum.relative, color = algorithm)) + geom_line()
	ggplot(impressions.time, aes(x = day.campaign, y = cum.relative)) + geom_line() + facet_wrap( ~ algorithm, ncol=4)
	ggplot(impressions.time[algorithm == "egreedy.01" | algorithm == "thompson"], aes(x = day.campaign, y = relative)) + geom_line() + facet_wrap( ~ algorithm, ncol=4)
	
	ggplot(impressions.time, aes(x = day.campaign, y = relative)) + geom_line() + facet_wrap( ~ algorithm, ncol=4)

	#Point plot
	ggplot(impressions.plot, aes(x = day.campaign, y = regret)) + geom_point() + facet_wrap( ~ algorithm, ncol=3)

	#Histogram
	ggplot(impressions.plot, aes(x = regret)) + geom_histogram(binwidth = 0.05) + facet_wrap( ~ algorithm, ncol=3) + coord_cartesian(ylim=c(0, 10000), xlim=c(0, 3))
	ggplot(impressions.plot, aes(x = relative)) + geom_histogram(binwidth = 0.01) + facet_wrap( ~ algorithm, ncol=4)

	#Final plots
	ggplot(impressions.plot[algorithm == "greedy" | algorithm == "thompson" | algorithm == "egreedy.decreasing.1", lapply(.SD, mean), by = .(day.campaign, algorithm), .SDcols = columnNames], aes(x = day.campaign, y = relative, color = algorithm)) + geom_line() + coord_cartesian(ylim=c(0, 0.25))

	#1. Boxplot showing distribution of relative regrets (or gertets?)
	ggplot(data.plot, aes(x = algorithm, y = relative)) + geom_boxplot(outlier.shape = NA) + coord_flip() + facet_wrap( ~ goal, ncol=3)
	ggplot(data.plot, aes(x = algorithm, y = relative)) + geom_violin(draw_quantiles = TRUE, scale = "width") + coord_flip() + facet_wrap( ~ goal, ncol=3)
	#2. Histogram showing the distribution of relative regrets for each algorithm?
	#3. Timeseries showing correlation of time with the regret
	ggplot(data.plot[days >= 200 & day.campaign < 200, .(relative = mean(relative)), by = .(day.campaign, algorithm, goal)], aes(x = day.campaign, y = relative, color = goal)) + geom_line() + facet_wrap( ~ algorithm, ncol=4)
	#4. Summary table of regrets
}

getCombinedPlotData <- function(impressions.weights, clicks.weights, conversions.weights) {
	impressions <- getRegret(impressions.weights, "Impressions")
	clicks <- getRegret(clicks.weights, "Clicks")
	conversions <- getRegret(conversions.weights, "Conversions")
	combined <- rbindlist(list(impressions, clicks, conversions))
	plot <- getPlotData(combined, c("group_id", "day.campaign", "goal"))
	return (plot)
}

# TODO: Check why campaigns with day 1 are not removed
# Check why clicks and conversions have NA rows 
# data.plot[is.na(relative)][day.campaign != 1][goal != "Conversions"]
getRegret <- function(data.weights, goal = "Impressions") {
	data <- copy(data.weights)
	data[, (returns) := .SD * data[, r], .SDcols = columns]
	adsets <- data[, .(adsets = .N), keyby = .(group_id, day.campaign)]
	data <- data[, lapply(.SD, sum, na.rm = TRUE), keyby = .(group_id, day.campaign), .SDcols = returns]
	data <- data[r.optimal > 0][day.campaign > 1] #Remove reduntant rows
	data[, (regrets) := data[, r.optimal] - .SD, .SDcols = returns[-1]]	
	#Fix negative regrets (due to rounding in alloction) to zero
	data[, (regrets) := lapply(.SD, function(regret) lapply(regret, max, 0)), .SDcols = regrets]
	data[, (regrets) := lapply(.SD, as.numeric), .SDcols = regrets]
	data[, (relative) := .SD / data[, r.optimal], .SDcols = regrets]
	
	data[, days := max(day.campaign), by = group_id]
	data <- data[adsets]
	data[, goal := goal]
	return(data)
}

getAggregatedByCampaign <- function(data) {
	sums = paste('sum', algorithms, sep=".")

	sum.regrets <- data[, lapply(.SD, sum, na.rm = TRUE), keyby=group_id, .SDcols = regrets]
	setnames(sum.regrets, old = regrets, new = sums)
	sum.optimal <- data[, .(sum.optimal = sum(r.optimal)), keyby=group_id]
	avrg.regrets <- data[, lapply(.SD, mean, na.rm = TRUE), keyby=group_id, .SDcols = regrets]
	avrg.returns <- data[, lapply(.SD, mean, na.rm = TRUE), keyby=group_id, .SDcols = returns]

	data.combined = avrg.returns[avrg.regrets][sum.regrets][sum.optimal]
	data.combined[, (relative) := .SD / data.combined[, sum.optimal], .SDcols = sums]

	return (data.combined)
}

getPlotData <- function(data, keys = c("group_id", "day.campaign")) {
	r = returns[-1]
	measures = list(r, regrets, relative)
	data.plot = melt(
		data, 
		id.vars = keys, 
		measure.vars = measures, 
		variable.name = "algorithm",
		value.name = c('r', 'regret', 'relative')
	)
	data.plot[, algorithm := algorithms[algorithm]]

	setkeyv(data.plot, keys)
	temp <- data[, .(days, adsets), keyby = keys]
	data.plot <- data.plot[temp]

	return (data.plot)
}

getTimeData <- function(data, correctSoftMix = FALSE) {
	if(correctSoftMix) {
		data <- removeNAs(data)
	}
	data.day <- data[, lapply(.SD, sum), by=day.campaign, .SDcols = -c("group_id")]

	algorithms = c("equal", "greedy", "egreedy.01", "egreedy.05", 
	"egreedy.decreasing.1", "egreedy.decreasing.10", "softmax.25", 
	"softmax.5", "softmix.25", "softmix.5", "ucb", 
	"ucb.tuned", "thompson")
	id.columns = c("day.campaign")
	columns = c('r', 'regret', 'relative')
	columns.cumulative = paste('cum', columns, sep=".")
	r = paste('r', algorithms, sep=".")
	regret = paste('regret', algorithms, sep=".")
	relative = paste('relative', algorithms, sep=".")
	measures = list(r, regret, relative)

	data.plot = melt(
		data.day, 
		id.vars = id.columns, 
		measure.vars = measures, 
		variable.name = "algorithm", 
		value.name = columns
	)
	data.plot[, (columns.cumulative) := lapply(.SD, cumsum), by=algorithm, .SDcols = columns]
	data.plot[, algorithm := algorithms[algorithm]]
	return (data.plot)
}

combineResults <- function(impressions.result, clicks.result, conversions.result) {
	impressions <- getSummaryTable(impressions.result)
	clicks <- getSummaryTable(clicks.result)
	conversions <- getSummaryTable(conversions.result)
	combined <- impressions[clicks][conversions]
}

getSummaryTable <- function(data) {
	temp <- copy(data)
	average <- getAggregate(temp, mean, "Average")
	stdev <- getAggregate(temp, sd, "SD")
	combined <- rbindlist(list(average, stdev))
	transposed <- getTranspose(combined)
	summary <- getRelevantColumns(transposed)
	return (summary)
}

getAggregate <- function(data, fun, metric.name) {
	aggregated.campaign <- data[, lapply(.SD, fun, na.rm = TRUE), by = .(group_id)]
	aggregated <- aggregated.campaign[, lapply(.SD, fun, na.rm = TRUE), .SDcols = -c("group_id")]
	# aggregated <- data[, lapply(.SD, fun, na.rm = TRUE), .SDcols = -c("group_id")]
	aggregated[, metric := metric.name]
	return(aggregated)
}

getTranspose <- function(dataTable) {
  row.names = names(dataTable)
	columns.numeric = dataTable[, metric]
	col.names = c(columns.numeric, "Algorithm")
  data.transpose = transpose(dataTable)
	data.transpose[, "Algorithm" := row.names]
  setnames(data.transpose, col.names)
	data.transpose = data.transpose[-1,] #remove first row containing day.campaign
	data.transpose = data.transpose[-.N] #remove last row containing metric
  setcolorder(data.transpose, c("Algorithm", "Average", "SD"))
	
	data.transpose = roundNumers(data.transpose, columns.numeric)
	setkey(data.transpose, Algorithm)
  return (data.transpose)
}

roundNumers <- function(data, columns.numeric) {
	data[, columns.numeric] = data[, lapply(.SD, as.numeric), .SDcols = -c("Algorithm")]
	data[, columns.numeric] = data[, lapply(.SD, round, digits = 3), .SDcols = -c("Algorithm")]
	return (data)
}

getRelevantColumns <- function(data.transposed) {
	regret <- data.transposed[Algorithm %like% "^regret"]
	relative <- data.transposed[Algorithm %like% "^relative"][, SD := NULL]
	parseAlgorithm(regret)
	parseAlgorithm(relative)
	data <- relative[regret]
	setnames(data, c("Algorithm", "Relative", "Average", "SD"))
	return(data)
}

parseAlgorithm <- function(data) {
	data[, Algorithm := gsub("^\\w+\\.", "", Algorithm)]
	setkey(data, Algorithm)
}