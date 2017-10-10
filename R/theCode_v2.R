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
	"egreedy.decreasing.1", "egreedy.decreasing.10", "softmax.1", 
	"softmax.5", "softmix.1", "softmix.5", "ucb", 
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

	#FINAL -----
	data.campaign <- getCombinedPlotData(impressions.weights, clicks.weights, conversions.weights, "campaign")
	data.campaign[, goal := as.factor(goal)]
	levels(data.campaign$goal) <- c("Impressions", "Clicks", "Conversions")
	ggplot(data.campaign, aes(x = algorithm, y = relative)) + geom_boxplot() + coord_flip() + facet_wrap( ~ goal, ncol=3)
	data.time <- getCombinedPlotData(impressions.weights, clicks.weights, conversions.weights, "time")
	ggplot(data.plot, aes(x = day.campaign, y = relative, color = goal)) + geom_line() + facet_wrap( ~ algorithm, ncol=4) + scale_colour_grey()
	summary <- getCombinedSummary(impressions.weights, clicks.weights, conversions.weights)
	fwrite(summary, file = "summary_sequential")
	# ------

	#Final plots -------------------
	ggplot(impressions.plot[algorithm == "greedy" | algorithm == "thompson" | algorithm == "egreedy.decreasing.1", lapply(.SD, mean), by = .(day.campaign, algorithm), .SDcols = columnNames], aes(x = day.campaign, y = relative, color = algorithm)) + geom_line() + coord_cartesian(ylim=c(0, 0.25))

	#1. Boxplot showing distribution of relative regrets (or regrets?)
	ggplot(data.plot, aes(x = algorithm, y = relative)) + geom_boxplot(outlier.shape = NA) + coord_flip() + facet_wrap( ~ goal, ncol=3)
	ggplot(data.plot[, .(relative = mean(relative)), by = .(group_id, algorithm, goal)], aes(x = algorithm, y = relative)) + geom_boxplot(outlier.shape = NA) + coord_flip() + facet_wrap( ~ goal, ncol=3)
	
	#Violin plot
	ggplot(data.plot, aes(x = algorithm, y = relative)) + geom_violin() + coord_flip() + facet_wrap( ~ goal, ncol=3)
	ggplot(data.plot, aes(x = algorithm, y = relative)) + geom_violin(draw_quantiles = TRUE, scale = "width") + coord_flip() + facet_wrap( ~ goal, ncol=3)
	ggplot(data.plot[, .(relative = mean(relative)), by = .(group_id, algorithm, goal)], aes(x = algorithm, y = relative)) + geom_violin(draw_quantiles = TRUE, scale = "width") + coord_flip() + facet_wrap( ~ goal, ncol=3)

	#2. Histogram showing the distribution of relative regrets for each algorithm?
	#3. Timeseries showing correlation of time with the regret
	# ... For non aggregated
	ggplot(data.plot[, .(relative = mean(relative)), by = .(day.campaign, algorithm, goal)], aes(x = day.campaign, y = relative, color = goal)) + geom_line() + facet_wrap( ~ algorithm, ncol=4)
	# ... For aggregated on campaign level
	ggplot(data.plot[, .(relative = mean(relative)), by = .(days, algorithm, goal)], aes(x = days, y = relative, color = goal)) + geom_line() + facet_wrap( ~ algorithm, ncol=4)

	#4. Summary table of regrets
	summary <- getCombinedSummary(impressions.weights, clicks.weights, conversions.weights)

	#5. Performace by number of adses:
	ggplot(data.plot[, .(relative = mean(relative)), by = .(adsets, algorithm, goal)], aes(x = adsets, y = relative, color = goal)) + geom_line() + facet_wrap( ~ algorithm, ncol=4)
}

getCombinedPlotData <- function(impressions.weights, clicks.weights, conversions.weights, cumulative = "campaign") {
	impressions <- getRegret(impressions.weights, "Impressions", cumulative)
	clicks <- getRegret(clicks.weights, "Clicks", cumulative)
	conversions <- getRegret(conversions.weights, "Conversions", cumulative)
	combined <- rbindlist(list(impressions, clicks, conversions))
	key <- if(cumulative == "time") "day.campaign" else "group_id"
	plot <- getPlotData(combined, c(key, "goal"))
	return (plot)
}

getRegret <- function(data.weights, goal = "Impressions", cumulative = "campaign") {
	data <- copy(data.weights)
	adsets <- data[, .(adsets = .N), keyby = .(group_id, day.campaign)]

	data[, (returns) := .SD * data[, r], .SDcols = columns]
	data <- data[, lapply(.SD, sum, na.rm = TRUE), keyby = .(group_id, day.campaign), .SDcols = returns]
	data <- data[r.optimal > 0][day.campaign > 1][day.campaign <= 100] #Remove reduntant rows [day.campaign <= 100]
	data[, (regrets) := data[, r.optimal] - .SD, .SDcols = returns[-1]]	
	#Fix negative regrets (due to rounding in alloction) to zero
	data[, (regrets) := lapply(.SD, function(regret) lapply(regret, max, 0)), .SDcols = regrets]
	data[, (regrets) := lapply(.SD, as.numeric), .SDcols = regrets]

	if (cumulative == "campaign") {
		data <- data[, lapply(.SD, sum), by = group_id]
		adsets <- adsets[, .(adsets = mean(adsets)), keyby = .(group_id)]
	}

	if (cumulative == "time") {
		data <- data[, lapply(.SD, sum), keyby = day.campaign, .SDcols = -c("group_id")]
		adsets <- adsets[, .(adsets = mean(adsets)), keyby = day.campaign]
	}

	data[, (relative) := .SD / data[, r.optimal], .SDcols = regrets]
	data <- adsets[data]
	data[, goal := goal]
	return(data)
}

getPlotData <- function(data, keys = c("group_id", "goal")) {
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
	temp <- data[, .(adsets), keyby = keys]
	data.plot <- data.plot[temp]

	return (data.plot)
}

getCombinedSummary <- function(impressions.weights, clicks.weights, conversions.weights) {
	impressions <- getRegret(impressions.weights, "Impressions")
	clicks <- getRegret(clicks.weights, "Clicks")
	conversions <- getRegret(conversions.weights, "Conversions")
	impressions <- getSummaryTable(impressions)
	clicks <- getSummaryTable(clicks)
	conversions <- getSummaryTable(conversions)
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
	aggregated <- data[, lapply(.SD, fun, na.rm = TRUE), .SDcols = -c("group_id", "goal")]
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

getInitialSummary <- function(impressions.sequential, clicks.sequential, conversions.sequential) {
	
}