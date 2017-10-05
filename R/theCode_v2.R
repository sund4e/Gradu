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

#Run code -----------------------
runCode <- function() {
	data <- getSavedData()

	#getSequential data
	impressions.sequential <- getSequentialData(data, "impressions_cr")
	clicks.sequential <- getSequentialData(data, "clicks_cr")
	conversions.sequential <- getSequentialData(data, "conversions_cr")
	
	#Sequential experiment---
	impressions.sequential <- getSequentialData(data, "impressions_cr")
	impressions.weights <- calculateReturns(impressions.sequential)
	impressions.result <- getRegret(impressions.weights)
	impressions.summary <- getSummaryTable(impressions.result)
	impressions.plot <- getPlotData(impressions.result)
	impressions.time <- getTimeData(impressions.result)
	ggplot(impressions.plot, aes(x = algorithm, y = value)) + geom_boxplot(outlier.shape = NA) + coord_cartesian(ylim=c(0,8))
	ggplot(impressions.time, aes(x = day.campaign, y = value, color = algorithm)) + geom_line()
	ggplot(impressions.time, aes(x = day.campaign, y = cumulative, color = algorithm)) + geom_line() + coord_cartesian(ylim=c(0, 100000))

	clicks.sequential <- getSequentialData(data, "clicks_cr")
	clicks.returns <- calculateReturns(clicks.sequential)
	clicks.result <- getRegret(clicks.returns)
	clicks.plot <- getPlotData(clicks.result)
	clicks.time <- getTimeData(clicks.result)

	conversions.sequential <- getSequentialData(data, "conversions_cr")
	conversions.returns <- calculateReturns(conversions.sequential)
	conversions.result <- getRegret(conversions.returns)
	conversions.plot <- getPlotData(conversions.result)
	conversions.time <- getTimeData(conversions.result)
	ggplot(conversions.plot, aes(x = variable, y = value)) + geom_boxplot(outlier.shape = NA) + coord_cartesian(ylim=c(0, 0.025))
	ggplot(conversions.time, aes(x = day.campaign, y = log(value), color = variable)) + geom_line()

	impressions.returns <- calculateReturns(impressions.sequential)
	clicks.returns <- calculateReturns(clicks.sequential)
	conversions.returns <- calculateReturns(conversions.sequential)

	impressions.result <- getRegret(impressions.returns)
	clicks.result <- getRegret(clicks.returns)
	conversions.result <- getRegret(conversions.returns)
	result <- combineResults(impressions.result, clicks.result, conversions.result)

	data.result.avrg = data.result[, lapply(.SD, mean), by=group_id]
	data.result.avrg = data.result[, lapply(.SD, mean), by=day.campaign]

	system.time({test <- testing(impressions.sequential)}) #~624.965
	data <- getSavedData()
	impressions.sequential <- getSequentialData(data, "impressions_cr")
	test.result <- getRegret(test)
	test.summary <- getSummaryTable(test.result)
	test.plot <- getPlotData(test.result)
	ggplot(test.plot, aes(x = variable, y = value)) + geom_boxplot(outlier.shape = NA) + coord_cartesian(ylim=c(0, 0.025))
	test <- testing(impressions.sequential[group_id == "5527905fd1a561f72d8b456c"])

	#Fix these
	#impressions.weights[group_id == "56b858a7f789b891048b456f"][day.campaign == 2]
	#sum of weights always 1 (especially greedy)
	#equal allocations correct (sum = 1)
	# impressions.weights[group_id == "5541b3c7d1a5612e548b458a"]
	# optimal broken: [group_id == "5527905fd1a561f72d8b456c"]

	# impressions.returns[group_id == "56d5a4d7f789b8ef188b4592"]

	summary(data.result)
}

#Anaysing data ------------------------------
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
		r.softmax.5 = sum(softmax.5 * r),
		r.softmix.25 = sum(softmix.25 * r),
		r.softmix.5 = sum(softmix.5 * r),
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
		regret.softmax.5 = r.optimal - r.softmax.5,
		regret.softmix.25 = r.optimal - r.softmix.25,
		regret.softmix.5 = r.optimal - r.softmix.5,
		regret.ucb = r.optimal - r.ucb,
		regret.ucb.tuned = r.optimal - r.ucb.tuned,
		regret.thompson = r.optimal - r.thompson
	)]

	data.campaigns[, `:=` (
		relative.equal = regret.equal/r.optimal,
		relative.greedy = regret.greedy/r.optimal,
		relative.egreedy.01 = regret.egreedy.01/r.optimal,
		relative.egreedy.05 = regret.egreedy.05/r.optimal,
		relative.egreedy.decreasing.1 = regret.egreedy.decreasing.1/r.optimal,
		relative.egreedy.decreasing.10 = regret.egreedy.decreasing.10/r.optimal,
		relative.softmax.25 = regret.softmax.25/r.optimal,
		relative.softmax.5 = regret.softmax.5/r.optimal,
		relative.softmix.25 = regret.softmix.25/r.optimal,
		relative.softmix.5 = regret.softmix.5/r.optimal,
		relative.ucb = regret.ucb/r.optimal,
		relative.ucb.tuned = regret.ucb.tuned/r.optimal,
		relative.thompson = regret.thompson/r.optimal
	)]

	data.campaigns = data.campaigns[day.campaign > 1]
	return(data.campaigns)
}

getPlotData <- function(data) {
	id.columns = c("group_id", "day.campaign")
	measures = c("regret.equal", "regret.greedy", "regret.egreedy.01", "regret.egreedy.05", 
	"regret.egreedy.decreasing.1", "regret.egreedy.decreasing.10", "regret.softmax.25", 
	"regret.softmax.5", "regret.softmix.25", "regret.softmix.5", "regret.ucb", 
	"regret.ucb.tuned", "regret.thompson")
	data.plot = melt(data, id.vars = id.columns, measure.vars = measures)
	return (data.plot)
}

getTimeData <- function(data) {
	data.excl <- data[, setdiff(names(data), c("group_id")), with = FALSE]
	data.day <- data.excl[, lapply(.SD, sum), by=day.campaign]
	id.columns = c("day.campaign")
	measures = c("regret.equal", "regret.greedy", "regret.egreedy.01", "regret.egreedy.05", 
	"regret.egreedy.decreasing.1", "regret.egreedy.decreasing.10", "regret.softmax.25", 
	"regret.softmax.5", "regret.softmix.25", "regret.softmix.5", "regret.ucb", 
	"regret.ucb.tuned", "regret.thompson")
	data.plot = melt(
		data.day, id.vars = id.columns, measure.vars = measures, variable.name = "algorithm")
	data.plot[, cumulative := cumsum(value), by=algorithm]
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
	aggregated <- data[, lapply(.SD, fun, na.rm = TRUE), .SDcols = -c("group_id")]
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