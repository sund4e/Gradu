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
source("R/simulation/runSimulation.R")

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

	#Simulation
	sim.impressions <- runSimulation(data, "impressions")
	sim.clicks <- runSimulation(data, "clicks")
	sim.conversions <- runSimulation(data, "conversions")
	save(sim.impressions, file = "simulated_impressions")
	save(sim.clicks, file = "simulated_clicks")
	save(sim.conversions, file = "simulated_conversions")
	sim.campaign <- getCombinedPlotData(sim.impressions, sim.clicks, sim.conversions, "campaign")
	ggplot(sim.campaign, aes(x = algorithm, y = relative)) + geom_boxplot() + coord_flip() + facet_wrap( ~ goal, ncol=3) + labs(x="" ,y="Regret relative to optimal allocation")
	summary <- getCombinedSummary(sim.impressions, sim.clicks, sim.conversions)
	fwrite(summary, file = "summary_simulation")

	#FINAL -----
	labels = c("Equal", "Greedy", "Epsilon-greedy 0.1", "Epsilon-greedy 0.5" 
	"Decreasing epsilon-greedy 1", "Decreasing epsilon-greedy 10", "Softmax 1", 
	"Softmax 5", "Softmix 1", "Softmix 5", "UCB", 
	"UCB tuned", "Thompson")
	linetypes = c(seq_len(13))
	colors = c("black", "gray30", "gray50",rep("gray", 10))

	#Plot the optimal returns:
	data.campaign <- getCombinedPlotData(impressions.weights, clicks.weights, conversions.weights, "campaign")
	ggplot(data.campaign, aes(x = r.optimal, linetype = goal)) + scale_x_log10(labels = scales::comma) + geom_density() + labs(y="Density" ,x="Reward per unit of budget", linetype = "Goal")
	data.campaign[, .(Avrg = mean(r.optimal), SD = sd(r.optimal)), by=goal]
	#Summary table:
	summary <- getCombinedSummary(impressions.weights, clicks.weights, conversions.weights)
	fwrite(summary, file = "summary_sequential")
	#Plot regrets in boxplot:
	data.campaign <- getCombinedPlotData(impressions.weights, clicks.weights, conversions.weights, "campaign")
	ggplot(data.campaign, aes(x = algorithm, y = relative)) + geom_boxplot() + coord_flip() + facet_wrap( ~ goal, ncol=3) + labs(x="" ,y="Regret relative to optimal allocation")
	#...with average:
	ggplot(data.campaign, aes(x = algorithm, y = relative)) + geom_boxplot() + coord_flip() + facet_wrap( ~ goal, ncol=3) + stat_summary(fun.y=mean, colour="darkred", geom="point", shape=18, size=3)

	#Timeseries
	data.time <- getCombinedPlotData(impressions.weights, clicks.weights, conversions.weights, FALSE)
	ggplot(data.time, aes(x = day.campaign, y = relative, color = goal)) + geom_smooth() + facet_wrap( ~ algorithm, ncol=4) + scale_colour_grey()
	ggplot(data.time, aes(x = day.campaign, y = relative, color = goal)) + geom_line() + facet_wrap( ~ algorithm, ncol=4) + scale_colour_grey()

	#Cumulative returns vs. predictability
	data.time <- getCombinedPlotData(impressions.weights, clicks.weights, conversions.weights, FALSE)
	data.time$algorithm <- factor(data.time$algorithm, levels = unique(data.time$algorithm))
	data.time[, r.cumulative := cumsum(r * 100), by = .(group_id, day.campaign, algorithm)]
	ggplot(data.time, aes(x = r.cumulative, y = relative, linetype = algorithm)) + geom_smooth(method = "glm", se = FALSE, colour="black", size=0.5) + scale_colour_grey() + scale_x_log10(labels = scales::comma) + scale_y_continuous(limits = c(0, NA)) + labs(x="Cumulative returns", y="Regret relative to optimal allocation", linetype = "Goal")

	#Plot regret for using different reward than the one given for the algorithms 
	test <- getReturnForDifferentGoals(impressions.weights, conversions.weights, clicks.sequential, conversions.sequential)
	ggplot(test, aes(x = algorithm, y = relative)) + geom_boxplot() + coord_flip() + facet_wrap( ~ goal, ncol=3)

	#Nice to have
	#Correlation of regret and standard deviation:
	ggplot(data.campaign[goal == "Impressions"], aes(x = stdErr, y = relative, color = goal)) + geom_point() + facet_wrap( ~ algorithm, ncol=4)+ geom_smooth()
	#Correlation if regret and numer of ad sets:
	ggplot(data.campaign, aes(x = adsets, y = relative, color = goal)) + facet_wrap( ~ algorithm, ncol=4)+ geom_smooth()
	# ------
}

getReturnForDifferentGoals <- function (impressions.weights, clicks.weights, conversions.weights) {
	impressions <- copy(impressions.weights)
	clicks <- copy(clicks.weights)
	conversions <- copy(conversions.weights)
	keys = c("date", "group_id", "id")
	setkeyv(impressions, keys)
	setkeyv(clicks, keys)
	setkeyv(conversions, keys)

	impressions.clicks <- impressions[, `:=` (
		optimal = clicks.weights[, optimal],
		r = clicks.weights[, r]
	)]
	impressions.conversions <- impressions[, `:=` (
		optimal = conversions.weights[, optimal],
		r = conversions.weights[, r]
	)]
	clicks.conversions <- clicks[, `:=` (
		optimal = conversions.weights[, optimal],
		r = conversions.weights[, r]
	)]
	data <- getCombinedPlotData(impressions.clicks, impressions.conversions, clicks.conversions, "campaign")
	return (data)
}

getCombinedPlotData <- function(impressions.weights, clicks.weights, conversions.weights, cumulative = "campaign") {
	impressions <- getRegret(impressions.weights, "Impressions", cumulative)
	clicks <- getRegret(clicks.weights, "Clicks", cumulative)
	conversions <- getRegret(conversions.weights, "Conversions", cumulative)
	combined <- rbindlist(list(impressions, clicks, conversions))
	key <- if(cumulative == FALSE) c("group_id", "day.campaign") else if (cumulative == "time") "day.campaign" else "group_id"
	plot <- getPlotData(combined, c(key, "goal"))
	return (plot)
}

getRegret <- function(data.weights, goal = "Impressions", cumulative = "campaign") {
	data <- copy(data.weights)
	keys = c("group_id", "day.campaign")
	if("run" %in% names(data)) {
		keys = c(keys, "run")
	}
	adsets <- data[, .(adsets = .N), keyby = .(group_id, day.campaign)]
	stdErr <- data[, .(stdErr = sd(r)), keyby = .(group_id, day.campaign)]

	data[, (returns) := .SD * data[, r], .SDcols = columns]
	data <- data[, lapply(.SD, sum, na.rm = TRUE), keyby = .(group_id, day.campaign), .SDcols = returns]
	data <- data[day.campaign > 1] #Remove reduntant rows [r.optimal > 0][day.campaign <= 100]
	data[, (regrets) := data[, r.optimal] - .SD, .SDcols = returns[-1]]	
	#Fix negative regrets (due to rounding in alloction) to zero
	data[, (regrets) := lapply(.SD, function(regret) lapply(regret, max, 0)), .SDcols = regrets]
	data[, (regrets) := lapply(.SD, as.numeric), .SDcols = regrets]

	if (cumulative == "campaign") {
		data <- data[, lapply(.SD, sum), by = group_id]
		adsets <- adsets[, .(adsets = mean(adsets)), keyby = .(group_id)]
		stdErr <- stdErr[, .(stdErr = mean(stdErr)), keyby = .(group_id)]
	}

	if (cumulative == "time") {
		data <- data[, lapply(.SD, sum), keyby = day.campaign, .SDcols = -c("group_id")]
		adsets <- adsets[, .(adsets = mean(adsets)), keyby = day.campaign]
		stdErr <- stdErr[, .(stdErr = mean(stdErr)), keyby = day.campaign]
	}

	data[, (relative) := .SD / data[, r.optimal], .SDcols = regrets]
	data <- adsets[data]
	data <- stdErr[data]
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
	temp <- data[, .(adsets, stdErr, r.optimal), keyby = keys]
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
	regret <- data.transposed[Algorithm %like% "^regret"][, SD := NULL]
	# relative <- data.transposed[Algorithm %like% "^relative"][, SD := NULL]
	relative <- data.transposed[Algorithm %like% "^relative"]
	parseAlgorithm(regret)
	parseAlgorithm(relative)
	data <- regret[relative]
	setnames(data, c("Algorithm", "Average", "Relative", "SD"))
	return(data)
}

parseAlgorithm <- function(data) {
	data[, Algorithm := gsub("^\\w+\\.", "", Algorithm)]
	setkey(data, Algorithm)
}