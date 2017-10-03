require(data.table)
require(ggplot2)
require(rbenchmark)
require(RcppRoll)
# source(here("R", "dataHandling", "getSavedData.R"), chdir=T)
source("R/dataHandling/getSavedData.R")
source("R/dataHandling/getSimulatedData.R")
source("R/dataHandling/getSequentialData.R")
source("R/calculations/calculateReturns.R")

#Run code -----------------------
runCode <- function() {
	data <- getSavedData()
	
	#Sequential experiment---
	impressions.sequential <- getSequentialData(data, "impressions_cr")
	impressions.returns <- calculateReturns(impressions.sequential)
	impressions.result <- getRegret(data.returns)
	impressions.plot <- getPlotData(impressions.result)
	impressions.time <- getTimeData(impressions.result)

	ggplot(impressions.plot, aes(x = variable, y = value)) + geom_boxplot(outlier.shape = NA) + coord_cartesian(ylim=c(0,8))
	ggplot(impressions.time, aes(x = day.campaign, y = value, color = variable)) + geom_line()

	data.result.avrg = data.result[, lapply(.SD, mean), by=group_id]
	data.result.avrg = data.result[, lapply(.SD, mean), by=day.campaign]

	summary(data.result)

	conversions.sequential <- getSequentialData(data, "conversions_cr")
	conversions.returns <- calculateReturns(conversions.sequential)
	conversions.result <- getRegret(conversions.returns)
	conversions.plot <- getPlotData(conversions.result)
	conversions.time <- getTimeData(conversions.result)
	

	#TO INVESTIGATE:
	#How did this get throug?: 560b5f7ef789b81e438b456a
	#data[group_id == "55fffb4058e7ab2d688b4567"][date == "2016-03-11"]
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

	data.campaigns = data.campaigns[day.campaign > 1]
	return(data.campaigns)
}

getPlotData <- function(data) {
	id.columns = c("group_id", "day.campaign")
	measures = c("regret.equal", "regret.greedy", "regret.egreedy.01", "regret.egreedy.05", 
	"regret.egreedy.decreasing.1", "regret.egreedy.decreasing.10", "regret.softmax.25", 
	"regret.softmax.50", "regret.softmix.25", "regret.softmix.50", "regret.ucb", 
	"regret.ucb.tuned", "regret.thompson")
	data.plot = melt(data, id.vars = id.columns, measure.vars = measures)
	return (data.plot)
}

getTimeData <- function(data) {
	data.excl <- data[, setdiff(names(data), c("group_id")), with = FALSE]
	data.cumulative <- data.excl[, lapply(.SD, sum), by=day.campaign]
	id.columns = c("day.campaign")
	measures = c("regret.equal", "regret.greedy", "regret.egreedy.01", "regret.egreedy.05", 
	"regret.egreedy.decreasing.1", "regret.egreedy.decreasing.10", "regret.softmax.25", 
	"regret.softmax.50", "regret.softmix.25", "regret.softmix.50", "regret.ucb", 
	"regret.ucb.tuned", "regret.thompson")
	data.plot = melt(
		data.cumulative, id.vars = id.columns, measure.vars = measures, variable.name = "variable")
	return (data.plot)
}