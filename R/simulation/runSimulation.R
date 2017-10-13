require(data.table)
source("R/dataHandling/getSimulatedData.R")
source("R/calculations/calculateReturns.R")

cols = c("group_id", "day.campaign", "r", "optimal", "equal", "greedy", "egreedy.01", "egreedy.05", 
	"egreedy.decreasing.1", "egreedy.decreasing.10", "softmax.1", 
	"softmax.5", "softmix.1", "softmix.5", "ucb", 
	"ucb.tuned", "thompson")

runSimulation <- function (data, goal = "impressions") {
	data.ready = data.table()
	runs <- 100
	for(i in 1:runs) {
		cat("Round ", i, "/", runs, ": \n", sep="");
		simulated <- getSimulatedData(data, goal)
		weights <- calculateReturns(simulated)
		weights <- weights[, .SD, .SDcols = cols] #Save memory by removing reduntant columns
		weights[, run := i]
		data.ready = rbindlist(list(data.ready, weights))
	}

	return(data.ready)
}