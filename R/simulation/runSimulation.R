require(data.table)
source("R/dataHandling/getSavedData.R")
source("R/dataHandling/getSimulatedData.R")

runSimulation <- function () {
  data <- getSavedData()
	data.simulated <- getSimulatedData(data, "impressions")
	data.simulated <- calculateReturns(data.simulated)
	data.campaigns <- getRegret(data.simulated)
}