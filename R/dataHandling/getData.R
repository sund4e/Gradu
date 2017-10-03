require(rmongodb)
require("RPostgreSQL")

getData <- function(){
	# create a connection 
	# loads the PostgreSQL driver
	drv <- dbDriver("PostgreSQL")

	# creates a connection to the postgres database
	# note that "con" will be used later in each connection to the database
	con <- dbConnect(drv, dbname = "smartly_production",
					 host = "localhost", port = 63333,
					 user = "bi", password = "")

	#Save the end and start days of months
	dates.start <- seq(as.Date("2016-03-01"),length=10,by="months")
	dates.end <- seq(as.Date("2016-04-01"),length=10,by="months")-1

	print("getting data")
	# get the data
	for (month in 1:length(dates.start)) {
		start <- dates.start[month]
		end <- dates.end[month]
		cat(paste("Dates", start, "-", end, sep=" "))
		data.month <- dbGetQuery(con, statement = paste(
			"SELECT 
			DATE,
			group_id,
			adset->>'id' adset_id,
			adset->>'budget' budget,
			adset->>'bid_amount' bid,
			adset->>'bid_is_autobid' autobid,
			adset->>'reachestimate' reach_estimate,
			adset->>'bid_billing_event' billing_event,
			adset->>'goal' optimization_goal,
			adset#>'{spent_ts,0}' spend,
			adset#>'{impressions_ts,0}' impressions,
			adset#>'{link_clicks_ts,0}' clicks,
			adset#>'{conversions_ts,0}' conversions
			FROM buster.pba_input_data_view_by_date
			CROSS JOIN json_array_elements(input_data_json::json) adset
			WHERE 
			DATE BETWEEN '", start, "' AND '", end, "'", sep=""
		))

		#Add up all data to data.all variable
		if (exists("data.all")) {
			data.all <- rbind(data.all, data.month)
		} else {
			data.all <- data.month
		}

		cat(" ", "\u2713")
		cat("\n")
	}
	#save the dataframe: 
	filename <- paste("data", Sys.time(),".RData", sep="")
	save(data.all, file = filename)
	cat("Data saved to file: ", filename, "\n")

	#close connection and return data
	cat("Closing connection... ")
	cat(dbDisconnect(con))
	return(data.all)
}