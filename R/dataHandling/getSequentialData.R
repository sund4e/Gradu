require(data.table)

getSequentialData <- function(data, conversion_column) {
	data.sequential <- data[, .(
		date = date,
		group_id = group_id,
		id = id,
		r = get(conversion_column)
	)]
	return(data.sequential)
}