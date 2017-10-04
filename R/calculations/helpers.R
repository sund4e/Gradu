require(data.table)
require(cumstats)

# Get the average return befor the date
getAverageReturn <- function (data) {
  temp <- copy(data)
  temp[, r.cumulative := cumsum(r), by=.(id)]
  temp[, r.avrg := ((r.cumulative - r) / (day.adset - 1))]
  return(temp[, r.avrg])
}

# Get the weight of adsets havign history, i.e. non new adsets
# Assumes that day.adset is set as key
getAllocableWeight <- function (data) {
  temp <- copy(data)
  temp[, w.allocable := 0]
  temp[!.(1), w.allocable := sum(w.equal), by=.(group_id, date)]
  return(temp[, w.allocable])
}

# Get the maximum adset return for each day in each campaign (ignoring new adsets)
# Assumes that day.adset is set as key
getMaxForDay <- function (data, column) {
  temp <- copy(data)
  temp[, max := 0]
  temp[!.(1), max := max(get(column)), by=.(group_id, date)]
  return(temp[, max])
}

# Get the maximum adset return for each day in each campaign (ignoring new adsets)
# Assumes that day.adset is set as key (adset observaitons sorted by day)
getAdsetSpend <- function (data, weightColumn) {
  temp <- copy(data)
  temp[, spend := get(weightColumn) * budget]
  temp[, spend.cumulative := cumsum(spend), by=.(id)]
  return(temp[, spend.cumulative - spend])
}

# Get the maximum adset return for each day in each campaign (ignoring new adsets)
# Assumes that day.adset is set as key
getReturnVariance <- function (data) {
  temp <- copy(data)
  temp[, variance := cumvar(r), by=.(id)]
  temp[, variance.lag := shift(variance, 1), by=.(id)]
  return(temp[, variance.lag])
}


# Greedy may end up allocating over 1 when the adsets are avaluated equally good
# (i.e. maximum is 0)
# Quicker to do the data cleanup afterwards
getCorrectedGreedy <- function (data) {
	temp <- copy(data)
	temp[, total.greedy := sum(greedy), by = .(group_id, day.campaign)]
	temp[total.greedy > 0 & greedy == w.allocable, greedy := w.allocable / n.allocable]
	return (temp[, greedy])
}