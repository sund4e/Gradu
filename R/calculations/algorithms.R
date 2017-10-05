require(data.table)

# Input keys: day.adset
getOptimalWeight <- function(data, column) {
	temp <- copy(data)
	temp[, max := getMaxForDay(.SD, column)]
	temp[, weight := 0]
  temp[get(column) == max, weight := w.allocable]
  temp[max == 0, weight := w.equal]
	temp[.(1), weight := w.equal]
	return(temp[, weight])
}

# Input keys: day.campign & id
getGreedyWeight <- function(data, avrg.column = 'r.avrg.greedy') {
	temp <- copy(data)
	setkey(temp, day.adset)
	temp[, weight := getOptimalWeight(.SD, avrg.column)]
	setkey(temp, day.campaign, id)
  return(temp[, weight])
}

# Input keys: day.adset
addWeight <- function(data, column, column.true, column.false) {
	data[, max := max(get(column)), by=.(group_id, day.campaign)]
	data[, weight := get(column.false)]
  data[get(column) == max, weight := get(column.true)]
  data[max == 0, weight := w.equal]
	data[.(1), weight := w.equal]
}

getEpsilonGreedyWeight <- function(data, epsilon) {
	temp <- copy(data)
	temp[, exploration.weight := epsilon * (w.allocable / n.allocable)]
	temp[, exploitation.weight := {1 - epsilon} * w.allocable + exploration.weight]
	addWeight(temp, 'r.avrg', 'exploitation.weight', 'exploration.weight')
	return (temp[, weight])
}

getDecreasingEpsilonGreedyWeight <- function(data, constant, avrg.column = 'r.avrg') {
	temp <- copy(data)
	temp[, c := constant]
	temp[, epsilon := constant/day.campaign]
	temp[epsilon > 1, epsilon := 1]
	temp[, exploration.weight := epsilon * (w.allocable / n.allocable)]
	temp[, exploitation.weight := {1 - epsilon} * w.allocable + exploration.weight]
	addWeight(temp, avrg.column, 'exploitation.weight', 'exploration.weight')
	return (temp[, weight])
}

getProbabilityWeights <- function(data, avrg.column = 'r.avrg') {
	temp <- copy(data)
	temp[!.(1), exp := exp(get(avrg.column)/temperature)]
	temp[!.(1), exp.sum := sum(exp), by=.(group_id, day.campaign)]
  temp[!.(1), weight := w.allocable * exp/exp.sum]
  temp[.(1), weight := w.equal]
	return(temp[, weight])
}

getSoftMaxWeight <- function(data, tau) {
	temp <- copy(data)
  temp[, temperature := tau]
  return(getProbabilityWeights(temp))
}

getSoftMixWeight <- function(data, tau, avrg.column) {
	temp <- copy(data)
	setkey(temp, day.adset)
	temp[, temperature := tau * log(day.campaign)/day.campaign]
	temp[, weight := getProbabilityWeights(temp, avrg.column)]
	# When tau -> 0, converges greedy policy
	# For small enough tau the exp becomes infinitive, replace these with greedy policy 
	temp[is.na(weight), weight := getGreedyWeight(.SD, avrg.column)]
	setkey(temp, day.campaign, id)
  return(temp[, weight])
}

getWeightWithCI <- function(data, avrg.column) {
	temp <- copy(data)
	temp[, `:=` (
		ucb = get(avrg.column) + ci,
		lcb = get(avrg.column) - ci
	)]
	temp[, lcb.max := max(lcb), by=.(group_id, day.campaign)]
	temp[, surviving := ucb >= lcb.max]
	temp[surviving == TRUE, n.surviving := .N, by=.(group_id, day.campaign)]
	temp[surviving == TRUE, weight := w.allocable/n.surviving]
	temp[surviving == FALSE, weight := 0]
	return (temp[, weight])
}

getUCBWeight <- function(data, avrg.column) {
	temp <- copy(data)
	setkey(temp, day.adset)
	temp[, ci := 0]
	temp[!.(1), ci := sqrt((2 * ln.spend)/spend.ucb)]
	temp[!.(1), weight := getWeightWithCI(.SD, avrg.column)]
	temp[.(1), weight := w.equal]
	setkey(temp, day.campaign, id)
	return (temp[, weight])
}

getTunedConfidenceInterval <- function(data) {
	temp <- copy(data)
	temp[, ci.variance := sqrt((2 * ln.spend)/spend.ucb.tuned)]
	temp[, V := r.variance + ci.variance]
	temp[, multiplier := 1/4]
	temp[V < multiplier, multiplier := V]
	temp[, ci := sqrt((ln.time/spend.ucb.tuned)*multiplier)]
	return(temp[, ci])
}

getUCBTunedWeight <- function(data, avrg.column) {
	temp <- copy(data)
	setkey(temp, day.adset)
	temp[, ci := 0]
	temp[!.(1, 2), ci := getTunedConfidenceInterval(.SD)]
	temp[!.(1, 2), weight := getWeightWithCI(.SD, avrg.column)]
	temp[.(1, 2), weight := w.equal]
	setkey(temp, day.campaign, id)
	return (temp[, weight])
}

sampleBest <- function(data, rep) {
	temp <- copy(data)
	temp[, best.count := 0]
	for(i in 1:rep) {
		temp[, r.sample := rgamma(1, shape=conversions.thompson, scale=1/spend.thompson), by=.(id)]
		temp[, max := max(r.sample), by=.(group_id, day.campaign)]
		temp[r.sample == max, best.count := best.count + 1]
	}
	return (temp[, best.count])
}

getThompsonWeight <- function(data) {
	temp <- copy(data)
	setkey(temp, day.adset)
	rep <- 100
	temp[!.(1), best.count := sampleBest(.SD, rep)]
	temp[!.(1), sum.best.count := sum(best.count), by = .(group_id, day.campaign)]
	temp[!.(1), weight := best.count / sum.best.count * w.allocable]
	temp[.(1), weight := w.equal]
	setkey(temp, day.campaign, id)
	return (temp[, weight])
}