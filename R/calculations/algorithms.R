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
getGreedyWeight <- function(data) {
	temp <- copy(data)
	setkey(temp, day.adset)
	temp[, weight := getOptimalWeight(.SD, 'r.avrg.greedy')]
	setkey(temp, day.campaign, id)
  return(temp[, weight])
}

# Input keys: day.adset
addWeight <- function(data, column, column.true, column.false) {
	max.column = paste(column, 'max', sep='.')
	data[, weight := get(column.false)]
  data[get(column) == get(max.column), weight := get(column.true)]
  data[get(max.column) == 0, weight := w.equal]
	data[.(1), weight := w.equal]
}

getEpsilonGreedyWeight <- function(data, epsilon) {
	temp <- copy(data)
	temp[, exploration.weight := epsilon * (w.allocable / n.allocable)]
	temp[, exploitation.weight := {1 - epsilon} * w.allocable + exploration.weight]
	addWeight(temp, 'r.avrg', 'exploitation.weight', 'exploration.weight')
	return (temp[, weight])
}

getDecreasingEpsilonGreedyWeight <- function(data, constant) {
	temp <- copy(data)
	temp[, c := constant]
	temp[, epsilon := constant/day.campaign]
	temp[epsilon > 1, epsilon := 1]
	temp[, exploration.weight := epsilon * (w.allocable / n.allocable)]
	temp[, exploitation.weight := {1 - epsilon} * w.allocable + exploration.weight]
	addWeight(temp, 'r.avrg', 'exploitation.weight', 'exploration.weight')
	return (temp[, weight])
}

getProbabilityWeights <- function(data) {
	data[!.(1), exp := exp(r.avrg/temperature)]
	# data[is.infinite(exp), exp := .Machine$double.xmax]
	data[!.(1), exp.sum := sum(exp), by=.(group_id, day.campaign)]
  data[!.(1), weight := w.allocable * exp/exp.sum]
  data[.(1), weight := w.equal]
	return(data[, weight])
}

getSoftMaxWeight <- function(data, tau) {
	temp <- copy(data)
  temp[, temperature := tau]
  return(getProbabilityWeights(temp))
}

getSoftMixWeight <- function(data, tau) {
	temp <- copy(data)
	temp[, temperature := tau * log(day.campaign)/day.campaign]
  return(getProbabilityWeights(temp))
}

getWeightWithCI <- function(data) {
	temp <- copy(data)
	temp[, `:=` (
		ucb = r.avrg + ci,
		lcb = r.avrg - ci
	)]
	temp[, lcb.max := max(lcb), by=.(group_id, day.campaign)]
	temp[, surviving := ucb >= lcb.max]
	temp[surviving == TRUE, n.surviving := .N, by=.(group_id, day.campaign)]
	temp[surviving == TRUE, weight := w.allocable/n.surviving]
	temp[surviving == FALSE, weight := 0]
	return (temp[, weight])
}

getUCBWeight <- function(data) {
	temp <- copy(data)
	setkey(temp, day.adset)
	temp[, ci := 0]
	temp[!.(1), ci := sqrt((2 * ln.spend)/sum.spend.ucb)]
	temp[!.(1), weight := getWeightWithCI(.SD)]
	temp[.(1), weight := w.equal]
	setkey(temp, day.campaign, id)
	return (temp[, weight])
}

getTunedConfidenceInterval <- function(data) {
	temp <- copy(data)
	temp[, ci.variance := sqrt((2 * ln.spend)/sum.spend.ucb.tuned)]
	temp[, V := r.variance + ci.variance]
	temp[, multiplier := 1/4]
	temp[V < multiplier, multiplier := V]
	temp[, ci := sqrt((ln.time/sum.spend.ucb.tuned)*multiplier)]
	return(temp[, ci])
}

getUCBTunedWeight <- function(data) {
	temp <- copy(data)
	setkey(temp, day.adset)
	temp[, ci := 0]
	temp[!.(1, 2), ci := getTunedConfidenceInterval(.SD)]
	temp[!.(1, 2), weight := getWeightWithCI(.SD)]
	temp[.(1, 2), weight := w.equal]
	setkey(temp, day.campaign, id)
	return (temp[, weight])
}

sampleBest <- function(data, rep) {
	temp <- copy(data)
	temp[, best.count := 0]
	for(i in 1:rep) {
		temp[, r.sample := rgamma(1, shape=sum.r.count, scale=1/sum.spend.thompson), by=.(id)]
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