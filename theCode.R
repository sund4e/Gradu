# From terminal run ssh -L 63333:10.125.0.46:5432 suvi@dev.smartly.io
# where 63333 is the port you want to use for the tunnel & 5432 is the port server listens to, and 10.125.0.46 is the host
# -L tells that you do local port forwarding, i.e. forward the trafic from your local port 63333 to 10.125.0.46:5432
# dev.smartly.io is the ssh host you use to connect

#NEW
#

# For mongo, you need to connect to the PRIMARY
# ssh -L 64444:127.0.0.1:27017 suvi@indymongo1.smartly.io
# ask for host & port from db: db.serverCmdLineOpts()

# To fix ssh connections remove respective line from known hosts (check by trying to ssh in terminal)
# nano .ssh/known_hosts
# Move to correct line: Ctrl + _
# Remove line (cut it): Ctrl + K
# Save & try to ssh again  

# install.packages("RPostgreSQL")
# install.packages("rmongodb")
# get all required packages & libraries
# tidyr
require("RPostgreSQL")
require(data.table)
require(ggplot2)
require(plyr)
require(dplyr)
require(rmongodb)

 
# create a connection 
# loads the PostgreSQL driver
drv <- dbDriver("PostgreSQL")

# creates a connection to the postgres database
# note that "con" will be used later in each connection to the database
con <- dbConnect(drv, dbname = "smartly_production",
                 host = "localhost", port = 63333,
                 user = "smartly", password = "")

#-- Create data tables from postgre --#
adsets_q1 <- dbGetQuery(con, statement = paste(
	"SELECT campaigns.id AS campaign_id, ad_sets.adgroup_id, ad_sets.date, campaigns.objective
	FROM campaigns
	INNER JOIN (
		SELECT adgroup_id, date, campaign_id
		FROM ad_stats
		WHERE ad_stats.date BETWEEN '2015-01-01' AND '2015-03-31'
		GROUP BY campaign_id, adgroup_id, date
		HAVING SUM(impressions) > 0
		) AS ad_sets
		ON ad_sets.campaign_id = campaigns.id
	WHERE campaigns.objective IS NOT NULL AND campaigns.campaign_type NOT IN ('product_feed', 'reach_frequency', 'page_post_boosting')"
));

adsets_q2 <- dbGetQuery(con, statement = paste(
	"SELECT campaigns.id AS campaign_id, ad_sets.adgroup_id, ad_sets.date, campaigns.objective
	FROM campaigns
	INNER JOIN (
		SELECT adgroup_id, date, campaign_id
		FROM ad_stats
		WHERE ad_stats.date BETWEEN '2015-04-01' AND '2015-06-30'
		GROUP BY campaign_id, adgroup_id, date
		HAVING SUM(impressions) > 0
		) AS ad_sets
		ON ad_sets.campaign_id = campaigns.id
	WHERE campaigns.objective IS NOT NULL AND campaigns.campaign_type NOT IN ('product_feed', 'reach_frequency', 'page_post_boosting')"
));

adsets_q3 <- dbGetQuery(con, statement = paste(
	"SELECT campaigns.id AS campaign_id, ad_sets.adgroup_id, ad_sets.date, campaigns.objective
	FROM campaigns
	INNER JOIN (
		SELECT adgroup_id, date, campaign_id
		FROM ad_stats
		WHERE ad_stats.date BETWEEN '2015-07-01' AND '2015-09-30'
		GROUP BY campaign_id, adgroup_id, date
		HAVING SUM(impressions) > 0
		) AS ad_sets
		ON ad_sets.campaign_id = campaigns.id
	WHERE campaigns.objective IS NOT NULL AND campaigns.campaign_type NOT IN ('product_feed', 'reach_frequency', 'page_post_boosting')"
));

adsets_q4 <- dbGetQuery(con, statement = paste(
	"SELECT campaigns.id AS campaign_id, ad_sets.adgroup_id, ad_sets.date, campaigns.objective
	FROM campaigns
	INNER JOIN (
		SELECT adgroup_id, date, campaign_id
		FROM ad_stats
		WHERE ad_stats.date BETWEEN '2015-10-01' AND '2015-12-31'
		GROUP BY campaign_id, adgroup_id, date
		HAVING SUM(impressions) > 0
		) AS ad_sets
		ON ad_sets.campaign_id = campaigns.id
	WHERE campaigns.objective IS NOT NULL AND campaigns.campaign_type NOT IN ('product_feed', 'reach_frequency', 'page_post_boosting')"
));

#join the above data frames & create data.table
adsets_h1 <- rbind(adsets_q1, adsets_q2)
adsets_h2 <- rbind(adsets_q3, adsets_q4)
adsets_df <- rbind(adsets_h1, adsets_h2)
# save the dataframe: save(adsets_df, file = "adsets_df_050316.RData")
# load dataframe: load("adsets_df_050316.RData")

#Create table
adsets <- as.data.table(adsets_df)
# save(adsets_all, file = "adsets_all_170416.RData")

#-------------- NEW CODE --------------#
# Create table that has all the campaigns campaigns <- adsets[, list(adsets=.N), by=c("campaign_id", "date")]
# Check that campaign has correct number of adsets adsets[campaign_id == "\\x54b8f33058e7ab801b8b456d" & date == as.Date("2015-01-27"), ]

# Create a column to adsets table that shows how many adsets the campaign has at that date
# Filter out campaign dates that only have one ad set
adsets[, adsets_in_campaign:=.N, by=c("campaign_id", "date")]
adsets.onlymultiple <- adsets[adsets_in_campaign > 1, ]

# Create a column to adsets table that shows how many days the adset has been running (adsets running less than a day can't have budget allocation)
# If you ad an adset, the campaign budget is respectively increased (no allocation happens)
# Filter out campaigns that have been running only one day with several adsets
adsets.onlymultiple[, days_adset_running:=.N, by="adgroup_id"]
adsets.filtered <- adsets.onlymultiple[days_adset_running > 1, ]

# Get daily budgets, objectives, and PBA settings
adsets.filtered[order(adgroup_id, date), ]

###MONGO STUFF###
#Query mongo for campaign objectives & PBA settings
campaigns <- adsets.filtered[, list(adsets=.N), by=c("campaign_id", "date")] #number of campaign dates
campaigns.ids <- campaigns[, list(days_live=.N), by="campaign_id"] #number of campaigns

# connect to mongo & create buffer
mongo <- mongo.create(host = "localhost:64444")
coll <- "changelog.campaigns_changelog"
buf <- mongo.bson.buffer.create()

# build the query
i <- 1 #LOOP HERE
cursorid <- campaigns.ids[i, campaign_id]
parsed_id <- substr(cursorid, 3, nchar(cursorid))
oid <- mongo.oid.from.string(parsed_id)

mongo.bson.buffer.append(buf, "object_id", oid)
mongo.bson.buffer.start.array(buf, "$or")

mongo.bson.buffer.start.object(buf, "0")
mongo.bson.buffer.start.object(buf, 'data.objective')
mongo.bson.buffer.append(buf, '$exists', 'true')
mongo.bson.buffer.finish.object(buf)
mongo.bson.buffer.finish.object(buf)

mongo.bson.buffer.start.object(buf, "1")
mongo.bson.buffer.start.object(buf, 'data.smartly_budget_allocation')
mongo.bson.buffer.append(buf, '$exists', 'true')
mongo.bson.buffer.finish.object(buf)
mongo.bson.buffer.finish.object(buf)

mongo.bson.buffer.finish.object(buf)
b <- mongo.bson.from.buffer(buf)

# query mongo
cursor <- mongo.find(mongo, coll, b)

# collect data and print it
while(mongo.cursor.next(cursor)) {
  value <- mongo.cursor.value(cursor)
  list <- mongo.bson.to.list(value)
  cursordate <- as.Date(as.POSIXct(list[["time"]], origin="1970-01-01"))
  print(paste("Date:", cursordate, " list:", list))
  
  if ("objective" %in% names(list[["data"]])) {
  	cursorobjective <- list[["data"]][["objective"]]
  	print(paste("Objective:", cursorobjective))

  	if(!is.null(previous_objective)) {
  		campaigns[id == cursorid & (date < cursordate | date >= change_date), objective := previous_objective]
  		print(paste("Saving: objective ", previous_objective, " ", change_date, " - ", cursordate))
  	}

  	previous_objective <- cursorobjective
  	change_date <- cursordate
  }
}

campaigns[id == cursorid & date >= change_date, objective := previous_objective]

#--------------------------------------#

#Create table for campaigns that contains only data with mutilple ad sets
adsets_dummygroup <-  adsets_all[, groupby := paste(campaign_id, date, sep="")] #column based on which to group
by_campaign <- group_by(adsets_dummygroup, groupby)
campaigns <- summarise(
	by_campaign,
	campaign_id = campaign_id,
	date = date,
	adset_count = n(),
	objective = objective)
#summarise creates a table with same amount of rows where each row has it's summary based on the group_by, following cleans up the table
campaigns <- filter(campaigns, adset_count > 1)
campaigns <- distinct(select(campaigns, campaign_id, date, adset_count, objective))

#Join campaign table to adsets so that only rows for dates when multiple adsets exist in campaign are kept
setkey(adsets, campaign_id, date)
setkey(campaigns, campaign_id, date)

adsets <- adsets_all[campaigns, nomatch=0]
# save(adsets, file = "adsets_170416.RData")
# save(campaigns, file = "campaigns_170416.RData")
# adsets_filtered <- adsets

## GET THE CHANGELOG ##
campaigns[, budget := NA]
campaigns[, budget_type := NA]
campaigns[, pba := NA]
previous_budget <- NULL
campaign_ids = distinct(select(campaigns, campaign_id))

# connect to mongo & create buffer
mongo <- mongo.create(host = "localhost:64444")
coll <- "changelog.campaigns_changelog"
buf <- mongo.bson.buffer.create()

# build the query
i <- 1
cursorid <- campaign_ids[i, campaign_id]
parsed_id <- substr(cursorid, 3, nchar(id))
oid <- mongo.oid.from.string(parsed_id)

mongo.bson.buffer.append(buf, "object_id", oid)
mongo.bson.buffer.start.array(buf, "$or")

mongo.bson.buffer.start.object(buf, "0")
mongo.bson.buffer.start.object(buf, 'data.budget')
mongo.bson.buffer.append(buf, '$exists', 'true')
mongo.bson.buffer.finish.object(buf)
mongo.bson.buffer.finish.object(buf)

mongo.bson.buffer.start.object(buf, "1")
mongo.bson.buffer.start.object(buf, 'data.smartly_budget_allocation')
mongo.bson.buffer.append(buf, '$exists', 'true')
mongo.bson.buffer.finish.object(buf)
mongo.bson.buffer.finish.object(buf)

mongo.bson.buffer.finish.object(buf)
b <- mongo.bson.from.buffer(buf)

# query mongo
cursor <- mongo.find(mongo, coll, b)

# collect data and print it
while(mongo.cursor.next(cursor)) {
  value <- mongo.cursor.value(cursor)
  list <- mongo.bson.to.list(value)
  cursordate <- as.Date(as.POSIXct(list[["time"]], origin="1970-01-01"))
  if ("budget" %in% names(list[["data"]])) {
  	budget_type <- list[["data"]][["budget"]][["type"]]
  	budget <- list[["data"]][["budget"]][[budget_type]]
  }

  #add data to campaigns table
  # access columns whith and (&) / or (|) conditions: campaigns(id == cursorid & date < cursordate & date > previous_budget["date"])
  if (!is.null(previous_budget)) {
  	campaigns[c(id == cursorid, date < cursordate, date > previous_budget[["date"]]), budget_type := previous_budget[["budget_type"]]]
  	campaigns[c(id == cursorid, date < cursordate, date > previous_budget[["date"]]), budget := previous_budget[["budget"]]]
  } else {
  	previous_budget <- list()
  }

  previous_budget["date"] <- date
  previous_budget["budget_type"] <- budget_type
  previous_budget["budget"] <- budget
}

i = 1
id = campaign_ids[i, campaign_id]


### GET THE STATS ###
stats <- adsets
stats[, objective := NULL]
stats[, adset_count := NULL]
stats[, i.objective := NULL]
stats[, impressions := NA]
stats[, spent := NA]
stats[, conversions := NA]

setkey(adsets, adgroup_id, date)
setkey(stats, adgroup_id, date)

i = 1
id <- stats[i, adgroup_id]
date <- stats[i, date]
campaign <- campaigns[stats[i, campaign_id]]

if (nrow(campaign) > 1) {
	stop(paste("multiple campaigns with same id:", id)
} else if (nrow(campaign) < 1) {
	stop(paste("no campaign found:", id)
}

conversion <- campaign[1, objective]

result <- dbGetQuery(con, statement = paste(
	"SELECT SUM(impressions) AS impressions, SUM(spent) AS spent, SUM((actions_28d_view->'",conversion,"')::text::INT) AS conversions
	FROM ad_stats
	WHERE adgroup_id = '",id,"' AND DATE = '",date,"'
	GROUP BY adgroup_id, date",
sep=""));
if (nrow(result) != 1) {
	stop("SQL query returned multiple/no rows with id:", id)
}
stats[i, impressions] = result[1, impressions]
stats[i, spent] = result[1, spent]
stats[i, conversions] = result[1, conversions]


