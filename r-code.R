# From terminal run ssh -L 63333:10.125.0.46:5432 suvi@smartly.io
# where 63333 is the port you want to use for the tunnel & 5432 is the port server listens to, and 10.125.0.46 is the host



# install.packages("RPostgreSQL")
require("RPostgreSQL")
 
# create a connection 
# loads the PostgreSQL driver
drv <- dbDriver("PostgreSQL")

# creates a connection to the postgres database
# note that "con" will be used later in each connection to the database
con <- dbConnect(drv, dbname = "smartly_production",
                 host = "localhost", port = 63333,
                 user = "smartly", password = "")
 
# check for the cartable
dbExistsTable(con, "ad_stats")
# TRUE

# Get companies to dataframe
df <- dbGetQuery(con, statement = paste(
	"SELECT campaigns.id AS campaign_id, campaigns.id::text AS campaign_id_text, campaigns.start_time, campaigns.end_time, campaigns.objective, campaigns.status, COUNT(DISTINCT adgroups.id) AS adgroups_count, SUM(stats.impressions) AS impressions",
	"FROM adgroups",
	"INNER JOIN campaigns ON campaigns.id = adgroups.campaign_id",
	"INNER JOIN (
		SELECT adgroup_id, SUM(impressions) AS impressions
		FROM ad_stats
		WHERE DATE BETWEEN '2016-01-01' AND '2016-04-01'
		GROUP BY adgroup_id
	) stats ON stats.adgroup_id = adgroups.id",
	"WHERE campaigns.objective IS NOT NULL AND stats.impressions > 0 AND campaigns.campaign_type NOT IN ('product_feed', 'reach_frequency', 'page_post_boosting')",
	"GROUP BY campaigns.id",
	"HAVING COUNT(DISTINCT adgroups.id) > 1"
));

#Create function for looping through campaigns
f3 <- function(n){
  df <- data.frame(x = numeric(1000), y = character(1000), stringsAsFactors = FALSE)
  for(i in 1:n){
    df$x[i] <- i
    df$y[i] <- toString(i)
  }
  df
}

#save dataframe
save(df, file="campaigns")

#Load dataframe to df variable
load("campaigns")

# Close PostgreSQL connection 
dbDisconnect(con)