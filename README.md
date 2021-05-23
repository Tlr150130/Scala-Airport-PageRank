# Scala-Airport-PageRank
Use of Scala to perform Airport PageRank to find largest hubs for airports. Hosted on DataBricks.

## Data
Airport data was provided by https://transtats.bts.gov/.
The airport data was from flights in January 2021.

## DataBricks Link
https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/2359164240640367/574436302816182/1498264311209917/latest.html

## Usefulness
The use of PageRank algorithm on airports have applications in epidemiology to help track the potential spread of diseases among airports. Since the PageRank algorithm considers the number of incoming flights from the source airports, the algorithm can determine which airports have the highest traffic with more accuracy. Placing increased screening or limiting flights in those airports would have the most effect in reducing disease transmission.

## Improvements
The PageRank algorithm used does not consider population size of the source airport nor the passenger size of the flights. That data could be used to impose weights on the flights to further improve accuracy of the PageRank algorithm if used for an epidemiology purpose.
