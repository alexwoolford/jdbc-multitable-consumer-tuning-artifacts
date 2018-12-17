rm(list = ls())

library(RMySQL)
library(dplyr)
library(ggplot2)
library(scales)

con <- dbConnect(RMySQL::MySQL(), dbname = "jdbc_benchmark", user = "root", password = "V1ctoria", host = "deepthought")
rs <- dbSendQuery(con, "select * from benchmark_start natural join benchmark_result order by start_time")
data <- dbFetch(rs)
dbDisconnect(con)

data$start_time <- as.POSIXct(data$start_time)
data$end_time <- as.POSIXct(data$end_time)

data <- mutate(data, runtime = end_time - start_time)
data <- mutate(data, cells = rows * columns)
data <- mutate(data, rows_per_second = rows / as.numeric(runtime))
data <- mutate(data, cells_per_second = cells / as.numeric(runtime))
data <- subset(data, batch_size %in% c(100, 1000, 10000))

data <- mutate(data, desc = paste(paste("table: ", table_name),
                                  paste("rows: ", comma(rows)),
                                  paste("columns:", comma(columns)),
                                  sep = "\n"))
data <- mutate(data, batch_size = paste("batch size: ", batch_size))
data <- mutate(data, partition_size = paste("partition size: ", partition_size))
data <- subset(data,  table_name %in% c("customer_demographics"))

title = unique(data$desc)
qplot(threads, rows_per_second, data=data, main=title, geom="point", color=partition_size, shape=batch_size) +
  scale_y_continuous(label=comma, breaks=seq(0, 200000, 10000)) +
  scale_x_continuous(breaks=seq(1:12))
