
#CUSTOMER ANALYSIS PROJECT

#installing packages.
install.packages("RPostgreSQL")
install.packages("DBI")
install.packages("RPostgres")
library(RPostgres)
library(RPostgreSQL)
library(DBI)
library(dplyr)
connect <- dbConnect(
  RPostgres::Postgres(),
  dbname = "Customer_ticketing_project",
  host = "localhost",
  port = 5432,
  user = "dell",
  password = "dell4403"
)

# confirm connection

result <- dbGetQuery(connect, "SELECT version()")
result
# to see the tables in my DB
tables <- dbListTables(connect)
tables

customer_service <- "customer_service"
project_data <- paste("SELECT * FROM", customer_service)

customer_data <- dbGetQuery(connect, project_data )
customer_data
View(customer_data )
head(customer_data)

#Data Cleaning/Manipulation
#Deleting unwanted columns from the table

unwanted_columns<- c("ticket_subject","ticket_description","ticket_resolution")
customer_data<- customer_data[, !(names(customer_data)%in% unwanted_columns)]

#Modified data
customer_data
View(customer_data)


# Splitting the data into 2 tables one with customer details and another with ticket details

install.packages("dplyr")
library(dplyr)

customer_table <- customer_data %>%
  select(ticket_id,customer_name,customer_age,customer_gender,product_purchased,date_of_purchase)

View(customer_table)

ticket_table <- customer_data %>%
  select(ticket_id,ticket_type,ticket_priority,ticket_status,ticket_channel,initial_response_time,time_resolved,customer_satisfaction_rating)

View(ticket_table)

#CUSTOMER_TABLE
#On the customer table, add a new column that indicates the purchase month and year.
library(lubridate)
customer_table$date_of_purchase <- as.Date(customer_table$date_of_purchase)

# new columns for year and month
customer_table$year <- year(customer_table$date_of_purchase)
customer_table$month <- month(customer_table$date_of_purchase,label = TRUE)
# Combine year and month into a single column
customer_table$year_month <- paste(customer_table$year, month(customer_table$date_of_purchase, 
                                                              label = TRUE), sep="-")
#removing the month column
customer_table <- subset(customer_table, select = -c(month))
View(customer_table)

# Grouping the customer age column.
breaks <- c(-Inf, 20, 30, 40, 50, Inf)
labels <- c("Young", "Young Adult", "Adult", "Old Adults", "Old")
customer_table$age_group <- cut(customer_table$customer_age, breaks = breaks, labels = labels, include.lowest = TRUE)
View(customer_table)

# store in a csv file.
customer_tabledf<-data.frame(customer_table)
write.csv(customer_tabledf, file =" customer_tabledf.csv", row.names = FALSE)

#TICKET_TABLE

#creating a column with resolution time for the closed tickets.

library(dplyr)

tickets_table <- ticket_table %>%
  mutate(response_time = as.numeric(interval(initial_response_time, time_resolved) / dminutes(1)))%>%
  select(ticket_id, ticket_type,ticket_priority, ticket_status, ticket_channel, customer_satisfaction_rating, response_time)
View(tickets_table)

#Relapsing NA values in the customer_satisfaction column with 0
tickets_table$customer_satisfaction_rating[is.na(tickets_table$ customer_satisfaction_rating)] <- 0

# Replasing NA values in the response_time column with 0
tickets_table$response_time[is.na(tickets_table$response_time)] <- 0
View(tickets_table)

write.csv(tickets_table, file =" tickets_table", row.names = FALSE)

# DATA ANALYSIS.

#Q1: Find all tickets that are closed, opened, pending by ticket type and priority.

get_filtered_tickets <- function(tickets_table) {
  filtered_tickets <- tickets_table %>%
    filter(ticket_status %in% c('Closed', 'Open', 'Pending Customer Response')) %>%
    arrange(ticket_status)
  
  return(filtered_tickets)
}
Question1<- data.frame(get_filtered_tickets(tickets_table))
write.csv(Question1, file =" Question1.csv", row.names = FALSE)
View(Question1)

#Q2: Find all priority high tickets that remained opened and pending customer response.

get_filtered_tickets1 <- function(tickets_table) {
  filtered_tickets1 <- tickets_table %>%
    filter((ticket_status == 'Open' | ticket_status == 'Pending Customer Response') &
             ticket_priority == 'High') %>%
    arrange(ticket_status)
  
  return(filtered_tickets1)
}
Question2<- data.frame(get_filtered_tickets1(tickets_table))
write.csv(Question2, file =" Question2.csv", row.names = FALSE)
View(Question2)

#Q3:Find all opened,closed and pending tickets a for all age groups

get_ticket_status_with_age_group <- function(tickets_table, customer_table) {
  joined_tables <- inner_join(tickets_table, customer_table, by = "ticket_id")
  
  filtered_tickets <- joined_tables %>%
    filter(ticket_status %in% c('Closed', 'Open', 'Pending Customer Response')) %>%
    arrange(age_group)
  
  return(filtered_tickets[, c("ticket_status", "age_group")])
}
Question3<- data.frame(get_ticket_status_with_age_group(tickets_table, customer_table))
write.csv(Question3, file =" Question3.csv", row.names = FALSE)
View(Question3)
#Q4: Find all tickets by channel and ticket status.

get_filtered_tickets_by_channel<- function(tickets_table) {
  filtered_tickets <- tickets_table %>%
    filter(ticket_status %in% c('Closed', 'Open', 'Pending Customer Response')) %>%
    arrange(ticket_channel)
  
  return(filtered_tickets)
}
Question4<- data.frame(get_filtered_tickets_by_channel(tickets_table))
write.csv(Question4, file =" Question4.csv", row.names = FALSE)
View(Question4)

#Q5: Find the average customer rating by the different ticket channels.

get_avg_csat_by_channel <- function(tickets_table) {
  avg_csat_by_channel <- tickets_table %>%
    group_by(ticket_channel) %>%
    summarise(avg_csat = mean(customer_satisfaction_rating, na.rm = TRUE)) %>%
    arrange(avg_csat)
  
  return(avg_csat_by_channel)
}
Question5<- data.frame(get_avg_csat_by_channel (tickets_table))
write.csv(Question5, file =" Question5.csv", row.names = FALSE)
View(Question5)

# Q6:Find average response time and customer satisfaction for all tickets by ticket type.

get_avg_csat_response_by_channel_type <- function(tickets_table) {
  avg_csat_response_by_channel_type <- tickets_table %>%
    group_by(ticket_channel, ticket_type) %>%
    summarise(avg_csat = mean(customer_satisfaction_rating, na.rm = TRUE),
              avg_response = mean(response_time, na.rm = TRUE)) %>%
    arrange(avg_csat, avg_response)
  
  return(avg_csat_response_by_channel_type)
}

Question6<- data.frame(get_avg_csat_response_by_channel_type (tickets_table))
write.csv(Question6, file =" Question6.csv", row.names = FALSE)
View(Question6)

# Q7: Find all tickets_ status by month to see to see the trends
#select ticket_type, ticket_sataus, ticket_channel, avg(customer_satisfaction_rating) as avg_csat,avg(response_time) as avg_response, year_month
#FROM tickets_table as tt inner_join customer_tabledf as ct ON tt.ticket_id= ct.ticket_id
#Group by ticket_type, ticket_satus,ticket_channel,year_month
#Order by year_month

get_ticket_metrics_by_type_status_channel_month <- function(tickets_table, customer_tabledf) {
  joined_tables <- inner_join(tickets_table, customer_tabledf, by = "ticket_id")
  
  ticket_metrics <- joined_tables %>%
    group_by(ticket_type, ticket_status, ticket_channel, year_month) %>%
    summarise(avg_csat = mean(customer_satisfaction_rating, na.rm = TRUE),
              avg_response = mean(response_time, na.rm = TRUE)) %>%
    arrange(year_month)
  
  return(ticket_metrics)
}

Question7<- data.frame(get_ticket_metrics_by_type_status_channel_month (tickets_table, customer_tabledf))
write.csv(Question7, file =" Question7.csv", row.names = FALSE)
View(Question7)

