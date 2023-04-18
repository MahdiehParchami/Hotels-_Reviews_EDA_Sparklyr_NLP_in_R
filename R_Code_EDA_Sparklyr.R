
options(sparklyr.console.log = TRUE)
library(sparklyr)
sc <- spark_connect(master = "local" , version = "2.4.3")

# Load the data

df <- spark_read_csv(sc, name = "df",  path = "D:/data_hotel_reviews_clean.csv")

#what is the best destination with high score review

df_Destinations_high_score <- df %>% 
  
  group_by (country) %>%
  
  summarise( Score = round(max(Average_Score),2)) 

df_Destinations_high_score$country <- factor(df_Destinations_high_score$country,                                   
                                             levels = df_Destinations_high_score$country[order(df_Destinations_high_score$Score, decreasing = TRUE)])
ggplot(df_Destinations_high_score, aes(x = country, y= Score)) +
  geom_bar(stat = "identity" , fill = "#9370DB")+
  geom_text(aes(label = Score), size = 4)+
  labs(subtitle="", 
       y="Average Score", 
       x="Hotle Name", 
       title="Top 5 Best Hotels in Austria", 
       caption = "Source: Kaggle")
# what is the best Hotel in each country for staying?


library(dplyr)

df_BestHotles_per_country <- df %>% 
  
  group_by (country, Hotel_Name) %>%
  
  summarise( averageScore = round(mean(Reviewer_Score),2)) 


# create spark data set
df_BestHotles_per_country_groupby_spark <- copy_to(sc, df_BestHotles_per_country, "df_BestHotles_per_country_groupby_spark", overwrite=TRUE)

# find top 5 best hotels per country

#France
library(DBI)
top_best_France <- dbGetQuery(sc, "select country,Hotel_Name,averageScore from df_BestHotles_per_country_groupby_spark WHERE country = 'France'
             order by averageScore desc")

top_best_France_plot <- head(top_best_France, 5)


library(ggcorrplot)
library(ggplot2)

top_best_France_plot$Hotel_Name <- factor(top_best_France_plot$Hotel_Name,                                   
                                          levels = top_best_France_plot$Hotel_Name[order(top_best_France_plot$averageScore, decreasing = TRUE)])
ggplot(top_best_France_plot, aes(x = Hotel_Name, y= averageScore)) +
  geom_bar(stat = "identity" , fill = "#9ACD32")+
  geom_text(aes(label = averageScore), size = 4)+
  labs(subtitle="", 
       y="Average Score", 
       x="Hotle Name", 
       title="Top 5 Best Hotels in France", 
       caption = "Source: Kaggle")

# *******************************************************************

#UK

top_best_UK <- dbGetQuery(sc, "select country,Hotel_Name,averageScore from df_BestHotles_per_country_groupby_spark WHERE country = 'UK'
             order by averageScore desc")

top_best_UK_plot <- head(top_best_UK, 5)

top_best_UK_plot$Hotel_Name <- factor(top_best_UK_plot$Hotel_Name,                                   
                                      levels = top_best_UK_plot$Hotel_Name[order(top_best_UK_plot$averageScore, decreasing = TRUE)])
ggplot(top_best_UK_plot, aes(x = Hotel_Name, y= averageScore)) +
  geom_bar(stat = "identity" , fill = "#00CED1")+
  geom_text(aes(label = averageScore), size = 4)+
  labs(subtitle="", 
       y="Average Score", 
       x="Hotle Name", 
       title="Top 5 Best Hotels in United Kingdom", 
       caption = "Source: Kaggle")


# *******************************************************************
#	Austria

top_best_Austria <- dbGetQuery(sc, "select country,Hotel_Name,averageScore from df_BestHotles_per_country_groupby_spark WHERE country = 'Austria'
             order by averageScore desc")

top_best_Austria_plot <- head(top_best_Austria, 5)

top_best_Austria_plot$Hotel_Name <- factor(top_best_Austria_plot$Hotel_Name,                                   
                                           levels = top_best_Austria_plot$Hotel_Name[order(top_best_Austria_plot$averageScore, decreasing = TRUE)])
ggplot(top_best_Austria_plot, aes(x = Hotel_Name, y= averageScore)) +
  geom_bar(stat = "identity" , fill = "#9370DB")+
  geom_text(aes(label = averageScore), size = 4)+
  labs(subtitle="", 
       y="Average Score", 
       x="Hotle Name", 
       title="Top 5 Best Hotels in Austria", 
       caption = "Source: Kaggle")

# *******************************************************************
#	Spain

top_best_Spain <- dbGetQuery(sc, "select country,Hotel_Name,averageScore from df_BestHotles_per_country_groupby_spark WHERE country = 'Spain'
             order by averageScore desc")

top_best_Spain_plot <- head(top_best_Spain, 5)

top_best_Spain_plot$Hotel_Name <- factor(top_best_Spain_plot$Hotel_Name,                                   
                                         levels = top_best_Spain_plot$Hotel_Name[order(top_best_Spain_plot$averageScore, decreasing = TRUE)])
ggplot(top_best_Spain_plot, aes(x = Hotel_Name, y= averageScore)) +
  geom_bar(stat = "identity" , fill = "#F4A460")+
  geom_text(aes(label = averageScore), size = 4)+
  labs(subtitle="", 
       y="Average Score", 
       x="Hotle Name", 
       title="Top 5 Best Hotels in Spain", 
       caption = "Source: Kaggle")

# *******************************************************************
#	Netherlands


top_best_Netherlands <- dbGetQuery(sc, "select country,Hotel_Name,averageScore from df_BestHotles_per_country_groupby_spark WHERE country = 'Netherlands'
             order by averageScore desc")

top_best_Netherlands_plot <- head(top_best_Netherlands, 5)

top_best_Netherlands_plot$Hotel_Name <- factor(top_best_Netherlands_plot$Hotel_Name,                                   
                                               levels = top_best_Netherlands_plot$Hotel_Name[order(top_best_Netherlands_plot$averageScore, decreasing = TRUE)])
ggplot(top_best_Netherlands_plot, aes(x = Hotel_Name, y= averageScore)) +
  geom_bar(stat = "identity" , fill = "#6495ED")+
  geom_text(aes(label = averageScore), size = 4)+
  labs(subtitle="", 
       y="Average Score", 
       x="Hotle Name", 
       title="Top 5 Best Hotels in Netherlands", 
       caption = "Source: Kaggle")

# *******************************************************************
#	Italy


top_best_Italy <- dbGetQuery(sc, "select country,Hotel_Name,averageScore from df_BestHotles_per_country_groupby_spark WHERE country = 'Italy'
             order by averageScore desc")

top_best_Italy_plot <- head(top_best_Italy, 5)

top_best_Italy_plot$Hotel_Name <- factor(top_best_Italy_plot$Hotel_Name,                                   
                                         levels = top_best_Italy_plot$Hotel_Name[order(top_best_Italy_plot$averageScore, decreasing = TRUE)])
ggplot(top_best_Italy_plot, aes(x = Hotel_Name, y= averageScore)) +
  geom_bar(stat = "identity" , fill = "#FFD700")+
  geom_text(aes(label = averageScore), size = 4)+
  labs(subtitle="", 
       y="Average Score", 
       x="Hotle Name", 
       title="Top 5 Best Hotels in Italy", 
       caption = "Source: Kaggle")

# *******************************************************************

# Top ten high review score hotels in Europe

top_High_score <- dbGetQuery(sc, "select country,Hotel_Name,averageScore from df_BestHotles_per_country_groupby_spark
             order by averageScore desc")


top_ten_High_score <- head(top_High_score , 10)

top_ten_High_score$Hotel_Name <- factor(top_ten_High_score$Hotel_Name,                                   
                                        levels = top_ten_High_score$Hotel_Name[order(top_ten_High_score$averageScore, decreasing = FALSE)])

# getColor <- function() {
#   a <-  sample(1:30, 1)
#    print(a)
#   return(a)
# }

ggplot(top_ten_High_score, aes(x = Hotel_Name, y= averageScore , fill = country)) +
  geom_bar(stat = "identity") + coord_flip() + 
  geom_text(aes(label = averageScore), size = 4)+
  labs(subtitle="", 
       y="Average Score", 
       x="Hotle Name", 
       title="Top 10 high review score Hotels in Europe", 
       caption = "Source: Kaggle")+
  scale_fill_manual(values=c('#BDB76B', '#8FBC8F', '#9e66ab'))

# viridis::scale_color_viridis(discrete = TRUE)+
#  viridis::scale_fill_viridis(discrete = TRUE)

##599ad3', '#f9a65a', '#9e66ab

# *******************************************************************

# Top ten low review score hotels in Europe

top_low_score <- dbGetQuery(sc, "select country,Hotel_Name,averageScore from df_BestHotles_per_country_groupby_spark
             order by averageScore asc")


top_low_High_score <- head(top_low_score , 10)

top_low_High_score$Hotel_Name <- factor(top_low_High_score$Hotel_Name,                                   
                                        levels = top_low_High_score$Hotel_Name[order(top_low_High_score$averageScore, decreasing = TRUE)])

ggplot(top_low_High_score, aes(x = Hotel_Name, y= averageScore , fill = country)) +
  geom_bar(stat = "identity") + coord_flip() + 
  geom_text(aes(label = averageScore), size = 4)+
  labs(subtitle="", 
       y="Average Score", 
       x="Hotle Name", 
       title="Top 10 Low review score Hotels in Europe", 
       caption = "Source: Kaggle")+
  scale_fill_manual(values=c('#BDB76B', '#8FBC8F', '#9e66ab','#F0E68C'))


# *******************************************************************

#best destination and best hotels for Canadian guests, and what were their reviews

df_best_canada <- df %>% 
  
  group_by (country, Hotel_Name,Reviewer_Nationality) %>%
  
  summarise( averageScore = round(mean(Reviewer_Score),2)) 

# create spark data set
df_best_canada_spark <- copy_to(sc, df_best_canada, "df_best_canada_spark", overwrite=TRUE)


Best_destination_hotels_canadian <- dbGetQuery(sc, "select country,Hotel_Name,averageScore   from df_best_canada_spark
  WHERE  Reviewer_Nationality = 'Canada' 
  order by averageScore desc")

Top_ten_Best_destination_hotels_canadian <- head(Best_destination_hotels_canadian , 10)


ggplot(Top_ten_Best_destination_hotels_canadian, aes(x = Hotel_Name, y= averageScore , fill = country)) +
  geom_bar(stat = "identity") + coord_flip() + 
  geom_text(aes(label = averageScore), size = 4)+
  labs(subtitle="", 
       y="Average Score", 
       x="Hotle Name", 
       title="Top 10 More favorite hotels per country as a canadian", 
       caption = "Source: Kaggle")+
  scale_fill_manual(values=c('#BDB76B', '#5F9EA0', '#9e66ab','#F0E68C','#D8BFD8'))

# *******************************************************************
#business trips analysis

# stayed days

library(DBI)
df_subset_business <- dbGetQuery(sc, 
                                 "select year,month,day,Reviewer_Score,trip_type,regions,Reviewer_Nationality,Hotel_Name,country,Negative_Review,Positive_Review,rooms_type,guests,stayed,extra_room_type,tourist from df
  Where trip_type = 'Business trip'")

# create spark data set
df_subset_business_spark <- copy_to(sc, df_subset_business, "df_subset_business_spark", overwrite=TRUE)

library(scales)

ggplot(df_subset_business_spark, aes(x = stayed)) +
  geom_histogram(color="#008080", fill="#00FF00") +
  
  labs(subtitle=" ", 
       y="count", 
       x="stayed days", 
       title="Business trips's average days of stay ", 
       caption = "Source: Kaggle")

# most reserved rooms

df_subset_business_spark_group <- df_subset_business_spark %>% 
  group_by(rooms_type)  %>% 
  summarise ( count_rooms = n())

#create spark df
df_subset_business_spark_group_spark <- copy_to(sc, df_subset_business_spark_group, "df_subset_business_spark_group_spark", overwrite=TRUE)

df_subset_business_spark_group_order <- dbGetQuery(sc, "select * from df_subset_business_spark_group_spark order by count_rooms desc") 

mostUsed_rooms <- head(df_subset_business_spark_group_order, 10)

mostUsed_rooms$rooms_type <- factor( mostUsed_rooms$rooms_type,                                   
                                     levels =  mostUsed_rooms$rooms_type[order(mostUsed_rooms$count_rooms, decreasing = FALSE)])
ggplot(mostUsed_rooms, aes(x = rooms_type, y= count_rooms , fill = rooms_type )) +
  geom_bar(stat = "identity")+
  coord_flip() + 
  labs(subtitle="", 
       y="Number", 
       x="level", 
       title="Most Used Rooms Types in Business Trips", 
       caption = "Source: Kaggle")


# *******************************************************************
# seasonality 

df_seasonality_review <- df %>% 
  group_by(month)  %>% 
  summarise (Total_Number_of_Reviews = sum(Total_Number_of_Reviews)) 

ggplot(df_seasonality_review, aes(x = month, y= Total_Number_of_Reviews)) +  
  geom_line(color = "#DC143C" , lwd=1 ) +
  labs(subtitle="", 
       y="Total_Number_of_Reviews", 
       x="Month", 
       title="Correlation between seasonality and review numbers during the year", 
       caption = "")+
  theme(plot.title = element_text(hjust = 0.5))

# *******************************************************************
# most guest
library(dplyr)

df_most_guest <- df %>% 
  group_by(guests)  %>% 
  summarise ( count_guests = n()) 

#create spark df
df_most_guest_spark <- copy_to(sc, df_most_guest, "df_most_guest_spark", overwrite=TRUE)

df_most_guest_spark_order <- dbGetQuery(sc, "select * from df_most_guest_spark order by count_guests desc") 


df_most_guest_spark_order$guests  <- factor( df_most_guest_spark_order$guests ,                                   
                                             levels =  df_most_guest_spark_order$guests [order(df_most_guest_spark_order$count_guests, decreasing = FALSE)])
ggplot(df_most_guest_spark_order, aes(x = guests, y= count_guests , fill = guests )) +
  geom_bar(stat = "identity")+
  coord_flip() + 
  labs(subtitle="", 
       y="Count", 
       x="Guests Types", 
       title="Most guests Types in Hotles", 
       caption = "Source: Kaggle")

# *******************************************************************

# Type of trip
library(dplyr)

df_trip_type <- df %>% 
  group_by(trip_type)  %>% 
  summarise ( count_type_trip = n()) 

#create spark df
df_trip_type_spark <- copy_to(sc,df_trip_type , "df_trip_type_spark", overwrite=TRUE)

df_trip_type_spark_order <- dbGetQuery(sc, "select * from df_trip_type_spark where trip_type in ('Leisure trip','Business trip') order by count_type_trip desc") 


df_trip_type_spark_order$trip_type  <- factor( df_trip_type_spark_order$trip_type ,                                   
                                               levels =  df_trip_type_spark_order$trip_type[order(df_trip_type_spark_order$count_type_trip, decreasing = FALSE)])
ggplot(df_trip_type_spark_order, aes(x = trip_type, y= count_type_trip, fill = trip_type )) +
  geom_bar(stat = "identity")+
  
  labs(subtitle="", 
       y="Count", 
       x="Trip Types", 
       title="Trip Types", 
       caption = "Source: Kaggle")

# *******************************************************************


#relationship between reviewer's nationality and scores

df_nationality <- df %>% 
  group_by(Reviewer_Nationality)  %>% 
  summarise ( Total_Number_of_Reviews = sum(Total_Number_of_Reviews)) 

df_nationality_spark <- copy_to(sc, df_nationality, "df_nationality_spark", overwrite=TRUE)

df_nationality_spark_order <- dbGetQuery(sc, "select * from df_nationality_spark order by Total_Number_of_Reviews desc") 


df_nationality_spark_order$Reviewer_Nationality  <- factor( df_nationality_spark_order$Reviewer_Nationality ,                                   
                                                            levels =  df_nationality_spark_order$Reviewer_Nationality [order(df_nationality_spark_order$Total_Number_of_Reviews, decreasing = FALSE)])

top_20_nationality <- head(df_nationality_spark_order , 20)
ggplot(top_20_nationality, aes(x = Reviewer_Nationality, y= Total_Number_of_Reviews )) +
  geom_bar(stat = "identity" , fill = "#BDB76B")+
  coord_flip() + 
  labs(subtitle="", 
       y="Count", 
       x="Guests Types", 
       title="Number of reviews per nationality ", 
       caption = "Source: Kaggle")

# *******************************************************************

# ratio of tourists to non-tourists in the data 

df_tourists <- df %>% 
  group_by(tourist)  %>% 
  summarise ( count_tourist = n()) 

df_tourists_spark <- copy_to(sc, df_tourists, "df_tourists_spark", overwrite=TRUE)

ff <- c(65 , 35)
tt <- c('Tourist' , 'Non-tourist')

pie(ff, labels = paste0(tt, " " ,ff, "%"), col = c("#9e66ab" , "#B0C4DE"),main = "Percentage of tourists compared to non-tourists ")


# *******************************************************************
#NLP analysis   

# Overall Positive Review

library("tm")
library("SnowballC")
library("wordcloud")
library("RColorBrewer")

df_text <- head(df , 140000)

text <- df_text$Positive_Review

# Create a corpus  
docs <- Corpus(VectorSource(text))

#Clean the text data

docs <- docs %>%
  tm_map(removeNumbers) %>%
  tm_map(removePunctuation) %>%
  tm_map(stripWhitespace)
docs <- tm_map(docs, content_transformer(tolower))
docs <- tm_map(docs, removeWords, stopwords("english"))

#Create a document-term-matrix    

dtm <- TermDocumentMatrix(docs) 
matrix <- as.matrix(dtm) 
words <- sort(rowSums(matrix),decreasing=TRUE) 
dff <- data.frame(word = names(words),freq=words)

# Generate the word cloud   

set.seed(1234) # for reproducibility 
wordcloud(words = dff$word, freq = dff$freq, min.freq = 1,
          max.words=200, random.order=FALSE, rot.per=0.35,
          colors=brewer.pal(8, "Dark2"))

# *******************************************************************

# Overall Negative Review

library("tm")
library("SnowballC")
library("wordcloud")
library("RColorBrewer")

library(DBI)  
df_neg <- head(df , 120000)

df_neg_spark <- copy_to(sc, df_neg, "df_neg_spark", overwrite=TRUE)

df_negative_review <- dbGetQuery(sc, "select Negative_Review from df_neg_spark
  where Negative_Review not in ('negative' , 'positive','no negative','nothing'
                                 'Positive_Review', 'Negative_Review','like','se','great','excellent','good','fantastic') ")      




text_negaive <- df_negative_review$Negative_Review

# Create a corpus  
docs <- Corpus(VectorSource(text_negaive))

# Clean the text data

docs <- docs %>%
  tm_map(removeNumbers) %>%
  tm_map(removePunctuation) %>%
  tm_map(stripWhitespace)
docs <- tm_map(docs, content_transformer(tolower))
docs <- tm_map(docs, removeWords, stopwords("english"))

#Create a document-term-matrix    

dtm_n <- TermDocumentMatrix(docs) 
matrix <- as.matrix(dtm_n) 
words <- sort(rowSums(matrix),decreasing=TRUE) 
dff_Neg <- data.frame(word = names(words),freq=words)

#Generate the word cloud   

set.seed(1234) # for reproducibility 
wordcloud(words = dff_Neg$word, freq = dff_Neg$freq, min.freq = 1,
          max.words=200, random.order=FALSE, rot.per=0.35,
          colors=brewer.pal(8, "Dark2"))

# *******************************************************************

# business Trip Reviews

#  Positive Review

library("tm")
library("SnowballC")
library("wordcloud")
library("RColorBrewer")

df_text <- head(df_subset_business , 140000)

text <- df_subset_business$Positive_Review

# Create a corpus  
docs <- Corpus(VectorSource(text))

#Clean the text data

docs <- docs %>%
  tm_map(removeNumbers) %>%
  tm_map(removePunctuation) %>%
  tm_map(stripWhitespace)
docs <- tm_map(docs, content_transformer(tolower))
docs <- tm_map(docs, removeWords, stopwords("english"))

#Create a document-term-matrix    

dtm <- TermDocumentMatrix(docs) 
matrix <- as.matrix(dtm) 
words <- sort(rowSums(matrix),decreasing=TRUE) 
dff <- data.frame(word = names(words),freq=words)

# Generate the word cloud   

set.seed(1234) # for reproducibility 
wordcloud(words = dff$word, freq = dff$freq, min.freq = 1,
          max.words=200, random.order=FALSE, rot.per=0.35,
          colors=brewer.pal(8, "Dark2"))


# Negative Review

library("tm")
library("SnowballC")
library("wordcloud")
library("RColorBrewer")

df_text <- head(df_subset_business , 140000)

text <- df_subset_business$Negative_Review

# Create a corpus  
docs <- Corpus(VectorSource(text))

#Clean the text data

docs <- docs %>%
  tm_map(removeNumbers) %>%
  tm_map(removePunctuation) %>%
  tm_map(stripWhitespace)
docs <- tm_map(docs, content_transformer(tolower))
docs <- tm_map(docs, removeWords, stopwords("english"))

#Create a document-term-matrix    

dtm <- TermDocumentMatrix(docs) 
matrix <- as.matrix(dtm) 
words <- sort(rowSums(matrix),decreasing=TRUE) 
dff <- data.frame(word = names(words),freq=words)

# Generate the word cloud   

dev.off()
set.seed(1234) # for reproducibility 
wordcloud(words = dff$word, freq = dff$freq, min.freq = 1,
          max.words=200, random.order=FALSE, rot.per=0.35,
          colors=brewer.pal(8, "Dark2"))   


# *******************************************************************
# Couples Reviews


df_Couples <- dbGetQuery(sc, "select * from df  where guests = 'Couple'") 
#  Positive Review

library("tm")
library("SnowballC")
library("wordcloud")
library("RColorBrewer")

df_text <- head(df_Couples , 140000)

text <- df_text$Positive_Review

# Create a corpus  
docs <- Corpus(VectorSource(text))

#Clean the text data

docs <- docs %>%
  tm_map(removeNumbers) %>%
  tm_map(removePunctuation) %>%
  tm_map(stripWhitespace)
docs <- tm_map(docs, content_transformer(tolower))
docs <- tm_map(docs, removeWords, stopwords("english"))

#Create a document-term-matrix    

dtm <- TermDocumentMatrix(docs) 
matrix <- as.matrix(dtm) 
words <- sort(rowSums(matrix),decreasing=TRUE) 
dff <- data.frame(word = names(words),freq=words)

# Generate the word cloud   

dev.off()
set.seed(1234) # for reproducibility 
wordcloud(words = dff$word, freq = dff$freq, min.freq = 1,
          max.words=200, random.order=FALSE, rot.per=0.35,
          colors=brewer.pal(8, "Dark2"))


# Negative Review

library("tm")
library("SnowballC")
library("wordcloud")
library("RColorBrewer")

df_text <- head(df_Couples , 120000)

text <- df_text$Negative_Review

# Create a corpus  
docs <- Corpus(VectorSource(text))

#Clean the text data

docs <- docs %>%
  tm_map(removeNumbers) %>%
  tm_map(removePunctuation) %>%
  tm_map(stripWhitespace)
docs <- tm_map(docs, content_transformer(tolower))
docs <- tm_map(docs, removeWords, stopwords("english"))

#Create a document-term-matrix    

dtm <- TermDocumentMatrix(docs) 
matrix <- as.matrix(dtm) 
words <- sort(rowSums(matrix),decreasing=TRUE) 
dff <- data.frame(word = names(words),freq=words)

# Generate the word cloud   

dev.off()
set.seed(1234) # for reproducibility 
wordcloud(words = dff$word, freq = dff$freq, min.freq = 1,
          max.words=200, random.order=FALSE, rot.per=0.35,
          colors=brewer.pal(8, "Dark2"))   
