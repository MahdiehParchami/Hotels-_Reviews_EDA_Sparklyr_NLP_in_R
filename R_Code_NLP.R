
library("tm")
library("SnowballC")
library("wordcloud")
library("RColorBrewer")


setwd("D:/Project/")

library(readr)
df <- read.csv("data_hotel_reviews_clean.csv")
library(dplyr)

#what is the best destination with high score review
df_Destinations_high_score <- df %>% 
  
  group_by (country) %>%
  
  summarise( Score = round(max(Average_Score),2)) 


df_Destinations_high_score$country <- factor(df_Destinations_high_score$country,                                   
                                             levels = df_Destinations_high_score$country[order(df_Destinations_high_score$Score, decreasing = TRUE)])
ggplot(df_Destinations_high_score, aes(x = country, y= Score , fill = country)) +
  geom_bar(stat = "identity")+
  geom_text(aes(label = Score), size = 4)+
  labs(subtitle="", 
       y="Average Score", 
       x=" Country", 
       title="Best destinations with high score review", 
       caption = "Source: Kaggle")


df1 <- head(df , 150000)


# Positive review
#Create a vector containing only the text

text <- df1$Positive_Review

# Create a corpus  
docs <- Corpus(VectorSource(text))

docs <- docs %>%
  tm_map(removeNumbers) %>%
  tm_map(removePunctuation) %>%
  tm_map(stripWhitespace)

docs <- tm_map(docs, content_transformer(tolower))
docs <- tm_map(docs, removeWords, stopwords("english"))
dtm <- TermDocumentMatrix(docs) 
matrix <- as.matrix(dtm) 
words <- sort(rowSums(matrix),decreasing=TRUE) 
df2 <- data.frame(word = names(words),freq=words)


set.seed(1234) # for reproducibility 
wordcloud(words = df2$word, freq = df2$freq, min.freq = 1,
          max.words=200, random.order=FALSE, rot.per=0.35, 
          colors=brewer.pal(8, "Dark2"))
