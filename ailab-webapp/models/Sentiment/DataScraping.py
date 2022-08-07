from finvizfinance.news import News
from finvizfinance.quote import finvizfinance
import snscrape.modules.twitter as sntwitter


# Getting recent financial news headlines
def get_news():
    news = News().get_news()['news']
    news_headlines = news['Title']
    return news_headlines


# Getting recent news headlines of a given stock
def get_stock_news(stock_name):
    stock = finvizfinance(stock_name)
    news = stock.ticker_news()
    news_headlines = news['Title']
    return news_headlines


# Getting Twitter data
def get_tweets(search_requirement, max_tweets=100):
    tweets_list = []
    for i, tweet in enumerate(sntwitter.TwitterSearchScraper(search_requirement).get_items()):
        if i >= max_tweets:
            break
        tweets_list.append(tweet.content)
    return tweets_list


if __name__ == '__main__':
    requirement = 'apple stock within_time:1d lang:en'
    result = get_tweets(requirement)
    for i in result:
        print(i)
        print('-------------------')