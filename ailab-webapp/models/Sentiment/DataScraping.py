from finvizfinance.news import News
from finvizfinance.quote import finvizfinance
import snscrape.modules.twitter as sntwitter
import snscrape.modules.reddit as snreddit


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


# Getting Reddit data
def get_reddits(search_requirement, max_reddits=100):
    reddits_list = []
    for i, reddit in enumerate(snreddit.RedditSearchScraper(search_requirement).get_items()):
        if i >= max_reddits:
            break
        if type(reddit) is snreddit.Submission:
            reddits_list.append(reddit.title)
        if type(reddit) is snreddit.Comment:
            reddits_list.append(reddit.body)
    return reddits_list


if __name__ == '__main__':
    requirement = 'apple stock within_time:1d lang:en'
    result = get_tweets(requirement)
    for i in result:
        print(i)
        print('-------------------')