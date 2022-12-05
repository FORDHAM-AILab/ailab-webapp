import pandas as pd
import snscrape.modules.twitter as sntwitter
import snscrape.modules.reddit as snreddit
from finvizfinance.util import web_scrap


# Getting recent financial news headlines
def get_news():
    NEWS_URL = "https://finviz.com/news.ashx"

    news_content = web_scrap(NEWS_URL).find(id="news").find("table")
    news_collection = news_content.findAll("tr", recursive=False)[1]

    rows = news_collection.findAll("table")[0].findAll("tr")

    titles = []
    for row in rows:
        cols = row.findAll("td")
        if len(cols) < 3:
            continue
        title = cols[2].text
        titles.append(title)

    return pd.Series(titles)


# Getting recent news headlines of a given stock
def get_stock_news(ticker):
    QUOTE_URL = "https://finviz.com/quote.ashx?t={}".format(ticker)

    fullview_news_outer = web_scrap(QUOTE_URL).find("table", class_="fullview-news-outer")
    rows = fullview_news_outer.findAll("tr")

    titles = []
    for row in rows:
        cols = row.findAll("td")
        if len(cols) < 2:
            continue
        title = cols[1].a.text
        titles.append(title)

    return pd.Series(titles)


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
    print(get_news())

    # requirement = 'apple stock within_time:1d lang:en'
    # result = get_tweets(requirement)
    # for i in result:
    #     print(i)
    #     print('-------------------')
    