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
        if cols[1].a is None:
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


def convert_twitter_search(content=None, hashtag=None, cashtag=None, url=None, from_user=None, to_user=None,
                           at_user=None, city=None, since=None, until=None, within_time=None):
    """
    :param content: search words
    :param hashtag: #words
    :param cashtag: $stock
    :param url: contained url
    :param from_user: sent by
    :param to_user: reply to
    :param at_user: @user
    :param city: geotagged place (if me, then near:me)
    :param since: on or after a specified date(yyyy-mm-dd)
    :param until: before a specified date(yyyy-mm-dd)
    :param within_time: within the last number of days, hours, minutes, or seconds
    :return: search_requirement
    """

    search_requirement = ''

    if content is not None:
        search_requirement = f'{content} '
    if hashtag is not None:
        search_requirement += f'{hashtag} '
    if cashtag is not None:
        search_requirement += f'{cashtag} '
    if at_user is not None:
        search_requirement += f'{at_user} '
    if url is not None:
        search_requirement += f'url:{url} '
    if from_user is not None:
        search_requirement += f'from:{from_user} '
    if to_user is not None:
        search_requirement += f'to:{to_user} '
    if city is not None:
        search_requirement += f'near:{city} '
    if since is not None:
        search_requirement += f'since:{since} '
    if until is not None:
        search_requirement += f'until:{until} '
    if within_time is not None:
        search_requirement += f'within_time:{within_time} '
    search_requirement += 'lang:en'
    return search_requirement


if __name__ == '__main__':
    # print(get_news())
    # print(get_stock_news('aapl'))

    # requirement = 'apple stock within_time:1d lang:en '
    # result = get_tweets(requirement)
    # for i in result:
    #     print(i)
    #     print('-------------------')

    di = {'within_time': '1d', 'at_user': '@Youtube @Google'}
    requirement = convert_twitter_search(**di)
    result = get_tweets(requirement)
    for i in result:
        print(i)
        print('-------------------')
    