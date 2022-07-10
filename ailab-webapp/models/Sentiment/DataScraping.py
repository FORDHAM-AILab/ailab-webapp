from finvizfinance.news import News
from finvizfinance.quote import finvizfinance


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

