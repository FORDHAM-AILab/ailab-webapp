from models.Sentiment import DataScraping as ds
from models.Sentiment.Analysis import tweet_preprocess

from keybert import KeyBERT
from keyphrase_vectorizers import KeyphraseCountVectorizer
from flair.embeddings import TransformerDocumentEmbeddings


def keyword_extraction(input_text):
    embedding_model = TransformerDocumentEmbeddings('ProsusAI/finbert')
    kw_model = KeyBERT(model=embedding_model)
    keywords = kw_model.extract_keywords(input_text, use_maxsum=True, use_mmr=True,
                                         vectorizer=KeyphraseCountVectorizer())
    return keywords


def news_keywords(ticker=None):
    if ticker is None:
        news = ds.get_news()
    else:
        news = ds.get_stock_news(ticker)
    return keyword_extraction(' '.join(news))


def txt_keywords(input_text):
    return keyword_extraction(input_text)


def tweets_keywords(search_requirement, tweets_num=None):
    if tweets_num is None:
        tweets = ds.get_tweets(search_requirement)
    else:
        tweets = ds.get_tweets(search_requirement, tweets_num)

    for i in range(len(tweets)):
        tweets[i] = tweet_preprocess(tweets[i])

    return keyword_extraction(' '.join(tweets))


def reddits_keywords(search_requirement, reddits_num=None):
    if reddits_num is None:
        reddits = ds.get_reddits(search_requirement)
    else:
        reddits = ds.get_reddits(search_requirement, reddits_num)

    return keyword_extraction(' '.join(reddits))


if __name__ == '__main__':
    print(news_keywords())

    # with open('/Users/alinluo/Desktop/Samples/sample news 2.txt', 'r') as f:
    #     texts = f.readlines()
    #     texts = ''.join(texts)
    # print(txt_keywords(texts))

    # print(tweets_keywords('apple stock within_time:1d lang:en'))

    # print(reddits_keywords('apple stock'))
