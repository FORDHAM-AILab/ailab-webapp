import torch.nn.functional

from models.Sentiment import DataScraping as ds
import pysentiment2 as ps
from transformers import pipeline
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from transformers import PegasusTokenizer, PegasusForConditionalGeneration

import re
import pandas as pd


# Loughran and McDonald Financial Sentiment
def sentiment_analyzer_lm(input_text):
    lm = ps.LM()
    tokens = lm.tokenize(input_text)
    score = lm.get_score(tokens)
    return score


def sentiment_analyzer_news(texts):
    model_name = 'mrm8488/distilroberta-finetuned-financial-news-sentiment-analysis'
    model = AutoModelForSequenceClassification.from_pretrained(model_name)
    tokenizer = AutoTokenizer.from_pretrained(model_name)

    classifier = pipeline('sentiment-analysis', model=model, tokenizer=tokenizer)

    pos_count, neg_count = 0, 0
    pos_score, neg_score = 0, 0

    for i in range(len(texts)):
        result = classifier(texts[i])

        if result[0]['label'] == 'positive':
            pos_count += 1
            pos_score += result[0]['score']
        if result[0]['label'] == 'negative':
            neg_count += 1
            neg_score += result[0]['score']

    score = (pos_score - neg_score) / len(texts)
    return {'positive_count': pos_count, 'negative_count': neg_count, 'score': round(score, 4)}


# News headlines or short posts
def sentiment_analyzer_finbert(texts):
    model_name = "ProsusAI/finbert"
    model = AutoModelForSequenceClassification.from_pretrained(model_name)
    tokenizer = AutoTokenizer.from_pretrained(model_name)

    pos_score, neg_score, neu_score = 0, 0, 0
    texts_len = len(texts)

    for i in range(texts_len):
        input = tokenizer(texts[i], padding=True, truncation=True, return_tensors='pt')
        output = model(**input)
        scores = torch.nn.functional.softmax(output.logits, dim=-1)

        pos_score += float(scores[0][0])
        neg_score += float(scores[0][1])
        neu_score += float(scores[0][2])

    return {'positive': round(pos_score / texts_len, 4), 'negative': round(neg_score / texts_len, 4),
            'neutral': round(neu_score / texts_len, 4)}


def sentiment_analyzer_finbert_txt(input_text):
    model_name = "ProsusAI/finbert"
    model = AutoModelForSequenceClassification.from_pretrained(model_name)
    tokenizer = AutoTokenizer.from_pretrained(model_name)

    tokens = tokenizer(input_text, add_special_tokens=False, return_tensors='pt')
    input_id_blocks = tokens['input_ids'][0].split(510)
    mask_blocks = tokens['attention_mask'][0].split(510)

    block_size = 512

    input_id_blocks = list(input_id_blocks)
    mask_blocks = list(mask_blocks)

    for i in range(len(input_id_blocks)):
        input_id_blocks[i] = torch.cat([torch.Tensor([101]), input_id_blocks[i], torch.Tensor([102])])
        mask_blocks[i] = torch.cat([torch.Tensor([1]), mask_blocks[i], torch.Tensor([1])])

        pad_len = block_size - input_id_blocks[i].shape[0]

        if pad_len > 0:
            input_id_blocks[i] = torch.cat([input_id_blocks[i], torch.Tensor([0] * pad_len)])
            mask_blocks[i] = torch.cat([mask_blocks[i], torch.Tensor([0] * pad_len)])

    input_ids = torch.stack(input_id_blocks)
    attention_mask = torch.stack(mask_blocks)
    input_dict = {'input_ids': input_ids.long(), 'attention_mask': attention_mask.int()}
    outputs = model(**input_dict)
    scores = torch.nn.functional.softmax(outputs.logits, dim=-1)
    scores = scores.mean(dim=0)

    return {'positive': float(scores[0]), 'negative': float(scores[1]),
            'neutral': float(scores[2])}


def financial_summation(input_text):
    model_name = "human-centered-summarization/financial-summarization-pegasus"
    model = PegasusForConditionalGeneration.from_pretrained(model_name)
    tokenizer = PegasusTokenizer.from_pretrained(model_name)

    texts = [s.strip() for s in input_text.splitlines()]
    summary = []
    for text in texts:
        if re.search('\w', text) is None:
            continue
        input_ids = tokenizer(text, return_tensors='pt').input_ids

        output = model.generate(input_ids)

        summary.append(tokenizer.decode(output[0], skip_special_tokens=True))

    return '\n'.join(summary)


def tweet_preprocess(text):
    new_text = []
    for t in text.split():
        t = '' if t.startswith('@') else t
        t = '' if t.startswith('http') else t
        t = '' if t.startswith('#') else t
        new_text.append(t)
    return ' '.join(new_text)


def sentiment_analyzer_twitter_xlm(texts):
    model_name = 'cardiffnlp/twitter-xlm-roberta-base-sentiment'
    model = AutoModelForSequenceClassification.from_pretrained(model_name)
    tokenizer = AutoTokenizer.from_pretrained(model_name)

    pos_score, neg_score, neu_score = 0, 0, 0
    texts_len = len(texts)

    for i in range(texts_len):
        input = tokenizer(texts[i], return_tensors='pt')
        output = model(**input)
        scores = torch.nn.functional.softmax(output.logits, dim=-1)

        neg_score += float(scores[0][0])
        neu_score += float(scores[0][1])
        pos_score += float(scores[0][2])

    return {'positive': pos_score / texts_len, 'negative': neg_score / texts_len,
            'neutral': neu_score / texts_len}


def news_analyzer(ticker=None):
    if ticker is None:
        news = ds.get_news()
    else:
        news = ds.get_stock_news(ticker)

    return {'fin_news_result': pd.Series(sentiment_analyzer_news(news)),
            'lm_result': pd.Series(sentiment_analyzer_lm(' '.join(news))),
            'finbert_result': pd.Series(sentiment_analyzer_finbert(news))}


def txt_analyzer(input_text):
    return {'finbert_result': sentiment_analyzer_finbert_txt(input_text),
            'lm_result': sentiment_analyzer_lm(input_text)}


def txt_summation(input_text):
    return financial_summation(input_text)


def tweets_analyzer(search_requirement, tweets_num=None):
    if tweets_num is None:
        tweets = ds.get_tweets(search_requirement)
    else:
        tweets = ds.get_tweets(search_requirement, tweets_num)

    for i in range(len(tweets)):
        tweets[i] = tweet_preprocess(tweets[i])

    return sentiment_analyzer_twitter_xlm(tweets), sentiment_analyzer_finbert(tweets)


def reddits_analyzer(search_requirement, reddits_num=None):
    if reddits_num is None:
        reddits = ds.get_reddits(search_requirement)
    else:
        reddits = ds.get_reddits(search_requirement, reddits_num)

    return sentiment_analyzer_finbert(reddits)


if __name__ == '__main__':
    print(news_analyzer())

    # with open('/Users/alinluo/Desktop/Samples/sample news.txt', 'r') as f:
    #     texts = f.readlines()
    #     texts = ''.join(texts)
    # print(txt_analyzer(texts))
    # print(',.,.,.,.,.,.,.,.,.,')
    #
    # print(txt_summation(texts))

    # requirement = 'google stock within_time:1d lang:en'
    # print(tweets_analyzer(requirement, 10))

    # print(reddits_analyzer('google stock', 10))
