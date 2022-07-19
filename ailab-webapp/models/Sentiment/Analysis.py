import torch.nn.functional

from models.Sentiment import DataScraping as ds
import pysentiment2 as ps
from transformers import pipeline
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from transformers import PegasusTokenizer, PegasusForConditionalGeneration

import re


# Loughran and McDonald Financial Sentiment
def sentiment_analyzer_lm(input_text):
    lm = ps.LM()
    tokens = lm.tokenize(input_text)
    score = lm.get_score(tokens)
    return score


def sentiment_analyzer_transfomers(texts):
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
    return {'positive': pos_count, 'negative': neg_count, 'score': score}


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

    return {'positive': pos_score / texts_len, 'negative': neg_score / texts_len,
            'neutral': neu_score / texts_len}


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


def news_analyzer(ticker=None):
    if ticker is None:
        news = ds.get_news()
    else:
        news = ds.get_stock_news(ticker)

    return sentiment_analyzer_transfomers(news), sentiment_analyzer_lm(' '.join(news)), \
           sentiment_analyzer_finbert(news)


def txt_analyzer(file):
    with open(file, 'r') as f:
        texts = f.readlines()
        texts = ''.join(texts)

    return sentiment_analyzer_finbert_txt(texts), sentiment_analyzer_lm(texts)


def txt_summation(file):
    with open(file, 'r') as f:
        texts = f.readlines()
        texts = ' '.join(texts)

    return financial_summation(texts)


if __name__ == '__main__':
    print(news_analyzer())

    # print(txt_analyzer('/Users/alinluo/Desktop/Samples/sample news.txt'))
    # print(',.,.,.,.,.,.,.,.,.,')
    #
    # print(txt_summation('/Users/alinluo/Desktop/Samples/sample news 2.txt'))
