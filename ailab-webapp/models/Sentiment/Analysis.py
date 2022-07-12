from models.Sentiment import DataScraping as ds
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk.tokenize import sent_tokenize
import spacy
import pysentiment2 as ps
from transformers import pipeline
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from transformers import PegasusTokenizer, PegasusForConditionalGeneration
from sense2vec import Sense2VecComponent

import re

# import nltk
# nltk.download('vader_lexicon')


def sentiment_analyzer_nltk(texts):
    analyzer = SentimentIntensityAnalyzer()

    pos_score_sum = 0
    neu_score_sum = 0
    neg_score_sum = 0
    compound_score_sum = 0

    for i in range(len(texts)):
        scores = analyzer.polarity_scores(texts[i])
        pos_score_sum += scores['pos']
        neu_score_sum += scores['neu']
        neg_score_sum += scores['neg']
        compound_score_sum += scores['compound']
        # print(texts[i])
        # print(scores)

    neg = float('{:.3f}'.format(neg_score_sum / len(texts)))
    neu = float('{:.3f}'.format(neu_score_sum / len(texts)))
    pos = float('{:.3f}'.format(pos_score_sum / len(texts)))
    compound = float('{:.4f}'.format(compound_score_sum / len(texts)))

    return {'neg': neg, 'neu': neu, 'pos': pos, 'compound': compound}


# Loughran and McDonald Financial Sentiment
def sentiment_analyzer_lm(texts):
    lm = ps.LM()
    tokens = lm.tokenize(' '.join(texts))
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
        print(texts[i])
        print(result)
        if result[0]['label'] == 'positive':
            pos_count += 1
            pos_score += result[0]['score']
        if result[0]['label'] == 'negative':
            neg_count += 1
            neg_score += result[0]['score']

    score = (pos_score - neg_score) / len(texts)
    return {'positive': pos_count, 'negative': neg_count, 'score': score}


def financial_summation(texts):
    model_name = "human-centered-summarization/financial-summarization-pegasus"
    model = PegasusForConditionalGeneration.from_pretrained(model_name)
    tokenizer = PegasusTokenizer.from_pretrained(model_name)

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

    # return sentiment_analyzer_nltk(news), sentiment_analyzer_lm(news)
    return sentiment_analyzer_transfomers(news), sentiment_analyzer_lm(news)


def txt_analyzer(file):
    with open(file, 'r') as f:
        texts = f.readlines()
        texts = ''.join(texts)

    texts = sent_tokenize(texts)
    # return sentiment_analyzer_nltk(texts), sentiment_analyzer_lm(texts)
    return sentiment_analyzer_transfomers(texts), sentiment_analyzer_lm(texts)


def entity_recognition_word2vec(text):
    nlp = spacy.load('en_core_web_lg')
    doc = nlp(text)
    entities = []
    for entity in doc.ents:
        if entity.label_ == 'ORG':
            print(entity.text, entity.label_)
            entities.append(entity.text)
    entities = list(set(entities))
    print(entities)
    for i in range(len(entities)):
        for j in range(i+1, len(entities)):
            doc1 = nlp(entities[i])
            doc2 = nlp(entities[j])
            simi_score = doc1.similarity(doc2)
            print(doc1, '<->', doc2, simi_score) if simi_score > 0.8 else ''


def entity_recognition_sense2vec(text):
    nlp = spacy.load("en_core_web_lg")
    s2v = nlp.add_pipe("sense2vec")
    s2v.from_disk("/Users/alinluo/Desktop/Samples/s2v_old")

    doc = nlp(text)
    entities = []
    entities_text = []
    for entity in doc.ents:
        if entity.label_ == 'ORG':
            print(entity.text, entity.label_, entity.has_vector, entity._.in_s2v)
            # print(entity._.s2v_most_similar(3))
            if entity.text not in entities_text:
                entities.append(entity)
                entities_text.append(entity.text)
    print(entities)
    # print(entities[0].similarity(entities[2]))
    # print(entities[0]._.s2v_similarity(entities[2]))





if __name__ == '__main__':
    print(news_analyzer())

    # print(txt_analyzer('/Users/alinluo/Desktop/Samples/sample news.txt'))
    # print(',.,.,.,.,.,.,.,.,.,')
    # print(txt_analyzer('/Users/alinluo/Desktop/Samples/sample news 2.txt'))
    # print(',.,.,.,.,.,.,.,.,.,')
    # print(txt_analyzer('/Users/alinluo/Desktop/Samples/sample news 3.txt'))
    # print(',.,.,.,.,.,.,.,.,.,')
    # print(txt_analyzer('/Users/alinluo/Desktop/Samples/sample news 4.txt'))

    # with open('/Users/alinluo/Desktop/Samples/sample news 2.txt', 'r') as f:
    #     texts = f.readlines()
    #     texts = ' '.join(texts)
    # # entity_recognition_sense2vec(texts)
    #
    # texts = [s.strip() for s in texts.splitlines()]
    # # print(texts)
    # print(financial_summation(texts))
