import math
import numpy as np

from peewee import *
import datetime

from playhouse.mysql_ext import JSONField

DB = SqliteDatabase('/my_app.db') # pending

# Create your models here.
class BaseModel(Model):
    class Meta:
        database = DB
    
class User(BaseModel):
    user = CharField(primary_key = True)
    email = CharField(unique=True)
    hashed_password = CharField()
    is_active = BooleanField(default=True)
    
class Homework1(BaseModel):
    user = ForeignKeyField(User, null=True)

    created_at = DateTimeField(default=datetime.datetime.now)
    updated_at = DateTimeField
    
    def save(self, *args, **kwargs):
        self.updated_at = datetime.datetime.now()
        return super(Homework1, self).save(*args, **kwargs)

class Homework2(BaseModel):
    user = ForeignKeyField(User, null=True)

    created_at = DateTimeField(default=datetime.datetime.now)
    updated_at = DateTimeField
    
    def save(self, *args, **kwargs):
        self.updated_at = datetime.datetime.now()
        return super(Homework1, self).save(*args, **kwargs)

class Homework3(BaseModel):
    user = ForeignKeyField(User, null=True)

    created_at = DateTimeField(default=datetime.datetime.now)
    updated_at = DateTimeField
    
    def save(self, *args, **kwargs):
        self.updated_at = datetime.datetime.now()
        return super(Homework1, self).save(*args, **kwargs)

class Game(BaseModel):
    user = ForeignKeyField(User, null=True)

    portfolio_info = JSONField(null=True)

    created_at = DateTimeField(default=datetime.datetime.now)
    updated_at = DateTimeField
    
    def save(self, *args, **kwargs):
        self.updated_at = datetime.datetime.now()
        return super(Homework1, self).save(*args, **kwargs)


class RanGame(BaseModel):
    user = ForeignKeyField(User, null=True)

    calculated_date = DateField(null=True)

    portfolio_created_date = DateTimeField(null=True)
    portfolio_info = JSONField(null=True)

    var_values = JSONField(null=True)
    pnl_values = JSONField(null=True)

    accumulated_pnl = FloatField(null=True)

    created_at = DateTimeField(default=datetime.datetime.now)
    updated_at = DateTimeField
    
    def save(self, *args, **kwargs):
        self.updated_at = datetime.datetime.now()
        return super(Homework1, self).save(*args, **kwargs)

# CRUD 

def calculateGamePnL(tickerList, weights, balance, priceTables):
    pnl = {}
    for idx, ticker in enumerate(tickerList):
        weight = weights[idx]
        last_price = priceTables[ticker].iloc[priceTables.shape[0]-2]
        current_price = priceTables[ticker].iloc[priceTables.shape[0]-1]
        pnl[ticker] = balance*weight/last_price*(current_price-last_price)
    return pnl

def calculateAccumulatedPnl(user):
    ran_games = RanGame.objects.filter(user=user)

    sum_of_pnl = 0
    for ran_game in ran_games:
        pnl_values = ran_game.pnl_values
        for ticker, pnl_value in pnl_values.items():
            sum_of_pnl = sum_of_pnl + pnl_value

    return sum_of_pnl

def normalizePNLValues(result):
    return {k: 0 if math.isnan(v) else v for k, v in result.items() }
    

def normalizeDictValues(computed_result, label):
    temp = computed_result[label]
    temp = {k: None if math.isnan(v) else v for k, v in temp.items() }
    computed_result[label] = temp 
    return computed_result

def normalizeMatrixValues(computed_result, label):
    temp = computed_result[label]
    for x in np.nditer(temp, op_flags = ['readwrite']):
        x[...] = -1 if math.isnan(x) else x
    computed_result[label] = temp 
    return computed_result