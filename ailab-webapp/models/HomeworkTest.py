from Homework import Homework

if __name__ =='__main__':
	tickerList = ['AMZN', 'MSFT', 'GOOG']
	weight = [0.5, 0.3, 0.2]
	startDate = '2017-8-10'
	endDate = '2018-9-11'
	notional = 100000
	# Look back window for historical VaR
	historicalWindow = 100

	demo = Homework()
	result = demo.Homework1(tickerList, weight, notional, historicalWindow=historicalWindow)
	print('Parametric', result['Parametric'])
	print('\nHistorical', result['Historical'])
	print('\nCov-Var Matrix')
	print(result['Cov-Var Matrix'])

	ticker = 'AMZN'
	rf = 0.05
	ttm = [1, 2]
	Debt = [6000, 20]
	demo = Homework()

	result = demo.Homework2(ticker, ttm, Debt)
	print('\nAsset Value:', result['Asset Price'])
	print('Default Prob:', result['Default Prob'])
	print('Market Cap:', result['Market Cap'])

	k1 = 900
	t1 = 1
	k2 = 1100
	t2 = 2

	ticker = 'AMZN'
	rf = 0.0
	vol = 0.210609
	div = 0
	pcFlag = 1

	sharpRatio = 0.23
	demo = Homework()
	result = demo.Homework3(ticker, t1, t2, k1, k2, rf, div, sharpRatio, steps=200)

	print('\nLiquid Asset Value:', result['Liquid Asset'])
	print('Illiquid Asset Value:', result['Illiquid Asset'])
	print('Illiquid Equity Value:', result['Illiquid Equity'])
	print('Market Cap:', result['Market Cap'])