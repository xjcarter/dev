from indicators import Volatility
import pandas

q = []
vol = Volatility()
df = pandas.read_csv('SPY.csv')
for index, row in df.iterrows():
	v = vol.push(row['Adj Close'])
	if v is not None:
		q.append(v)

import pdb; pdb.set_trace()
series = pandas.Series(q)
print(series.info())
