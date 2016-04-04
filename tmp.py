import sys
import pandas as pd
import statsmodels.api as sm
import sklearn
from sklearn.linear_model import LogisticRegression
from sklearn.naive_bayes import GaussianNB,BernoulliNB,MultinomialNB

fn1 = 'output/happiness_by_op.txt'

fn2 = 'outputdata_with_happiness5.csv'

with open('happiness_matrix.csv') as f:
	df = pd.read_csv(f)

print df
sys.exit()

happiness = {}
with open(fn1) as f:
	for line in f:
		op,h = line.strip().split('\t')
		happiness[int(op)] = [h]

counter=1
with open(fn2) as g:
	for line in g:
		if counter in happiness:
			line = line.strip().split(',')
			happiness[counter] = happiness[counter] + line
			happiness[counter] = map(lambda x: int(x), happiness[counter])
			#print happiness[counter]
			#sys.exit()
		counter+=1


df = pd.DataFrame.from_dict(happiness, orient='index')

# print df.corr()


x_cols = df.columns[1:]
x = df[x_cols]
y = df.apply(lambda x: int(x[0]>=4), axis=1)


w1 = 575./ 543
w2 = 17#575./ 32


weights = df.apply(lambda x: w1 if x[0]>=4 else w2, axis=1)


#model = LogisticRegression(solver='newton-cg').fit(df[x_cols], y, sample_weight=weights)
#model = LogisticRegression(class_weight='balanced').fit(df[x_cols], y)
model = MultinomialNB().fit(x, y, sample_weight=weights)

x_sat = df[y==1]
y_sat = y[y==1]
x_unsat = df[y==0]
y_unsat = y[y==0]

print model.class_count_
print model.coef_
# sys.exit()
print "Satisfied:"
print len(y_sat)
print model.score(x_sat[x_cols], y_sat)

print "\nUnsatisfied:"
print len(y_unsat)
print model.score(x_unsat[x_cols], y_unsat)

print "\nTotal:"
print len(y)
print model.score(df[x_cols], y)
