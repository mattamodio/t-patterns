import sys
import pandas as pd
import statsmodels.api as sm
import sklearn
import sklearn.metrics
import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.naive_bayes import GaussianNB,BernoulliNB,MultinomialNB
from sklearn.ensemble import RandomForestClassifier, AdaBoostClassifier, ExtraTreesClassifier
from sklearn.neural_network import BernoulliRBM
from sklearn.svm import SVC
import matplotlib.pyplot as plt

fn1 = 'output/happiness_by_op.txt'

fn2 = 'outputdata_with_happiness5.csv'

with open('happiness_matrix.csv') as f:
	df = pd.read_csv(f)


# happiness = {}
# with open(fn1) as f:
# 	for line in f:
# 		op,h = line.strip().split('\t')
# 		happiness[int(op)] = [h]

# counter=1
# with open(fn2) as g:
# 	for line in g:
# 		if counter in happiness:
# 			line = line.strip().split(',')
# 			happiness[counter] = happiness[counter] + line
# 			happiness[counter] = map(lambda x: int(x), happiness[counter])
# 			#print happiness[counter]
# 			#sys.exit()
# 		counter+=1


# df = pd.DataFrame.from_dict(happiness, orient='index')

lines = []
for line in open('output/informative_patterns.txt'):
	lines.append(line.strip())



imp = [80, 58, 20, 139, 188, 55, 42, 141, 140, 56, 105, 104, 47, 45, 172, 7]

x_cols = df.columns[imp]
#x_cols = df.columns[2:]
x = df[x_cols]
y = df['satisfaction']>=3
# print x
# print y
w1 = 575./ 543
w2 = 575./ 32

df_with_imp_pattern = df[ df[x_cols].applymap(lambda z: z==1).any(axis=1) ]

#print df_with_imp_pattern[ df.columns[[1] + imp]  ]
sats = []
for i,col in enumerate(df.columns):
	#if i>100: break
	# print col
	sat = sorted(df[df[col]==1]['satisfaction'].tolist())
	sats.append(sat)
sats = sats[2:]

pairs=[]
for z1,z2 in zip(lines,sats):
	pairs.append((z1,z2))

pairs = sorted(pairs, key=lambda x: len(filter(lambda z:z<=3, x[1])), reverse=True )
for pair in pairs:
	print "{0}\t{1}".format(pair[1],pair[0])	
sys.exit()
# weights = df.apply(lambda x: w1 if x['satisfaction']>=4 else w2, axis=1)


#model = LogisticRegression(solver='newton-cg').fit(x, y)
model = RandomForestClassifier(n_estimators=100, class_weight='balanced').fit(x,y)
#model = ExtraTreesClassifier().fit(x,y)
#model = AdaBoostClassifier().fit(x,y)
#model = SVC(probability=True).fit(x,y)
#model = LogisticRegression(class_weight='balanced').fit(x, y)
#model = MultinomialNB().fit(x, y, sample_weight=weights)

scores = model.predict_proba(x)[:,1]



#model = BernoulliRBM(n_components=1024, n_iter=1000).fit(x,y)
#scores = model.score_samples(x)


fpr, tpr, thresholds = sklearn.metrics.roc_curve(y, scores, pos_label=True)
area = sklearn.metrics.auc(fpr, tpr)

print model.feature_importances_
features = []
for index,importance in enumerate(np.nditer(model.feature_importances_)):
	features.append( ( index,importance ) )
print sorted(features, key=lambda x: x[1], reverse=True)

# print fpr
# print tpr
# print thresholds

plt.figure()
plt.plot(fpr, tpr, label='ROC curve (area = %0.2f)' % area)
plt.plot([0, 1], [0, 1], 'k--')
plt.xlim([0.0, 1.0])
plt.ylim([0.0, 1.05])
plt.xlabel('False Positive Rate')
plt.ylabel('True Positive Rate')
plt.title('Receiver operating characteristic example')
plt.legend(loc="lower right")
plt.show()


x_sat = df[y==1]
y_sat = y[y==1]
x_unsat = df[y==0]
y_unsat = y[y==0]

# print model.class_count_
# print model.coef_

print "Satisfied:"
print len(y_sat)
print model.score(x_sat[x_cols], y_sat)

print "\nUnsatisfied:"
print len(y_unsat)
print model.score(x_unsat[x_cols], y_unsat)

print "\nTotal:"
print len(y)
print model.score(df[x_cols], y)
