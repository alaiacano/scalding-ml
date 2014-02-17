import numpy as np
import pandas as pd
from sklearn import neighbors

n_neighbors = 15

iris = pd.read_csv("../iris.tsv", sep="\t", names=["id", "clazz", "sl", "sw", "pl", "pw"])

# training set
X_train = np.array(iris[["sl", "sw", "pl", "pw"]][iris.id % 3 != 0])
y_train = np.array(iris.clazz[iris.id % 3 != 0])

# test set
X_test = np.array(iris[["sl", "sw", "pl", "pw"]][iris.id % 3 == 0])
y_test = np.array(iris.clazz[iris.id % 3 == 0])
id_test = np.array(iris.id[iris.id % 3 == 0])

clf = neighbors.KNeighborsClassifier(n_neighbors, weights='uniform')
clf.fit(X_train, y_train)

pred = clf.predict(X_test)

# print the test set ID and predicted value.

for result in zip(id_test, pred):
    print result[0], result[1]

print "\naccuracy: %d of %d" % (sum(pred==y_test), len(y_test))
