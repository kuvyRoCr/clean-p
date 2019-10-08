from sklearn.decomposition import PCA
import pandas as pd

def getPCAoneCom(pdcolumn):
    columns = pd.get_dummies(pdcolumn)
    pca = PCA(n_components=1)
    pca.fit(columns)
    return pca.transform(columns).reshape(1, -1)[0]

def convertToNumber():
    cars=pd.read_csv('../output/partialCleanOnlyM.csv').drop(columns=['Unnamed: 0','make'])
    
    for col in list(cars.columns)[:-1]:
        cars[col]=getPCAoneCom(cars[col]).astype('float32')
        cars[col]=cars[col].apply(lambda p:p/10)
        print(col,'converted and normalized')
    cars.price = cars.price.apply(lambda p:p/10**10)#normalicing prize
    cars.to_csv('../output/dfForTrain1.csv')

    