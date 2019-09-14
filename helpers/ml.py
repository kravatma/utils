from sklearn.cluster import KMeans
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.preprocessing import LabelEncoder, OneHotEncoder, QuantileTransformer
from sklearn.metrics import mean_squared_error
from pandas import DataFrame, concat


class clasterisator():
    def __init__(self, K, model=KMeans):
        self.kmeans = model(n_clusters=K, random_state=42)
    def fit(self, df):
        self.kmeans.fit(df)
        return self.kmeans
    def predict(self, df):
        return self.kmeans.predict(df)
    def fit_predict(self, df):
        return self.fit(df).predict(df)
        
    
class MultiColumnLabelEncoder:
    def __init__(self, columns = None):
        self.columns = columns

    def fit(self, X):
        encoders_dict = dict()
        if self.columns:
            for col in self.columns:
                encoders_dict[col] = LabelEncoder().fit(X[col])
        else:
            for colname, values in X.iteritems():
                encoders_dict[colname] = LabelEncoder().fit(values)
        self.encoders_dict = encoders_dict
        return self
        
    def transform(self, X):
        output = X.copy()
        if self.columns:
            for col in self.columns:
                output[col] = self.encoders_dict[col].transform(output[col])
        else:
            for colname, values in output.iteritems():
                output[colname] = self.encoders_dict[colname].transform(values)
        return output
        
    def fit_transform(self, X):
        return self.fit(X).transform(X)
        
    def inverse_transform(self, X):
        output = X.copy()
        if self.columns:
            for col in self.columns:
                output[col] = self.encoders_dict[col].inverse_transform(output[col])
        else:
            for colname, values in output.iteritems():
                output[colname] = self.encoders_dict[colname].inverse_transform(values)
        return output


class SimpleDataTransformer():
    def __init__(self, numerical_cols=None, categorical_cols=None, numerical_transformer=None):
        self.transformers_dict = dict()        
        if numerical_cols:
            self.numerical_cols = numerical_cols
        if categorical_cols:
            self.categorical_cols = categorical_cols
            self.mcle = MultiColumnLabelEncoder(categorical_cols)
        if numerical_transformer is None:
            self.numerical_transformer = QuantileTransformer(1000, 'normal')
        elif isinstance(numerical_transformer, type):
            self.numerical_transformer = numerical_transformer()
        else:
            self.numerical_transformer = numerical_transformer
        
    def fit(self, X):
        if self.numerical_cols:
            for col in self.numerical_cols:
                self.transformers_dict[col] = self.numerical_transformer.fit(X[[col]])
        
        if self.categorical_cols:
            mcle_out = self.mcle.fit_transform(X[self.categorical_cols])
            self.ohe = OneHotEncoder(sparse=False, categories='auto').fit(mcle_out)
            #for col in self.categorical_cols:
            #    self.transformers_dict[col] = OneHotEncoder.fit(mcle_out[[col]])
        return self
        
    def transform(self, X, drop_cat=True):
        output = X.copy()
        if self.numerical_cols:
            for col in self.numerical_cols:
                output[col] = self.transformers_dict[col].transform(output[[col]])
        if self.categorical_cols:
            mcle_out = self.mcle.transform(X[self.categorical_cols])
            ohe_out = self.ohe.transform(mcle_out)
            if drop_cat:
                output.drop(columns=self.categorical_cols, inplace=True)
            catcol_names = ['ohe%d'%i for i in range(ohe_out.shape[1])]
            ohe_out_df = DataFrame(ohe_out, columns=catcol_names)
            output = concat((output, ohe_out_df), axis=1)
        return output

    def fit_transform(self, X):
        return self.fit(X).transform(X)

    def inverse_transform(self, X):
        pass
        """
        output = X.copy()
        if self.columns:
            for col in self.columns:
                output[col] = self.encoders_dict[col].inverse_transform(output[col])
        else:
            for colname, values in output.iteritems():
                output[colname] = self.encoders_dict[colname].inverse_transform(values)
        return output        
        """
        

def cv_search(base_model, X, y, param_grid, testsize=0.5, cv=5):
    (X_train, X_test, y_train, y_test) = train_test_split(X, y, test_size=testsize, random_state=42)
    gs_cv = GridSearchCV(base_model, param_grid=param_grid, cv=cv, n_jobs=-1, verbose=True)
    gs_cv.fit(X_train, y_train)
    #print(gs_cv.score(X_test, y_test))
    print('mse validation:', mean_squared_error(y_test, gs_cv.predict(X_test))**0.5)
    return gs_cv.best_estimator_
    
