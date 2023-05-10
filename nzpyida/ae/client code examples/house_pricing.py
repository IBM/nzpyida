from nzpyida.ae.install import NZInstall
nzpy_dsn = {
        "database":"housing",
         "port" :5480,
        "host": "9.30.250.118",
        "securityLevel":3,
        "logLevel":0
       }
import pandas as pd
from nzpyida import IdaDataBase, IdaDataFrame
from nzpyida.ae import NZFunTApply
idadb = IdaDataBase(nzpy_dsn, uid="admin", pwd="password", verbose=True)

#idadb.ida_query("create view houseprice_temp as select * from houseprice")

#idadb.ida_query("alter table houseprice add column date_str varchar(1000)")
#idadb.ida_query("update  houseprice set date_str = cast(ID as varchar(1000))")
#idadf = IdaDataFrame(idadb,"HOUSEPRICE")
idadf = IdaDataFrame(idadb,"HOUSEPRICE_TEMP")

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
print(idadf.head())
print(idadf.dtypes)
#package_name = 'sklearn'
#nzinstall = NZInstall(idadb, package_name)
#result = nzinstall.getResultCode()
#print(result)
code_house_prediction_model = """def house_prediction_model(self,df):
            import numpy as np
            import pandas as pd
            import nzaeCppWrapper

            import sklearn

            from sklearn.model_selection import cross_val_score
            from sklearn.model_selection import train_test_split
            from sklearn.linear_model import LinearRegression
            # from sklearn.ensemble import GradientBoostingRegression
            # from xgboost import XGBRegressor
            from sklearn.metrics import r2_score,mean_squared_error
            from datetime import datetime

            # Getting the current date and time
            #dt = datetime.now()

            # getting the timestamp
            #ts = datetime.timestamp(dt)



            data = df.copy()

            #id1 = data['ID'][0]
            #id = id1/100000
            #data['test_field'] = str(ts)
            
           

            


            
            

            


            

            


           

            self.output(id,0.85, 'hello')











"""

output_signature = {'ID': 'float', 'accuracy': 'double', 'date':'str'}

nz_fun_tapply = NZFunTApply(df=idadf, code_str=code_house_prediction_model, fun_name="house_prediction_model",
                            parallel=False,  output_signature=output_signature)

print(nz_fun_tapply.get_result())