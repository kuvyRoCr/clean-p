import dask
from dask.distributed import Client, progress
import dask.dataframe as dd

def getDaskClient():
    client = Client(n_workers=2, threads_per_worker=3, memory_limit='4GB')
    return client
 
def getCars():
    return dd.read_csv('./input/cars_train.csv')

def prepCity(city):
    return city

def cleanNameStateName(stateName):
    if stateName == 'District of Columbia':
        return 'Washington'
    elif stateName == 'FAILED':
        return 'Canada'
    return stateName

def dropLocationColumns(daskCars):
    return daskCars.drop(columns=['city','county_fips','county_name','state_fips','state_code','long','lat'])

def fillnaYear(cars):
    cars.year=cars.year.fillna(0.0)
    return cars

def cleanYear(floatYear):
    return str(floatYear).split('.')[0]
    

