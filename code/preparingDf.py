import dask
from dask.distributed import Client, LocalCluster
import dask.dataframe as dd
import pandas as pd

def getDaskClient():
    cluster = LocalCluster() 
    client = Client(cluster)
    return client
 
def getCars():
    return dd.read_csv('../input/cars_train.csv')

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

def cleanManufacturer1(manufacturer):
    '''Esta función limpia marcas duplicadas'''
    manufacturer = str(manufacturer)
    if manufacturer=='acura':
        return 'honda'
    if manufacturer.find('harley')!=-1:
        return 'harley'
    if manufacturer=='chevy' or manufacturer=='chev':
        return 'chevrolet'
    if manufacturer.find('mercedes')!=-1:
        return 'mercedes'
    if manufacturer.find('aston')!=-1:
        return 'aston-martin'
    if manufacturer.find('alfa')!=-1:
        return 'alfa-romeo'
    if manufacturer=='vw':
        return 'volkswagen'
    if manufacturer=='land rover':
        return 'landrover'
    return manufacturer

def cleanManufacturer2(row,listaMarcas):
    '''Esta función intenta rescatar alguna marca perdidad de la columna de make,
    las que no rescata las inicializa unknown'''
    if row.manufacturer== 'nan':
        for marca in listaMarcas:
            if type(row.make)==str and row.make.lower().find(marca)!=-1:
                row.manufacturer=marca
                return row
        row.manufacturer='unknown'
        return row
    return row

def cleanCondition(condition):
    if condition in ['like new','fair']:
        return 'excellent'
    elif condition in ['salvage']:
        return 'unknown'
    return condition

def cleanAllDf():
    #client=getDaskClient()
    cars=getCars()
    cars.state_name=cars.state_name.apply(cleanNameStateName)
    cars= dropLocationColumns(cars)

    cars = fillnaYear(cars)
    cars.year=cars.year.apply(cleanYear,meta=('year', 'object'))
    
    cars.manufacturer=cars.manufacturer.apply(cleanManufacturer1)
    
    listaMarcas=sorted(list(cars.manufacturer.unique().compute()))
    listaMarcas.remove('nan')

    cars=cars.apply(lambda row:cleanManufacturer2(row,listaMarcas),axis=1)
    cars=cars.drop(columns='make')#arriesgado, pero está muy sucia
    cars.condition=cars.condition.fillna('unknown')
    cars.condition= cars.condition.apply(cleanCondition)
    return cars


