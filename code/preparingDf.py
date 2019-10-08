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

def cleanOdometer(odo):
    '''Categoriza la columna odometro en rangos de millas'''
    if odo == 0.01:
        return 'unknown'
    elif odo == 0.0:
        return '0'
    elif 0<odo<10000:
        return '<10000'
    elif 10000<odo<30000:
        return '10000<30000'
    elif 30000<odo<60000:
        return '30000<60000'
    elif 60000<odo<90000:
        return '60000<90000'
    elif 90000<odo<120000:
        return '90000<120000'
    elif 120000<odo<150000:
        return '120000<150000'
    elif 150000<odo<180000:
        return '150000<180000'
    elif 180000<odo<210000:
        return '180000<210000'
    elif odo>210000:
        return '>210000'
    
def cleanWeather(weather):
    '''Categoriza weather redondeando'''
    try:
        return str(int(round(weather,1)))
    except:
        return 'unknown'

def cleanAllDf():
    client=getDaskClient()
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
    cars=cars.drop(columns=['Id']).compute()
    cars.cylinders=cars.cylinders.fillna('other')

    cars.odometer=cars.odometer.fillna(0.01)


    cars.fuel=cars.fuel.fillna('other')

    cars.odometer= cars.odometer.apply(cleanOdometer)
    cars.odometer=cars.odometer.fillna('unknown')

    cars.title_status=cars.title_status.fillna('missing')
    cars.transmission=cars.transmission.fillna('other')

    cars.drive=cars.drive.fillna('unknown')

    cars.type=cars.type.fillna('unknown')

    cars=cars.drop(columns='size')

    cars=cars.drop(columns='paint_color')

    cars.weather=cars.weather.apply(cleanWeather)

    cars.to_csv('../output/cleanDF.csv')

    return cars


