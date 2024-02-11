import pandas as pd
import numpy as np
from shutil import rmtree
import sys 
from os import listdir, makedirs 
from os.path import isfile, join, exists 

def process_store(sell, supply, inventory, sell_parser):
    '''
    На вход 3 файла - продажи, закупки, инвентарь.
    На выход - фреймы данных, которые далее будут переводиться в csv
    '''
    supply.set_index('date', inplace=True) 
    inventory.set_index('date', inplace=True)
    
    ''' 
    Идентификационный код мы заменяем либо на 'a', либо на 'p' - все остальное нам не нужно
    Чтобы понять, какую букву взять - используем sell_parser - 8 элемент идентификационного кода
    '''
    sell.sku_num = sell.sku_num.map(sell_parser)

    def apple(daily): 
        return np.sum(daily == 'a')
    def pen(daily): 
        return np.sum(daily == 'p')
    
    '''
    Группирует по дате. По данным столбца 'sku_num' рассчитывается количество
    проданных за один день ручек и яблок. Происходит подразделение на 2 столбца:
    'apple', 'pen' - со значениями соответствующих количеств
    '''
    sells_daily = sell.groupby('date')['sku_num'].agg([apple, pen])
    '''
    Закупки за день = { Закупки за месяц, если день == 1 или 15
                      { 0               , иначе
    
    Изменения за день = Закупки за день - Продажи за день
    '''
    changes_daily = supply.reindex_like(sells_daily).fillna(0) - sells_daily
    
    '''
    Теперь суммируем значения за каждый день в месяцах, группируем по месяцам,
    а не по дням
    
    '2011-12-25'[:-2] = '2011-12-'
    '''
    monthly_cumsum_changes = changes_daily.groupby(lambda date: date[:-2]).agg(np.cumsum)
    extend_inventory = inventory.reindex_like(monthly_cumsum_changes).shift(1).fillna(axis=0, method='ffill').fillna(0)
    store_daily_state = extend_inventory + monthly_cumsum_changes
    stolen_monthly = store_daily_state.reindex_like(inventory) - inventory
    
    return store_daily_state, stolen_monthly, sells_daily.groupby(lambda date: date[:-2]).sum()

def agregate_statistics(stats):
    return pd.concat(stats).groupby(['year', 'state']).sum()

inp_dir = 'input'
out_dir = 'output'

sell_parser = lambda transaction: transaction[6]

all_files = [f for f in listdir(inp_dir) if isfile(join(inp_dir, f))] 

stores = [f[:-8] for f in all_files if f[-8:] == "sell.csv"] 

if not exists(out_dir): 
    makedirs(out_dir)
else:
    rmtree(out_dir)
    makedirs(out_dir)

    
statistics = []
for store in stores:
    '''
    Каждый шаг цикла соответствует работе с одним префиксом - store. С помощью этого префикса мы получаем доступ к
    соответствующим файлам о закупках, продажах, инвентаре.
    '''
    print ("Processing \"{0}\"".format(store[:-min(len(store), 1)]))

    try:
        inventory = pd.read_csv(inp_dir + '/' + store + 'inventory.csv') 
        sold = pd.read_csv(inp_dir + '/' + store + 'sell.csv')
        supply = pd.read_csv(inp_dir + '/' + store + 'supply.csv')
    except:
        print ("Error while reading \"{0}\" files, ignore".format(store[:-min(len(store), 1)]))
        continue

    state, stolen, sells = process_store(sold, supply, inventory, sell_parser)

    state.to_csv(out_dir + '/' + store + 'daily.csv') 
    stolen.to_csv(out_dir + '/' + store + 'steal.csv') 

    sells.index = stolen.index 
    
    stats = sells.join(stolen, lsuffix='_sold', rsuffix='_stolen').reset_index()
    stats['state'] = store[:2] 
    stats['test'] = store[:6]
    stats['year'] = stats.date.map(lambda date: date[:4]) 
    statistics.append(stats) 

agregate_statistics(statistics).to_csv(out_dir + '/states.csv') 
    
print('Finished successfully')

st = pd.read_csv("output/states.csv")
print(st)