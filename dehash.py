import csv
import time
import os
import pandas as pd

# ---------------------------------------------------------------------------------------------------------------------
# Указать путь до большого файла с основной информацией (включая название файла)
path_big_file = '/Users/anton/Projects/WorkWithData/test001.csv'

# Указать путь до файла с хэшами, для которого нужно найти совпадения (включая название файла)
path_hashes = '/Users/anton/Projects/WorkWithData/test001_h.csv'

# Указать путь до папки, куда будут сохранены файлы, после разбивки основного на более мелкие
path_small_files = '/Users/anton/Projects/Automation/Automation/Рез'

# Указать путь для сохранения файла с результатом (включая название файла)
path_result = '/Users/anton/Projects/Automation/Automation/Рез/final.csv'

# Указать кол-во строк в большом файле
rows_in_file = 120000000

# ---------------------------------------------------------------------------------------------------------------------
name, format_file = (path_small_files + '/ver'), path_big_file[-4:]
rows_in_one_file = 1000000
numbers_of_files = int(rows_in_file // rows_in_one_file + 1) \
    if rows_in_file % rows_in_one_file != 0 \
    else int(rows_in_file / rows_in_one_file)
rows_counter, count_files = 0, 0

small_file = None

# ---------------------------------------------------------------------------------------------------------------------
print('----Разбиение файла----')
start_time = time.time()
with open(path_big_file, "r", encoding="utf8") as in_file:
    for hsh, number in csv.reader(in_file):
        if rows_counter % (rows_in_file // 100) == 0:
            print(f'Прогресс - {rows_counter / rows_in_file * 100} %')
        if rows_counter % rows_in_one_file == 0:
            if small_file:
                small_file.close()
                print(f'файл {count_files} из {numbers_of_files} готов')
            count_files += 1
            small_filename = f'{name}_{count_files}{format_file}'
            small_file = open(small_filename, "w")
            writer = csv.writer(small_file)
        writer.writerow((hsh, number))
        rows_counter += 1
    if small_file:
        small_file.close()
print('----Готово----\n')

print('----Нахождение совпадений----')
start_time = time.time()
df1 = pd.read_csv(path_hashes, names=['hash'], index_col=False)
df_result = []

for i, filename in enumerate(sorted(os.listdir(path_small_files))):
    if filename.endswith('.csv'):
        print(filename)
        df_n = pd.read_csv(
            f'{path_small_files}/{filename}',
            names=['hash', 'numbers'], index_col=False
        )
        df_result += pd.concat([df1.set_index('hash'), df_n.set_index('hash')],
                               axis=1, join='inner').reset_index()['numbers'].tolist()
        df1 = pd.DataFrame({'hash': list(set(df1['hash'].tolist()) - set(df_n['hash'].tolist()))})
        print(len(df_result))
        print(len(df1))
        print(f'ok{i}')
    if len(df1) == 0:
        break
print('----Готово----\n')

# ---------------------------------------------------------------------------------------------------------------------
pd.DataFrame({'a': df_result}).to_csv(path_result, index=False, header=False)
print(f'-------{time.time() - start_time}-------')
