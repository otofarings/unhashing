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

# ---------------------------------------------------------------------------------------------------------------------


class Unhash:

    def __init__(self):

        self.name = (path_small_files + '/ver')
        self.format_file = path_big_file[-4:]

        self.rows_in_file = sum(1 for line in open(path_big_file))
        self.rows_in_one_file = 1000000
        self.numbers_of_files = int(self.rows_in_file // self.rows_in_one_file + 1) \
            if self.rows_in_file % self.rows_in_one_file != 0 \
            else int(self.rows_in_file / self.rows_in_one_file)
        self.rows_counter, self.count_files = 0, 0

        self.small_file = None
        self.df_for_unhash = None
        self.lst_result = []

# ---------------------------------------------------------------------------------------------------------------------
    def start_script(self):
        start_time = time.time()
        self.split_big_file()
        self.get_file_with_hashes()
        self.iter_throw_small_files()
        self.save_results()
        print(f'-------{time.time() - start_time}-------')

    def split_big_file(self):
        print('----Разбиение файла----')
        start_time = time.time()
        with open(path_big_file, "r", encoding="utf8") as in_file:
            for hsh, number in csv.reader(in_file):
                if self.rows_counter % (self.rows_in_file // 100) == 0:
                    print(f'Прогресс - {self.rows_counter / self.rows_in_file * 100} %')
                if self.rows_counter % self.rows_in_one_file == 0:
                    if small_file:
                        small_file.close()
                        print(f'файл {self.count_files} из {self.numbers_of_files} готов')
                    self.count_files += 1
                    small_filename = f'{self.name}_{self.count_files}{self.format_file}'
                    small_file = open(small_filename, "w")
                    writer = csv.writer(small_file)
                writer.writerow((hsh, number))
                self.rows_counter += 1
            if small_file:
                small_file.close()
        print(f'-------{time.time() - start_time}-------')
        print('----Готово----\n')

    def get_file_with_hashes(self):
        start_time = time.time()
        self.df_for_unhash = pd.read_csv(path_hashes, names=['hash'], index_col=False)
        print(f'-------{time.time() - start_time}-------')

    def iter_throw_small_files(self):
        start_time = time.time()
        for i, filename in enumerate(sorted(os.listdir(path_small_files))):
            if filename.endswith('.csv'):
                print(filename)
                df_n = pd.read_csv(
                    f'{path_small_files}/{filename}',
                    names=['hash', 'numbers'], index_col=False
                )
                self.unhashing(df_n)
                print(f'ok{i}')
            if len(self.df_for_unhash) == 0:
                break
        print(f'-------{time.time() - start_time}-------')

    def unhashing(self, df):
        start_time = time.time()
        self.lst_result += pd.concat([self.df_for_unhash.set_index('hash'), df.set_index('hash')],
                                     axis=1, join='inner').reset_index()['numbers'].tolist()
        self.df_for_unhash = pd.DataFrame(
            {'hash': list(set(self.df_for_unhash['hash'].tolist()) - set(df['hash'].tolist()))}
        )
        print(f'-------{time.time() - start_time}-------')

# ---------------------------------------------------------------------------------------------------------------------
    def save_results(self):
        start_time = time.time()
        pd.DataFrame({'a': self.lst_result}).to_csv(path_result, index=False, header=False)
        print(f'-------{time.time() - start_time}-------')


if __name__ == '__main__':
    x = Unhash()
    x.start_script()
