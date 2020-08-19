import dask.bag as db
import numpy as np

stocks_per_core=628
stock_count=1255
date_count=1258
core_count=2
nfs = []

def main():
    for i in range(core_count):
        nf = np.zeros((date_count,min(stock_count-(i*stocks_per_core),stocks_per_core))).astype('float32')
        nfs.append(nf)


    stocks = db.read_text('stocks_closing_price_sorted.csv', encoding='utf-8')
    print(stocks.npartitions)

    df = stocks.map(extract_df_columns) \
    .to_dataframe()

    producetoqueue(df)


    for i in range(core_count):
        np.save('numpy/' + str(i), nfs[i])

def extract_column_value(line):
    return line.split(',')


def extract_df_columns(line):
    match = extract_column_value(line)
    row = {
        'symbol_id': int(match[1]),
        'date_id': int(match[0]),
		'close': float(match[2]),
		'core_id': int(int(match[1])/stocks_per_core)
    }
    return row





def producetoqueue(df):
    for row in df.itertuples():
        symbol_id = row.symbol_id - (row.core_id*stocks_per_core)
        nfs[row.core_id][row.date_id, symbol_id] = row.close
 



if __name__ == '__main__':
    main()