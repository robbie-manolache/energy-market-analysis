import pandas as pd

def parse_weather_data(bom_df, name):
    """
    """
    df_list = []
    for col in [c for c in bom_df.columns.tolist() 
                if c.startswith(name)]:
        df = bom_df.copy()[['Date', col]]
        time = col.split('_')[-1]
        df.loc[:, 'TIMESTAMP'] = pd.to_datetime(df['Date'].apply(
                                 lambda x: '-'.join([x,time])))
        df = df.drop('Date', 1).rename(columns={col:name})
        df_list.append(df)
    return pd.concat(df_list, ignore_index=True
            ).dropna().sort_values(by='TIMESTAMP')

def normalize_series(series):
    """
    """
    mean = series.mean()
    std = series.std()
    return (series-mean)/std