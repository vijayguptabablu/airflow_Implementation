import pandas as pd

path = "/home/ubuntu/data.csv"

def dataframe():
    df = pd.read_csv(path)
    return df

df = dataframe()

def get_name():
    name = df[df['name'].isin(['Dinesh', 'Shruthy'])]
    print(name)
    return name