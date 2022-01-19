import pandas as pd
import numpy as np

def make_underzero_reverse_df(df:pd.DataFrame,col):
    reverse_df = df[::-1]
    reverse_df[col] = reverse_df.apply(lambda x: x[col] * -1, axis=1)
    return reverse_df


def make_Relatively_reverse_df(df,col):
    reverse_df = df[::-1]
    # reverse_df = df[col]
    reverse_df = reverse_df.reset_index()
    # print(reverse_df)
    table_list = []
    old_temp = 0
    for i in range(len(reverse_df)):
        row = []
        if i==0:
            row.append(i)
            row.append(reverse_df[col][i])
            old_temp = reverse_df[col][i]
        else:
            old_temp = old_temp + (reverse_df[col][i-1]-reverse_df[col][i])
            row.append(i)
            row.append(old_temp)

        table_list.append(row)
    temp = pd.DataFrame(table_list, columns=['Timestamp',col])
    # temp['flag'] = temp.apply(lambda x: "rev", axis=1)

    return temp

import matplotlib.pyplot as plt

def make_Relatively_reverse_df_4ending(df):
    temp = pd.DataFrame(columns=['CCT'])
    temp.loc[0,'CCT'] = df['re_0'][len(df)-1]
    for i in range(1, 100):
        temp.loc[i,'CCT'] = temp['CCT'][i-1]+(df['re_0'][len(df)-i]-df['re_0'][len(df)-i-1])

    # plt.scatter(range(100),df['re_0'][len(df)-100:], color='red', marker='o', s=1)
    # plt.scatter(range(100,200),temp['CCT'], color='blue', marker='o', s=1)
    # plt.show()
    return temp

if __name__ == '__main__':
    import numpy as np
    df = pd.read_csv("C:\\Users\\GAKA\\Desktop\\workspace\\Python\\TadGAN_final\\RealTime_system\\result\\result_2020-04-07_15%_new.csv")
    print(df.loc[0, 'datetime'])
    if pd.isnull(df.loc[0, 'datetime']):
        print("!!!!")

