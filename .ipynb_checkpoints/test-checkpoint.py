import pandas as pd
def process():
    ori_path = "C:\\Users\\GAKA\\bigstring\\OneDrive - 공주대학교\\바탕 화면\\논문관련\\2021 확장논문(이미지_태양객체+조도)\\소스\\"
    file_name = "5L128N_20200408.csv"

    result = pd.read_csv(ori_path+file_name)
    result['datetime'] = pd.to_datetime(result['datetime'])
    result = result.set_index("datetime")

    # print(result)

    remake = pd.DataFrame()
    remake['CAS_UVI_1H']=result.UVI_org.resample('1H').mean()
    remake['DNN_UVI_1H'] = result.DNN_UVI_org.resample('1H').mean()
    remake['ae_1H'] = result.ae.resample('1H').mean()
    remake.to_csv(ori_path+"remake_"+file_name)
    print(remake)
    # print(result.DNN_UVI_org.resample('10T').mean())

if __name__ == '__main__':
    process()