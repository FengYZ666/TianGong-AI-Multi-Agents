import pandas as pd

def filter_process(file_path):
    # 读取'End_Process'工作表
    data = pd.read_excel(file_path, sheet_name='End_Process')

    # 过滤ID为99的数据
    data = data.loc[data['process_ID'] == 99]

    # 只保留特定的列
    data = data[['process_ID', 'process_name']]

    # 将DataFrame转换为JSON
    filtered_process_json = data.to_json(orient='records')

    return filtered_process_json

def filter_flow(file_path):
    # 读取'End_Process'工作表
    data = pd.read_excel(file_path, sheet_name='End_Flow')

    # 过滤ID为99且输入/输出为Input的数据
    data = data.loc[(data['process_ID'] == 99) & (data['Input/Output'] == 'Input')]

    # 只保留特定的列
    data = data[['process_ID', 'flow_name','UUID']]

    # 将DataFrame转换为JSON
    filtered_flow_json = data.to_json(orient='records')

    return filtered_flow_json

file_path = "dags/aluminum/Al_test.xlsx"
filtered_process_json = filter_process(file_path)
filtered_flow_json = filter_flow(file_path)

# 连接两个JSON字符串
filtered_data_json = filtered_process_json + filtered_flow_json
print(filtered_data_json)

# 储存数据为txt文件
with open('dags/aluminum/filtered_data.txt', 'w') as f:
    f.write(filtered_data_json)