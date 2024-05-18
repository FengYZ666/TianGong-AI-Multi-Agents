import pandas as pd

def convert_excel_to_json(file_path):
    # 读取Excel文件的所有工作表
    data = pd.read_excel(file_path, sheet_name=None)

    # 将每个工作表的DataFrame转换为JSON，并存储在一个字典中
    json_dict = {sheet_name: df.to_json(orient='records') for sheet_name, df in data.items()}

    return json_dict

file_path = "dags/aluminum/Al_test.xlsx"
json_dict = convert_excel_to_json(file_path)

# 打印每个工作表的JSON字符串
for sheet_name, json_str in json_dict.items():
    print(f"Sheet name: {sheet_name}")
    print(json_str)

