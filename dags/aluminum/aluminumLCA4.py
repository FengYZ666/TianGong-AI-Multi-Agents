import json
import os
from datetime import timedelta
from functools import partial

import httpx
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator as DummyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from httpx import RequestError

import pandas as pd
import json
import re

BASE_URL = "http://host.docker.internal:7778"
TOKEN = Variable.get("FAST_API_TOKEN")


agent_url = "/openai_agent/invoke"
openai_url = "/openai/invoke"

def post_request(
    post_url: str,
    formatter: callable,
    ti: any,
    data_to_send: dict = None,
):
    if not data_to_send:
        data_to_send = formatter(ti)
    headers = {"Authorization": f"Bearer {TOKEN}"}
    with httpx.Client(base_url=BASE_URL, headers=headers, timeout=120) as client:
        try:
            response = client.post(post_url, json=data_to_send)

            response.raise_for_status()

            data = response.json()

            return data

        except RequestError as req_err:
            print(f"An error occurred during the request: {req_err}")
        except Exception as err:
            print(f"An unexpected error occurred: {err}")


# agent to agent
def agent_to_agent_formatter(ti, prompt: str = "", task_ids: list = None, session_id: str = ""):
    if task_ids:
        results = []
        for task_id in task_ids:
            data = ti.xcom_pull(task_ids=task_id)
            task_output = data["output"]["output"]  # Output from agent
            task_output_with_id = (
                f"{task_id}:\n {task_output}"  # Concatenate task ID with its output
            )
            results.append(task_output_with_id)
        content = "\n\n".join(results)  # Join all task results into a single string
        formatted_data = {
            "input": {"input": prompt + "\n\n INPUT:" + content},
            "config": {"configurable": {"session_id": session_id}},
        }
        print("look at formatted data: ", formatted_data)
        return formatted_data
    else:
        pass

# agent to openai
def agent_to_openai_formatter(ti, prompt: str = None, task_ids: list = None):
    if task_ids:
        results = []
        for task_id in task_ids:
            data = ti.xcom_pull(task_ids=task_id)
            task_output = data["output"]["output"]  # Output from agent
            task_output_with_id = (
                f"{task_id}:\n {task_output}"  # Concatenate task ID with its output
            )
            results.append(task_output_with_id)
        content = "\n\n".join(results)  # Join all task results into a single string
        if prompt:
            content = prompt + "\n\n" + content
        formatted_data = {"input": content}
        return formatted_data
    else:
        pass

# openai to agent
def openai_to_agent_formatter(ti, prompt: str = None, task_ids: list = None):
    if task_ids:
        results = []
        for task_id in task_ids:
            data = ti.xcom_pull(task_ids=task_id)
            task_output = data["output"]["content"]  # Output from openai
            task_output_with_id = (
                f"{task_id}:\n {task_output}"  # Concatenate task ID with its output
            )
            results.append(task_output_with_id)
        content = "\n\n".join(results)  # Join all task results into a single string
        formatted_data = {
            "input": {"input": content},
            "config": {"configurable": {"session_id": ""}},
        }
        return formatted_data
    else:
        pass

# openai to openai
def openai_to_openai_formatter(ti, prompt: str = None, task_ids: list = None):
    if task_ids:
        results = []
        for task_id in task_ids:
            data = ti.xcom_pull(task_ids=task_id)
            task_output = data["output"]["content"]  # Output from openai
            task_output_with_id = (
                f"{task_id}:\n {task_output}"  # Concatenate task ID with its output
            )
            results.append(task_output_with_id)
        content = "\n\n".join(results)  # Join all task results into a single string
        if prompt:
            content = prompt + "\n\n" + content
        formatted_data = {"input": content}
        return formatted_data
    else:
        pass


# def filter_process(file_path):
#     # 读取'End_Process'工作表
#     data = pd.read_excel(file_path, sheet_name='End_Process')

#     # 过滤ID为99的数据
#     data = data.loc[data['ID'] == 99]

#     # 只保留特定的列
#     data = data[['ID', '工艺过程名称EN']]

#     # 将DataFrame转换为JSON
#     filtered_process_json = data.to_json(orient='records')

#     return filtered_process_json

# def filter_flow(file_path):
#     # 读取'End_Process'工作表
#     data = pd.read_excel(file_path, sheet_name='End_Flow')

#     # 过滤ID为99且输入/输出为Input的数据
#     data = data.loc[(data['工艺过程ID'] == 99) & (data['输入/输出'] == 'Input')]

#     # 只保留特定的列
#     data = data[['工艺过程ID', '流名称','流ID']]

#     # 将DataFrame转换为JSON
#     filtered_flow_json = data.to_json(orient='records')

#     return filtered_flow_json

# file_path = "dags/aluminum/Al_test.xlsx"
# filtered_process_json = filter_process(file_path)
# filtered_flow_json = filter_flow(file_path)

# # 连接两个JSON字符串
# filtered_data_json = filtered_process_json + filtered_flow_json

filtered_data_path = 'dags/aluminum/filtered_data.txt'
with open(filtered_data_path, 'r') as f:
    filtered_data_json = f.read()


base_session_id = "20240517-35-"

# prompt
distributor_prompt = f"""
- The data is stored in the following JSON format:{filtered_data_json}
- Analyse the name of the process to determine the industry to which it belongs. 
- Analyse the English name of each flow to determine the top three industries that each flow is commonly used in. 
- Output as results the names and UUIDs of all flows whose common industries contain the industry to which the process belongs. Your output must be in JSON format. The fields are as follows: ['process_ID', 'process_name', 'flow_name', 'UUID']
- You mustn't search the internet for information. Just use your common sense.
"""



# agent to agent
data_path = 'dags/aluminum/Al_test.xlsx'

def agent_to_agent_formatter(ti, prompt: str = "", task_ids: list = None, session_id: str = ""):
    if task_ids:
        results = []
        for task_id in task_ids:
            data = ti.xcom_pull(task_ids=task_id)
            task_output = data["output"]["output"]  # Output from agent
            task_output_with_id = (
                f"{task_id}:\n {task_output}"  # Concatenate task ID with its output
            )
            results.append(task_output_with_id)
        content = "\n\n".join(results)  # Join all task results into a single string
        formatted_data = {
            "input": {"input": prompt + "\n\n INPUT:" + content},
            "config": {"configurable": {"session_id": session_id}},
        }
        return formatted_data
    else:
        pass





def json_agent_to_demand_extractor_formatter(ti, prompt: str = "", task_ids: list = None, session_id: str = ""):
    if task_ids:
        process_json = ""  # Initialize as empty string
        flow_json = ""  # Initialize as empty string
        for task_id in task_ids:
            data_list = ti.xcom_pull(task_ids=task_id)
            data_list = data_list["output"]["output"]
            print(data_list)
            
            # 使用正则表达式提取 JSON 字符串
            pattern = re.compile(r"```json(.*?)```", re.DOTALL)
            match = pattern.search(data_list)

            if match:
                json_string = match.group(1).strip()
                # 将 JSON 字符串转换为 Python 列表
                data_list = json.loads(json_string)
            else:
                print("No JSON string found.")
                continue
            print(data_list)
            for data in data_list:
                process_id = data.get('process_ID')
                uuid = data.get('UUID')
                # Read Excel file
                xls = pd.ExcelFile(data_path)
                end_process_df = pd.read_excel(xls, 'End_Process')
                end_flow_df = pd.read_excel(xls, 'End_Flow')
                # Extract rows and keep specified columns
                process_data = end_process_df[end_process_df['process_ID'] == process_id][['process_ID', 'process_name', 'location', 'validity_start']]
                flow_data = end_flow_df[end_flow_df['UUID'] == uuid][['flow_name', 'UUID', 'flow_classification', 'flow_type']]
                # Convert to JSON string and append to existing strings
                process_json += process_data.to_json(orient='records')
                flow_json += flow_data.to_json(orient='records')

        # Prepare new input prompt
        prompt = f"""Based on the following extracted data, generate anoher json string for the required process. 
        Process Data:{process_json}
        Flow Data:{flow_json}
        The fields are as follows: ['flow_name', 'UUID', 'flow_classification', 'flow_type', 'location',  'validity_start']
        Your output must ONlY be an JSON string.
        """
        print("look at prompt: ", prompt)

        formatted_data = {
            "input": {"input": prompt},
            "config": {"configurable": {"session_id": session_id}},
        }
        print("look at formatted data: ", formatted_data)
        return formatted_data  # Return the dictionary directly
    else:
        pass


def demand_extractor_to_technique_grader_formatter(ti, prompt: str = "", task_ids: list = None, session_id: str = ""):
    if task_ids:
        flow_data_list = []  # List to store initial flow data

        for task_id in task_ids:
            flow_data_list = ti.xcom_pull(task_ids=task_id)
            flow_data_list = flow_data_list["output"]["output"]
            print(flow_data_list)
        # Read Excel file
        xls = pd.ExcelFile(data_path)
        print("1111111",xls)
        # Extract new flow data from the "Start_Flow" sheet
        start_flow_df = pd.read_excel(xls, 'Start_Flow')
        print("1111111",start_flow_df)
        filtered_start_flow_df = start_flow_df[(start_flow_df['Input/Output'] == 'Output') & (start_flow_df['reference'] == 'Siu')][['UUID', 'flow_name', 'flow_type', 'flow_classification']]
        print("1111111",filtered_start_flow_df)
        new_flow_data_list = filtered_start_flow_df.to_dict(orient='records')
        print("111111111111",new_flow_data_list)
        new_flow_data_list = json.dumps(new_flow_data_list)
        print("111111111111",new_flow_data_list)
        # Prepare new input prompt
        prompt = f"""For each flow in New Flow Data, do a grading based on the Initial Flow Data. Grading criteria are as follows:
        Rating Criteria:
        - Grade 3: UUID matches
        - Grade 2: flow_name matches and both flow_type and flow_classification match
        - Grade 1: flow_name matches and flow_type matches
        - Grade 0: Other cases
        Initial Flow Data: {flow_data_list}
        New Flow Data: {new_flow_data_list}
        Your output must be in JSON format. The fields are as follows: ['UUID', 'flow_name', 'flow_type', 'flow_classification', 'grade']
        """
        print("look at prompt: ", prompt)
        formatted_data = {
            "input": {"input": prompt},
            "config": {"configurable": {"session_id": session_id}},
        }
        print("look at formatted data: ", formatted_data)
        return formatted_data  # Return the dictionary directly
    else:
        pass

def demand_extractor_to_spatial_grader_formatter(ti, prompt: str = "", task_ids: list = None, session_id: str = ""):
    if task_ids:
        flow_data_list = []  # List to store initial flow data

        for task_id in task_ids:
            flow_data_list = ti.xcom_pull(task_ids=task_id)
            flow_data_list = flow_data_list["output"]["output"]
            print(flow_data_list)
        # Read Excel file
        xls = pd.ExcelFile(data_path)
        print("1111111",xls)
        # Extract new flow data from the "Start_Flow" sheet
        start_process_df = pd.read_excel(xls, 'Start_Process')
        print("1111111",start_process_df)
        filtered_start_process_df = start_process_df[['process_ID','process_name','location']]
        print("1111111",filtered_start_process_df)
        new_process_data_list = filtered_start_process_df.to_dict(orient='records')
        print("111111111111",new_process_data_list)
        new_process_data_list = json.dumps(new_process_data_list)
        print("111111111111",new_process_data_list)
        # Prepare new input prompt
        prompt = f"""For each process in New Process Data, do a grading based on the Initial Flow Data. Grading criteria are as follows:
        Rating Criteria:
        - Grade 3: Location from the New Process Data is exactly the same as the location from the Initial Flow Data.
        - Grade 2: Location from the Initial Flow Data is a sub-region of the location from the New Process Data.
        - Grade 1: Location from the New Process Data is a sub-region of the location from the Initial Flow Data.
        - Grade 0: Other cases
        Initial Flow Data: {flow_data_list}
        New Process Data: {new_process_data_list}
        Your output must be in JSON format. The fields are as follows: ['process_ID', 'process_name', 'location', 'grade']
        """
        print("look at prompt: ", prompt)
        formatted_data = {
            "input": {"input": prompt},
            "config": {"configurable": {"session_id": session_id}},
        }
        print("look at formatted data: ", formatted_data)
        return formatted_data  # Return the dictionary directly
    else:
        pass


def demand_extractor_to_time_grader_formatter(ti, prompt: str = "", task_ids: list = None, session_id: str = ""):
    if task_ids:
        flow_data_list = []  # List to store initial flow data

        for task_id in task_ids:
            flow_data_list = ti.xcom_pull(task_ids=task_id)
            flow_data_list = flow_data_list["output"]["output"]
            print(flow_data_list)

        # Read Excel file
        xls = pd.ExcelFile(data_path)
        print("Excel file loaded successfully.")
        
        # Extract new flow data from the "Start_Process" sheet
        start_process_df = pd.read_excel(xls, 'Start_Process')
        print("Start_Process sheet content:\n", start_process_df.head())
        
        # Filter the DataFrame
        filtered_start_process_df = start_process_df[['process_ID', 'process_name', 'validity_start']]
        print("Filtered Start_Process DataFrame:\n", filtered_start_process_df.head())
        
        # Convert DataFrame to list of dictionaries
        new_process_data_list = filtered_start_process_df.to_dict(orient='records')
        print("New Process Data List (dict format):\n", new_process_data_list)
        
        # Ensure all items in the list are serializable
        try:
            new_process_data_list_json = json.dumps(new_process_data_list, default=str)
        except TypeError as e:
            print("Error serializing new_process_data_list:", e)
            raise

        print("New Process Data List (JSON format):\n", new_process_data_list_json)
        
        # Prepare new input prompt
        prompt = f"""For each process in New Process Data, do a grading based on the Initial Flow Data. Grading criteria are as follows:
        Rating Criteria:
        - Grade 4: validity_start from the New Process Data is the same as the validity_start from the Initial Flow Data.
        - Grade 3: The difference between the year of validity_start from the New Process Data and the year of validity_start from the Initial Flow Data is less than 2 year.
        - Grade 2: The difference between the year of validity_start from the New Process Data and the year of validity_start from the Initial Flow Data is more than 2 year but less than 3 years.
        - Grade 1: The difference between the year of validity_start from the New Process Data and the year of validity_start from the Initial Flow Data is more than 3 year but less than 4 years.
        - Grade 0: The difference between the year of validity_start from the New Process Data and the year of validity_start from the Initial Flow Data is more than 4 year.
        Initial Flow Data: {flow_data_list}
        New Process Data: {new_process_data_list_json}
        Your output must be in JSON format. The fields are as follows: ['process_ID', 'process_name', 'vlidity_start','grade']
        """
        print("look at prompt: ", prompt)
        
        formatted_data = {
            "input": {"input": prompt},
            "config": {"configurable": {"session_id": session_id}},
        }
        print("look at formatted data: ", formatted_data)
        return formatted_data  # Return the dictionary directly
    else:
        pass


distributor_formatter = partial(
    agent_to_agent_formatter,
    session_id = f"""{base_session_id}distributor""",
)

json_agent_formatter = partial(
    agent_to_agent_formatter,
    task_ids=["distributor"],
    session_id = f"""{base_session_id}json_agent""",
    prompt = "Based on the input, extract the json string. You ONLY output the json string in the input.",
)

demand_extractor_formatter = partial(
    json_agent_to_demand_extractor_formatter,
    task_ids=["json_agent"],
    session_id = f"""{base_session_id}demand_extractor""",
)

technique_grader_formatter = partial(
    demand_extractor_to_technique_grader_formatter,
    task_ids=["demand_extractor"],
    session_id = f"""{base_session_id}technique_grader""",
)

spatial_grader_formatter = partial(
    demand_extractor_to_spatial_grader_formatter,
    task_ids=["demand_extractor"],
    session_id = f"""{base_session_id}spatial_grader""",
)

time_grader_formatter = partial(
    demand_extractor_to_time_grader_formatter,
    prompt="grade the following text from the perspective of time.",
    task_ids=["demand_extractor"],
    session_id = f"""{base_session_id}time_grader""",
)


selector_formatter = partial(
    agent_to_agent_formatter,
    prompt="select the following text.",
    task_ids=["technique_grader", "spatial_grader", "time_grader"],
    session_id = f"""{base_session_id}selector""",
)

# 定义任务，callable对象
distributor = partial(post_request, post_url=agent_url, formatter=distributor_formatter)
json_agent = partial(post_request, post_url=agent_url, formatter=json_agent_formatter)
demand_extractor = partial(post_request, post_url=agent_url, formatter=demand_extractor_formatter)
# grader = partial(post_request, post_url=agent_url, formatter=grader_formatter)
technique_grader = partial(post_request, post_url=agent_url, formatter=technique_grader_formatter)
spatial_grader = partial(post_request, post_url=agent_url, formatter=spatial_grader_formatter)
time_grader = partial(post_request, post_url=agent_url, formatter=time_grader_formatter)
selector = partial(post_request, post_url=agent_url, formatter=selector_formatter)


default_args = {
    "owner": "airflow",
}

with DAG(
    dag_id="aluminumLCA3",
    default_args=default_args,
    description="aluminumLCA3",
    schedule_interval=None,
    tags=["aluminumLCA3"],
    catchup=False,
) as dag:

    distribution = PythonOperator(
        task_id="distributor",
        python_callable= distributor,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": distributor_prompt
                },
                "config": {"configurable": {"session_id": base_session_id}},
            }
        },
    ),
    json_agent = PythonOperator(
        task_id="json_agent",
        python_callable=json_agent,
    )
    damand_extraction = PythonOperator(
        task_id="demand_extractor",
        python_callable=demand_extractor,
    )
    technique_grading = PythonOperator(
        task_id="technique_grader",
        python_callable=technique_grader,
    )
    spatial_grading = PythonOperator(
        task_id="spatial_grader",
        python_callable=spatial_grader,
    )
    time_grading = PythonOperator(
        task_id="time_grader",
        python_callable=time_grader,
    )
    # selecting = PythonOperator(
    #     task_id="selector",
    #     python_callable=selector,
    # )

    distribution >> json_agent >> damand_extraction >> [technique_grading, spatial_grading, time_grading] #>> selecting
    # distribution >> damand_extraction >> [technique_grading, spatial_grading, time_grading] >> selecting