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

BASE_URL = "http://host.docker.internal:7778"
TOKEN = Variable.get("FAST_API_TOKEN")


agent_url = "/openai_agent/invoke"
openai_url = "/openai/invoke"

base_session_id = "20240516-12-"

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




# prompt
distributor_prompt = f"""
- Analyse the name of the process to determine the industry to which it belongs. 
- Analyse the English name of each flow to determine the top three industries that each flow is commonly used in. 
- Output as results the names and UUIDs of all flows whose common industries contain the industry to which the process belongs.
- You mustn't search the internet for information. Just use your common sense.
{filtered_data_json}
"""

demand_extractor_prompt = f"""
"""


distributor_formatter = partial(
    agent_to_agent_formatter,
    session_id = f"""{base_session_id}distributor""",
)


demand_extractor_formatter = partial(
    agent_to_agent_formatter,
    prompt="extract the demand from the following text.",
    task_ids=["distributor"],
    session_id = f"""{base_session_id}demand_extractor""",
)

technique_grader_formatter = partial(
    agent_to_agent_formatter,
    prompt="grade the following text from the perspective of technique.",
    task_ids=["demand_extractor"],
    session_id = f"""{base_session_id}technique_grader""",
)

spatial_grader_formatter = partial(
    agent_to_agent_formatter,
    prompt="grade the following text from the perspective of spatial.",
    task_ids=["demand_extractor"],
    session_id = f"""{base_session_id}spatial_grader""",
)

time_grader_formatter = partial(
    agent_to_agent_formatter,
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
    dag_id="aluminumLCA2",
    default_args=default_args,
    description="aluminumLCA2",
    schedule_interval=None,
    tags=["aluminumLCA2"],
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
    # damand_extraction = PythonOperator(
    #     task_id="demand_extractor",
    #     python_callable=demand_extractor,
    # )
    # technique_grading = PythonOperator(
    #     task_id="technique_grader",
    #     python_callable=technique_grader,
    # )
    # spatial_grading = PythonOperator(
    #     task_id="spatial_grader",
    #     python_callable=spatial_grader,
    # )
    # time_grading = PythonOperator(
    #     task_id="time_grader",
    #     python_callable=time_grader,
    # )
    # selecting = PythonOperator(
    #     task_id="selector",
    #     python_callable=selector,
    # )

    distribution
    # distribution >> damand_extraction >> [technique_grading, spatial_grading, time_grading] >> selecting