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

base_session_id = "20240516-10-"

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


file_path = "/home/yuzhen/projects/TianGong-AI-Multi-Agents/dags/aluminum/Al_test.xlsx"








# 最终产品
product_prompt = "The aluminum industry is a highly energy-intensive sector. The production of primary aluminum involves the extraction of alumina from bauxite ore and the subsequent electrolytic reduction of alumina to aluminum. The production of alumina is energy-intensive, and the electrolytic reduction process is even more energy-intensive. The production of primary aluminum is also associated with greenhouse gas emissions, which contribute to climate change. The aluminum industry is facing increasing pressure to reduce its energy consumption and greenhouse gas emissions. One way to reduce the energy consumption and greenhouse gas emissions of the aluminum industry is to increase the recycling of aluminum. Recycling aluminum requires much less energy than producing primary aluminum from bauxite ore. The recycling of aluminum also reduces the need for bauxite mining, which can have negative environmental and social impacts. The recycling of aluminum is therefore an important strategy for reducing the environmental impact of the aluminum industry."

distributor_formatter = partial(
    agent_to_agent_formatter,
    # session_id = f"""{base_session_id}distributor""",
)


demand_extractor_formatter = partial(
    agent_to_agent_formatter,
    prompt="extract the demand from the following text.",
    task_ids=["distributor"],
    # session_id = f"""{base_session_id}demand_extractor""",
)

technique_grader_formatter = partial(
    agent_to_agent_formatter,
    prompt="grade the following text from the perspective of technique.",
    task_ids=["demand_extractor"],
    # session_id = f"""{base_session_id}technique_grader""",
)

spatial_grader_formatter = partial(
    agent_to_agent_formatter,
    prompt="grade the following text from the perspective of spatial.",
    task_ids=["demand_extractor"],
    # session_id = f"""{base_session_id}spatial_grader""",
)

time_grader_formatter = partial(
    agent_to_agent_formatter,
    prompt="grade the following text from the perspective of time.",
    task_ids=["demand_extractor"],
    # session_id = f"""{base_session_id}time_grader""",
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
    dag_id="aluminumLCA1",
    default_args=default_args,
    description="aluminumLCA1",
    schedule_interval=None,
    tags=["aluminumLCA1"],
    catchup=False,
) as dag:

    distribution = PythonOperator(
        task_id="distributor",
        python_callable= distributor,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": product_prompt
                },
                "config": {"configurable": {"session_id": base_session_id}},
            }
        },
    ),
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
    selecting = PythonOperator(
        task_id="selector",
        python_callable=selector,
    )
    distribution >> damand_extraction >> [technique_grading, spatial_grading, time_grading] >> selecting