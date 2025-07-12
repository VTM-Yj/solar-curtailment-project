import pandas as pd
from datetime import datetime, timedelta
import math

# 读取任务 trace（PAI 任务）
trace_path = "../data/pai_job_duration_estimate_100K.csv"
trace_df = pd.read_csv(trace_path)
base_time = datetime(2023, 6, 1, 0, 0)

def convert_trace_to_tasks(df, num_tasks=1000):
    df = df.head(num_tasks).copy()
    df["arrival_time"] = df["submit_time"].apply(lambda x: base_time + timedelta(seconds=int(x)))
    df["power_requirement_MW"] = df["num_gpu"].fillna(0).apply(lambda x: round(float(x) * 0.8 if x > 0 else 0.5, 2))
    df["duration_slots"] = df["duration"].apply(lambda x: max(1, math.ceil(x / 1800)))
    task_df = df[["job_id", "arrival_time", "power_requirement_MW", "duration_slots"]].copy()
    task_df = task_df.rename(columns={"job_id": "task_id"})
    task_df["executed"] = False
    task_df["start_time"] = None
    return task_df

task_df = convert_trace_to_tasks(trace_df, num_tasks=1000)

# 读取电力供应数据（Larks Green）
power_df = pd.read_csv("../data/larks_green_curtailment_2023_06_01_to_06_07.csv")
power_df["datetime"] = pd.to_datetime(power_df["datetime"])
power_df["available_power"] = power_df["actual_MW"].apply(lambda x: max(0, x))
power_df = power_df.reset_index(drop=True)

# 执行调度逻辑
for idx, task in task_df.iterrows():
    task_time_index = power_df[power_df["datetime"] >= task["arrival_time"]].index

    for i in task_time_index:
        window = power_df.iloc[i : i + task["duration_slots"]]
        if len(window) < task["duration_slots"]:
            continue
        if all(window["available_power"] >= task["power_requirement_MW"]):
            task_df.at[idx, "executed"] = True
            task_df.at[idx, "start_time"] = power_df.at[i, "datetime"]
            for j in range(i, i + task["duration_slots"]):
                power_df.at[j, "available_power"] -= task["power_requirement_MW"]
            break

# 保存结果
output_path = "../output/pai_scheduler_result.csv"
task_df.to_csv(output_path, index=False)
print(f"✅ 调度完成，结果保存至：{output_path}")
