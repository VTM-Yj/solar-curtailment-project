import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import timedelta

# === Load scheduler result ===
task_df = pd.read_csv("../output/pai_scheduler_result.csv")
task_df["arrival_time"] = pd.to_datetime(task_df["arrival_time"])
task_df["start_time"] = pd.to_datetime(task_df["start_time"])

# === Figure 1: Number of Executed vs Non-Executed Tasks ===
plt.figure(figsize=(6, 4))
sns.countplot(data=task_df, x="executed")
plt.title("Task Execution Status")
plt.xlabel("Execution Status")
plt.ylabel("Number of Tasks")
plt.xticks([0, 1], ["Not Executed", "Executed"])
plt.tight_layout()
plt.savefig("../figures/task_execution_status.png")
plt.close()

# === Figure 2: Task Success Rate by Power Requirement ===
task_df["power_bin"] = pd.cut(task_df["power_requirement_MW"], bins=[0, 0.5, 1.0, 1.5, 2.0, 3.0])
success_rate = task_df.groupby("power_bin", observed=False)["executed"].mean().reset_index()
success_rate["success_rate"] = success_rate["executed"] * 100

plt.figure(figsize=(7, 4))
sns.barplot(data=success_rate, x="power_bin", y="success_rate")
plt.title("Task Success Rate by Power Requirement")
plt.ylabel("Success Rate (%)")
plt.xlabel("Power Requirement Range (MW)")
plt.xticks(rotation=30)
plt.tight_layout()
plt.savefig("../figures/success_rate_by_power.png")
plt.close()

# === Figure 3: Available vs Used Green Energy ===
power_df = pd.read_csv("../data/larks_green_curtailment_2023_06_01_to_06_07.csv")
power_df["datetime"] = pd.to_datetime(power_df["datetime"])
power_df["available_power"] = power_df["actual_MW"].apply(lambda x: max(0, x))

used_power_df = power_df[["datetime", "available_power"]].copy()
used_power_df["used_power"] = 0.0  # instead of 0

for _, task in task_df[task_df["executed"]].iterrows():
    for i in range(task["duration_slots"]):
        t = task["start_time"] + timedelta(minutes=30 * i)
        if t in used_power_df["datetime"].values:
            used_power_df.loc[used_power_df["datetime"] == t, "used_power"] += task["power_requirement_MW"]

plt.figure(figsize=(10, 4))
plt.plot(used_power_df["datetime"], used_power_df["available_power"], label="Available Green Power", linewidth=1.2)
plt.plot(used_power_df["datetime"], used_power_df["used_power"], label="Used Green Power", linewidth=1.2)
plt.fill_between(used_power_df["datetime"], used_power_df["used_power"], color="skyblue", alpha=0.3)
plt.legend()
plt.ylabel("Power (MW)")
plt.xlabel("Time")
plt.title("Available vs Used Green Power Over Time")
plt.tight_layout()
plt.savefig("../figures/green_power_usage.png")
plt.close()

# === Figure 4: Task Scheduling Density Over Time ===
density_df = task_df[task_df["executed"]].groupby("start_time").size().reset_index(name="task_count")

plt.figure(figsize=(10, 4))
plt.plot(density_df["start_time"], density_df["task_count"], label="Scheduled Tasks")
plt.xlabel("Time")
plt.ylabel("Number of Tasks")
plt.title("Scheduling Density Per 30-Minute Interval")
plt.tight_layout()
plt.savefig("../figures/scheduling_density.png")
plt.close()

print("✅ All plots saved to ../figures/")
