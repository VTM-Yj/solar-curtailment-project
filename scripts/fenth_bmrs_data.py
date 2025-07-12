import requests
import pandas as pd
from datetime import datetime, timedelta

# 参数设置
bm_unit = "T_LARKS-1"  # Larks Green Solar Farm 的 BM Unit ID
start_date = datetime(2023, 6, 1)
end_date = datetime(2023, 6, 8)  # 注意：结束日期是非包含的

# 将 settlementDate + settlementPeriod 转换为 datetime
def convert_to_datetime(date_str, period):
    date = datetime.strptime(date_str, "%Y-%m-%d")
    time = date + timedelta(minutes=(int(period) - 1) * 30)
    return time

# 获取 BMRS 数据的函数
def fetch_bmrs_data(dataset, bm_unit, start, end):
    url = f"https://data.elexon.co.uk/bmrs/api/v1/datasets/{dataset}/stream"
    params = {
        "from": start.isoformat() + "Z",
        "to": end.isoformat() + "Z",
        "bmUnit": [bm_unit]
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()

# 主执行部分
def main():
    print("📡 正在下载 Larks Green Solar Farm 的 BMRS 数据...")
    pn_data = fetch_bmrs_data("PN", bm_unit, start_date, end_date)
    actual_data = fetch_bmrs_data("B1610", bm_unit, start_date, end_date)

    if not pn_data or not actual_data:
        print("⚠️ 下载失败：返回的数据为空")
        return

    # 转换为 DataFrame
    pn_df = pd.DataFrame(pn_data)
    actual_df = pd.DataFrame(actual_data)

    # ✅ 打印字段信息用于调试
    print("\n📋 预测数据字段名：", list(pn_df.columns))
    print("🔍 预测数据样本：", pn_df.head(2).to_dict(orient="records"))
    print("\n📋 实际数据字段名：", list(actual_df.columns))
    print("🔍 实际数据样本：", actual_df.head(2).to_dict(orient="records"))

    # 计算 datetime 字段
    pn_df["datetime"] = pn_df.apply(
        lambda row: convert_to_datetime(row["settlementDate"], row["settlementPeriod"]), axis=1
    )
    actual_df["datetime"] = actual_df.apply(
        lambda row: convert_to_datetime(row["settlementDate"], row["settlementPeriod"]), axis=1
    )

    # ✅ 使用正确字段提取 MW 数据
    pn_df["forecast_MW"] = pd.to_numeric(pn_df["levelFrom"], errors="coerce")
    actual_df["actual_MW"] = pd.to_numeric(actual_df["quantity"], errors="coerce")

    # 合并并计算削减量
    merged_df = pd.merge(
        pn_df[["datetime", "forecast_MW"]],
        actual_df[["datetime", "actual_MW"]],
        on="datetime", how="outer"
    ).sort_values("datetime")

    merged_df["curtailment_MW"] = merged_df["forecast_MW"] - merged_df["actual_MW"]

    # 保存为 CSV 文件
    output_path = "../data/larks_green_curtailment_2023_06_01_to_06_07.csv"
    merged_df.to_csv(output_path, index=False)
    print(f"✅ 数据已保存到：{output_path}")

if __name__ == "__main__":
    main()
