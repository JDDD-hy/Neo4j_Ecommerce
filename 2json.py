import pandas as pd
import numpy as np
import json

df = pd.read_csv(
    "data/ecommerce_clickstream_transactions.csv",
    # 把这些都当成缺失
    na_values=["<null>", "NULL", "NaN", "nan", "", " "]
)

# 标准化空白：把全表空串/空格变成 NaN
df = df.replace(r"^\s*$", np.nan, regex=True)
df["Amount"] = pd.to_numeric(df["Amount"], errors="coerce")

# 只对需要为 None 的列，先转 object 再替换 NaN→None
for col in ["Amount", "ProductID", "Outcome"]:
    if col in df.columns:
        df[col] = df[col].astype(object).where(pd.notna(df[col]), None)


df["User_Session"] = df["UserID"].astype(str) + "_" + df["SessionID"].astype(str)
df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce").dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

# 导出 JSON
records = df.to_dict(orient="records")
with open("data/ecommerce_all.json", "w", encoding="utf-8") as f:
    json.dump(records, f, indent=2, ensure_ascii=False)
