import json

def split_json_by_users(src_path, dst_path, user_cap=300):
    with open(src_path, "r", encoding="utf-8") as f:
        rows = json.load(f)   # rows: list of dict

    # 找出所有用户 ID
    all_users = []
    for r in rows:
        uid = str(r["UserID"])
        if uid not in all_users:
            all_users.append(uid)

    # 取前 user_cap 个用户并过滤数据
    keep_users = set(all_users[:user_cap])
    print(f"总用户数: {len(all_users)}, 保留: {len(keep_users)}")
    new_rows = [r for r in rows if str(r["UserID"]) in keep_users]

    with open(dst_path, "w", encoding="utf-8") as f:
        json.dump(new_rows, f, ensure_ascii=False, indent=2)
    return new_rows

subset = split_json_by_users("data/ecommerce_all.json", "data/ecommerce_300.json", user_cap=300)
