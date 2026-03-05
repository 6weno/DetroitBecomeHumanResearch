import requests
import csv
import time
import random
import os
import json


target_appid = 1222140  # 目标游戏appid
basic_csv = f"Steam_{target_appid}_基础数据.csv"  # 游戏基础数据
comment_csv = f"Steam_{target_appid}_评论数据.csv"  # 评论原始数据
playtime_csv = f"Steam_{target_appid}_游玩时长数据.csv"  # 游玩时长原始数据
request_delay = [1, 3]  # 每页请求延迟（秒），建议1-3秒
max_empty_page = 3  # 连续空页面数（超过则判定爬完）
save_interval = 5  # 每爬取N页就写入一次临时数据（避免数据丢失）
reviews_per_page = 100  # 每页评论数（API最大100）

# 请求头（模拟浏览器，避免被限制）
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Referer": f"https://store.steampowered.com/app/{target_appid}/",
    "Accept": "application/json, text/javascript, */*; q=0.01"
}


def get_game_basic_info(appid):
    """爬取游戏基础数据（通过官方API）"""
    api_url = f"https://store.steampowered.com/appreviews/{appid}?json=1&num_per_page=0"
    try:
        response = requests.get(api_url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()

        # 提取基础数据
        basic_data = {
            "游戏APPID": appid,
            "总评论数": data.get("query_summary", {}).get("total_reviews", 0),
            "好评数": data.get("query_summary", {}).get("total_positive", 0),
            "差评数": data.get("query_summary", {}).get("total_negative", 0),
            "好评率": round(data.get("query_summary", {}).get("total_positive", 0) / max(
                data.get("query_summary", {}).get("total_reviews", 1), 1) * 100, 2)
        }
        print("===== 爬取游戏基础评论数 =====")
        print(basic_data)
        return basic_data
    except Exception as e:
        print(f"基础数据爬取失败：{e}")
        return None


def extract_playtime(playtime_str):
    """提取游玩时长数值"""
    if not playtime_str:
        return ""
    # 处理 "123.4 hrs on record" 或 "1,234 小时" 等格式
    playtime_str = str(playtime_str).replace(",", "").lower()
    import re
    match = re.search(r"(\d+\.?\d*)", playtime_str)
    return match.group(1) if match else ""


def write_csv(file_path, data_list, headers, mode="w"):
    """通用CSV写入函数（处理编码和空值）"""
    # 确保目录存在
    os.makedirs(os.path.dirname(file_path) or ".", exist_ok=True)

    with open(file_path, mode=mode, encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        if mode == "w":
            writer.writeheader()
        writer.writerows(data_list)
    print(f"✅ 数据已{'覆盖写入' if mode == 'w' else '追加写入'}：{file_path}（本次写入{len(data_list)}条）")


def get_all_page_comment_data(appid):
    """通过Steam官方API爬取所有评论数据"""
    comment_raw_list = []  # 纯评论数据
    playtime_raw_list = []  # 纯游玩时长数据
    page = 1  # 起始页码
    empty_page_count = 0  # 连续空页面计数器
    temp_comment = []
    temp_playtime = []

    print("\n===== 开始自动爬取所有评论页面 =====")
    print(f"📝 配置：每{save_interval}页自动写入一次临时数据\n")

    while empty_page_count < max_empty_page:
        # 构造API请求URL（按最新评论排序）
        api_url = (
            f"https://store.steampowered.com/appreviews/{appid}?"
            f"json=1&num_per_page={reviews_per_page}&page={page}&"
            f"filter=mostrecent&language=all&purchase_type=all"
        )

        try:
            # 发送请求并添加随机延迟
            time.sleep(random.uniform(request_delay[0], request_delay[1]))
            response = requests.get(api_url, headers=headers, timeout=15)
            response.raise_for_status()
            data = response.json()

            # 提取当前页评论列表
            reviews = data.get("reviews", [])
            current_page_count = len(reviews)

            # 判定空页面
            if current_page_count == 0:
                empty_page_count += 1
                print(f"第{page}页：无评论数据（连续空页面{empty_page_count}/{max_empty_page}）")
                page += 1
                continue
            else:
                empty_page_count = 0  # 重置空页面计数器

            # 遍历当前页评论
            for idx, review in enumerate(reviews):
                # 1. 提取核心字段（API返回的结构化数据）
                user_id = review.get("author", {}).get("steamid", f"未知用户_{page}_{idx}")
                comment_data = {
                    "页码": page,
                    "用户ID": user_id,
                    "评论语言": review.get("language", ""),
                    "用户地区": review.get("author", {}).get("location", ""),
                    "推荐状态": "推荐" if review.get("voted_up", False) else "不推荐",
                    "评论内容": review.get("review", "").strip().replace("\n", " "),
                    "评论时间": review.get("timestamp_created", ""),
                    "是否购买": "是" if review.get("received_for_free", False) is False else "否",
                    "帮助数": review.get("votes_up", 0)
                }

                # 清洗字符串字段
                for k, v in comment_data.items():
                    comment_data[k] = v.strip() if isinstance(v, str) else v
                comment_raw_list.append(comment_data)

                temp_comment.append(comment_data)

                # 2. 提取游玩时长数据
                playtime_text = review.get("author", {}).get("playtime_forever", 0)  # 总游玩时长（分钟）
                playtime_hours = str(round(playtime_text / 60, 2)) if playtime_text else ""
                playtime_data = {
                    "用户ID": user_id,
                    "页码": page,
                    "游玩时长（小时）": playtime_hours,
                    "近2周游玩时长（小时）": str(
                        round(review.get("author", {}).get("playtime_last_two_weeks", 0) / 60, 2))
                }
                playtime_raw_list.append(playtime_data)
                temp_playtime.append(playtime_data)

            # 打印进度
            print(
                f"第{page}页：爬取{current_page_count}条评论 → 累计评论{len(comment_raw_list)}条 | 累计时长{len(playtime_raw_list)}条")

            # 按间隔写入临时数据
            if page % save_interval == 0:
                print(f"\n🔄 达到{save_interval}页间隔，开始写入临时数据...")
                # 评论数据
                comment_mode = "w" if page == save_interval else "a"
                write_csv(comment_csv, temp_comment,
                          ["页码", "用户ID", "评论语言", "用户地区", "推荐状态", "评论内容", "评论时间", "是否购买",
                           "帮助数"],
                          mode=comment_mode)

                # 游玩时长数据
                playtime_mode = "w" if page == save_interval else "a"
                write_csv(playtime_csv, temp_playtime,
                          ["用户ID", "页码", "游玩时长（小时）", "近2周游玩时长（小时）"],
                          mode=playtime_mode)

                # 清空临时存储
                temp_comment = []
                temp_playtime = []
                print(f"✅ 第{page - save_interval + 1}~{page}页数据已写入\n")

            # 页码+1
            page += 1

        except Exception as e:
            print(f"第{page}页：爬取失败 → {e}")
            empty_page_count += 1
            page += 1
            continue

    # 写入剩余临时数据
    if temp_comment or temp_playtime:
        print(f"\n🔄 爬取结束，写入剩余临时数据（{len(temp_comment)}条评论 + {len(temp_playtime)}条时长）...")
        write_csv(comment_csv, temp_comment,
                  ["页码", "用户ID", "评论语言", "用户地区", "推荐状态", "评论内容", "评论时间", "是否购买", "帮助数"],
                  mode="a")
        write_csv(playtime_csv, temp_playtime,
                  ["用户ID", "页码", "游玩时长（小时）", "近2周游玩时长（小时）"],
                  mode="a")

    # 爬取结束提示
    total_page = page - 1 - empty_page_count
    print(f"\n===== 爬取结束 =====")
    print(f"- 总爬取页数：{total_page}页")
    print(f"- 总评论数据：{len(comment_raw_list)}条")
    print(f"- 总时长数据：{len(playtime_raw_list)}条")

    return comment_raw_list, playtime_raw_list


if __name__ == "__main__":
    # 第一步：爬取游戏基础数据
    game_basic = get_game_basic_info(target_appid)
    if not game_basic:
        exit("基础数据爬取失败，终止程序")

    # 第二步：自动爬取所有评论页面数据
    comment_raw, playtime_raw = get_all_page_comment_data(target_appid)

    # 第三步：最终完整覆盖写入
    if comment_raw and playtime_raw:
        print("\n===== 开始写入最终完整数据 =====")

        # 1. 写入游戏基础数据CSV
        basic_headers = ["游戏APPID", "总评论数", "好评数", "差评数", "好评率"]
        write_csv(basic_csv, [game_basic], basic_headers)

        # 2. 最终覆盖写入评论原始数据CSV
        comment_headers = ["页码", "用户ID", "评论语言", "用户地区", "推荐状态", "评论内容", "评论时间", "是否购买",
                           "帮助数"]
        write_csv(comment_csv, comment_raw, comment_headers)

        # 3. 最终覆盖写入游玩时长原始数据CSV
        playtime_headers = ["用户ID", "页码", "游玩时长（小时）", "近2周游玩时长（小时）"]
        write_csv(playtime_csv, playtime_raw, playtime_headers)

        print("\n📊 所有原始数据已分别写入3个CSV文件：")
        print(f"- 基础数据：{basic_csv}")
        print(f"- 评论数据：{comment_csv}")
        print(f"- 游玩时长：{playtime_csv}")
    else:
        print("❌ 未爬取到任何评论/游玩时长数据！")