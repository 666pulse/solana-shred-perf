# %%
import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# 设置中文字体，解决中文乱码问题
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS', 'PingFang SC', 'STHeiti', 'SimHei', 'Microsoft YaHei']
plt.rcParams['axes.unicode_minus'] = False  # 解决负号显示问题

# Read the JSON file
with open("stats.json", "r") as f:
    data = json.load(f)

# Extract endpoint titles
endpoint1_title = data["endpoint1_summary"].get("title", "endpoint1")
endpoint2_title = data["endpoint2_summary"].get("title", "endpoint2")

# Extract slots data
slots = data["slots"]

# Create a list to store the data
rows = []
for slot_data in slots:
    slot = slot_data["slot"]
    endpoint1_delay = slot_data["endpoint1"]["first_shred_delay_ms"]
    endpoint2_delay = slot_data["endpoint2"]["first_shred_delay_ms"]
    rows.append({
        "slot": slot,
        "endpoint1_delay_ms": endpoint1_delay,
        "endpoint2_delay_ms": endpoint2_delay,
    })

# Create DataFrame
df = pd.DataFrame(rows)

# 定义延迟范围（毫秒）- 表示另一个端点落后的时间
delay_ranges = [
    (0, 1, "0-1ms"),
    (1, 5, "1-5ms"),
    (5, 10, "5-10ms"),
    (10, 20, "10-20ms"),
    (20, 30, "20-30ms"),
    (30, 40, "30-40ms"),
    (40, 50, "40-50ms"),
    (50, 100, "50-100ms"),
    (100, float('inf'), ">100ms"),
]

# 统计每个端点领先时，另一个端点落后的延迟分布
# endpoint1 领先时（endpoint1_delay_ms == 0.0），统计 endpoint2_delay_ms 的分布
# endpoint2 领先时（endpoint2_delay_ms == 0.0），统计 endpoint1_delay_ms 的分布

endpoint1_leading_data = df[df["endpoint1_delay_ms"] == 0.0]["endpoint2_delay_ms"]
endpoint2_leading_data = df[df["endpoint2_delay_ms"] == 0.0]["endpoint1_delay_ms"]

endpoint1_leading_counts = []  # endpoint1 领先时，endpoint2 落后的延迟分布
endpoint2_leading_counts = []  # endpoint2 领先时，endpoint1 落后的延迟分布
range_labels = []

for min_val, max_val, label in delay_ranges:
    if max_val == float('inf'):
        count1 = (endpoint1_leading_data >= min_val).sum()
        count2 = (endpoint2_leading_data >= min_val).sum()
    else:
        count1 = ((endpoint1_leading_data >= min_val) & (endpoint1_leading_data < max_val)).sum()
        count2 = ((endpoint2_leading_data >= min_val) & (endpoint2_leading_data < max_val)).sum()

    endpoint1_leading_counts.append(count1)
    endpoint2_leading_counts.append(count2)
    range_labels.append(label)

# 统计领先总次数
endpoint1_leading_total = len(endpoint1_leading_data)
endpoint2_leading_total = len(endpoint2_leading_data)
total = len(df)

# 打印统计结果
print(f"\n领先次数统计：")
print(f"{endpoint1_title} 领先次数：{endpoint1_leading_total} ({endpoint1_leading_total/total*100:.2f}%)")
print(f"{endpoint2_title} 领先次数：{endpoint2_leading_total} ({endpoint2_leading_total/total*100:.2f}%)")
print(f"\n当 {endpoint1_title} 领先时，{endpoint2_title} 落后的延迟分布：")
print(f"{'延迟范围':<15} | {'次数':<10} | {'占比':<10}")
print("-" * 40)
for i, label in enumerate(range_labels):
    count = endpoint1_leading_counts[i]
    percent = (count / endpoint1_leading_total * 100) if endpoint1_leading_total > 0 else 0
    print(f"{label:<15} | {count:<10} | {percent:.2f}%")

print(f"\n当 {endpoint2_title} 领先时，{endpoint1_title} 落后的延迟分布：")
print(f"{'延迟范围':<15} | {'次数':<10} | {'占比':<10}")
print("-" * 40)
for i, label in enumerate(range_labels):
    count = endpoint2_leading_counts[i]
    percent = (count / endpoint2_leading_total * 100) if endpoint2_leading_total > 0 else 0
    print(f"{label:<15} | {count:<10} | {percent:.2f}%")

# 创建图表
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

# 左图：当 endpoint1 领先时，endpoint2 落后的延迟分布
x_pos = np.arange(len(range_labels))
max_count1 = max(endpoint1_leading_counts) if endpoint1_leading_counts else 1
ax1.bar(x_pos, endpoint1_leading_counts, alpha=0.7, color='steelblue', edgecolor='black')
ax1.set_xlabel("延迟范围（另一个端点落后的时间）")
ax1.set_ylabel("次数")
ax1.set_title(f"当 {endpoint1_title} 领先时，{endpoint2_title} 落后的延迟分布\n(总领先次数：{endpoint1_leading_total})")
ax1.set_xticks(x_pos)
ax1.set_xticklabels(range_labels, rotation=45, ha='right')
ax1.grid(True, alpha=0.3, axis='y')

# 在柱状图上添加数值标签
for i, v in enumerate(endpoint1_leading_counts):
    if v > 0:
        ax1.text(i, v + max_count1 * 0.01, str(v), ha='center', va='bottom', fontsize=9)

# 右图：当 endpoint2 领先时，endpoint1 落后的延迟分布
max_count2 = max(endpoint2_leading_counts) if endpoint2_leading_counts else 1
ax2.bar(x_pos, endpoint2_leading_counts, alpha=0.7, color='coral', edgecolor='black')
ax2.set_xlabel("延迟范围（另一个端点落后的时间）")
ax2.set_ylabel("次数")
ax2.set_title(f"当 {endpoint2_title} 领先时，{endpoint1_title} 落后的延迟分布\n(总领先次数：{endpoint2_leading_total})")
ax2.set_xticks(x_pos)
ax2.set_xticklabels(range_labels, rotation=45, ha='right')
ax2.grid(True, alpha=0.3, axis='y')

# 在柱状图上添加数值标签
for i, v in enumerate(endpoint2_leading_counts):
    if v > 0:
        ax2.text(i, v + max_count2 * 0.01, str(v), ha='center', va='bottom', fontsize=9)

plt.tight_layout()
plt.savefig("delay_range_stats.png", dpi=300, bbox_inches='tight')
print(f"\n图表已保存为 'delay_range_stats.png'")
plt.show()

# %%
