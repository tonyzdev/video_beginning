import pandas as pd
import numpy as np
import gc  # 添加垃圾回收模块

# 定义av号转bv号的函数
def av2bv(av):
    table = 'fZodR9XQDSUm21yCkr6zBqiveYah8bt4xsWpHnJE7jL5VG3guMTKNPAwcF'
    tr = {}
    for i in range(58):
        tr[table[i]] = i
    s = [11, 10, 3, 8, 4, 6]
    xor = 177451812
    add = 8728348608
    
    av = int(av)
    x = (av ^ xor) + add
    r = list('BV1  4 1 7  ')
    for i in range(6):
        r[s[i]] = table[x // 58 ** i % 58]
    return ''.join(r)

# 读取parquet文件
file_path = 'sampled_avid.parquet'
df = pd.read_parquet(file_path)    

# 确保日期字段为 datetime 类型
df['pub_date'] = pd.to_datetime(df['pub_date'])
df['data_date'] = pd.to_datetime(df['data_date'])

# 计算从发布日起的天数
df['day_since_pub'] = (df['data_date'] - df['pub_date']).dt.days

# 保留 day_since_pub 在 0 到 29 天之间的数据（即前30天）
full_days = 30 
df = df[(df['day_since_pub'] >= 0) & (df['day_since_pub'] <  full_days)]
gc.collect()  # 回收内存

# 找出有完整30天数据的 avid
avid_day_counts = df.groupby('avid')['day_since_pub'].nunique()
complete_avids = avid_day_counts[avid_day_counts == full_days].index
clean_df = df[df['avid'].isin(complete_avids)].copy()
# 释放不再需要的变量
del df, avid_day_counts
gc.collect()  # 回收内存

# 筛选 duration >= 120 且 <= 128 的记录
min_duration = 120
max_duration = 128

filtered_df = clean_df[(clean_df['duration'] >= min_duration) & (clean_df['duration'] <= max_duration)].copy()
del clean_df
gc.collect()  # 回收内存

# 再次检查这些 avid 是否依然有完整30天数据
avid_day_counts_filtered = filtered_df.groupby('avid')['day_since_pub'].nunique()
final_avids = avid_day_counts_filtered[avid_day_counts_filtered == 30].index
step_1_df = filtered_df[filtered_df['avid'].isin(final_avids)].copy()
del filtered_df, avid_day_counts_filtered, final_avids
gc.collect()  # 回收内存

del step_1_df['unnamed0']

# 使用av2bv函数创建bvid列
step_1_df['bvid'] = step_1_df['avid'].apply(lambda x: av2bv(x))
print(len(step_1_df))

step_1_df.to_parquet('step_1_df.parquet', index=False)
# 如果不再需要 step_1_df，可以释放
del step_1_df
gc.collect() 