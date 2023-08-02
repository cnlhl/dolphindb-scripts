import matplotlib.pyplot as plt
import numpy as np
import time

def generate_time_series(start_time, end_time, interval=3600):
    """
    生成时间序列数据
    :param start_time: 开始时间戳（Unix时间戳，单位：秒）
    :param end_time: 结束时间戳（Unix时间戳，单位：秒）
    :param interval: 时间间隔（默认为1小时，单位：秒）
    :return: 时间戳和对应的随机值列表
    """
    timestamps = np.arange(start_time, end_time, interval)
    values = np.random.rand(len(timestamps)) * 100  # 生成0到100之间的随机值
    return timestamps, values

def plot_time_series(timestamps, values):
    """
    绘制时间序列图
    :param timestamps: 时间戳列表
    :param values: 值列表
    """
    formatted_timestamps = [time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(ts)) for ts in timestamps]
    print(formatted_timestamps)
    plt.figure(figsize=(12, 6))
    plt.plot(formatted_timestamps, values, marker='o', linestyle='-', color='b')
    plt.xlabel('时间戳')
    plt.ylabel('值')
    plt.title('时间序列图')
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.tight_layout()
    plt.savefig("testfig.png")

if __name__ == "__main__":
    start_time = int(time.mktime(time.strptime("2023-07-24 00:00:00", "%Y-%m-%d %H:%M:%S")))
    end_time = int(time.mktime(time.strptime("2023-07-24 12:00:00", "%Y-%m-%d %H:%M:%S")))
    timestamps, values = generate_time_series(start_time, end_time, interval=1800)  # 每30分钟生成一个数据点
    print(timestamps)
    plot_time_series(timestamps, values)
