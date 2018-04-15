import time
import threading
import json
import matplotlib.pyplot as plt
import psutil
from matplotlib import font_manager
import os


def get_cpu_usage():
    return psutil.cpu_percent(interval=1, percpu=True)


def get_memory():
    return psutil.virtual_memory().percent


POINTS = 60
fig = plt.figure(figsize=(20, 10))
CPU_data = []
Memory_data = [None] * POINTS
NET_data = [[None] * POINTS, [None] * POINTS]
DIS_data = [[None] * POINTS, [None] * POINTS]
if(os.path.exists('file.txt')):
    with open('file.txt') as f:
        data = json.load(f)
    CPU_data=data[0].get('CPU_data')
    Memory_data=data[1].get('Memory_data')
    NET_data=data[2].get('NET_data')
    DIS_data=data[3].get('DIS_data')

##CPU利用率
l_CPUs = []
cpu_count = psutil.cpu_count()


def init_cpu():
    ax_CPU.set_ylim([0, 100])
    ax_CPU.set_xlim([0, POINTS])
    ax_CPU.set_autoscale_on(False)
    ax_CPU.set_title('CPU usage percent %')
    ax_CPU.set_xticks([])
    ax_CPU.set_yticks(range(0, 101, 10))
    ax_CPU.grid(True)


ax_CPU = fig.add_subplot(2, 2, 1)
init_cpu()
for i in range(cpu_count):
    CPU_data.append([None] * POINTS)
    l_CPU, = ax_CPU.plot(range(POINTS), CPU_data[i], label='CPU ' + str(i))
    l_CPUs.append(l_CPU)
ax_CPU.legend(loc='upper center',
              ncol=int(cpu_count / 2), prop=font_manager.FontProperties(size=10))
bg = fig.canvas.copy_from_bbox(ax_CPU.bbox)

##内存利用率
ax_MEM = fig.add_subplot(2, 2, 2)



def init_memory():
    ax_MEM.set_ylim([0, 100])
    ax_MEM.set_xlim([0, POINTS])
    ax_MEM.set_autoscale_on(False)
    ax_MEM.set_title('Memory usage percent')
    ax_MEM.set_xticks([])
    ax_MEM.set_yticks(range(0, 101, 10))
    ax_MEM.grid(True)


init_memory()
l_MEM, = ax_MEM.plot(range(POINTS), Memory_data, label='Memory%')
ax_MEM.legend(loc='upper center', prop=font_manager.FontProperties(size=10))

bg = fig.canvas.copy_from_bbox(ax_MEM.bbox)

##网络情况
up, down = 0, 0


def calc_ul_dl(dt=1):
    global up, down
    t0 = time.time()
    counter = psutil.net_io_counters()
    tot = (counter.bytes_sent, counter.bytes_recv)

    while True:
        last_tot = tot
        time.sleep(dt)
        counter = psutil.net_io_counters()
        t1 = time.time()
        tot = (counter.bytes_sent, counter.bytes_recv)
        ul, dl = [(now - last) / (t1 - t0) / 1000.0
                  for now, last in zip(tot, last_tot)]
        up = ul
        down = dl
        t0 = time.time()


t1 = threading.Thread(target=calc_ul_dl)

# The program will exit if there are only daemonic threads left.
t1.daemon = True
t1.start()

ax_NET = fig.add_subplot(2, 2, 3)


def init_NET():
    ax_NET.set_ylim([0, 2000])
    ax_NET.set_xlim([0, POINTS])
    ax_NET.set_autoscale_on(False)
    ax_NET.set_title('network speed')
    ax_NET.set_xticks([])
    ax_NET.set_yticks(range(0, 2001, 100))
    ax_NET.grid(True)


init_NET()

l_Net_up, = ax_NET.plot(range(POINTS), NET_data[0], label='NET_up kb/s')
l_Net_down, = ax_NET.plot(range(POINTS), NET_data[1], label='NET_down kb/s')
ax_NET.legend(loc='upper center', ncol=2, prop=font_manager.FontProperties(size=10))
bg = fig.canvas.copy_from_bbox(ax_NET.bbox)

##磁盘情况

read, write = 0, 0


def cal_dis_r_w(dt=1):
    global read, write
    t0 = time.time()
    counter = psutil.disk_io_counters()
    tot = (counter.read_bytes, counter.write_bytes)
    while True:
        last_tot = tot
        time.sleep(dt)
        counter = psutil.disk_io_counters()
        t1 = time.time()
        tot = (counter.read_bytes, counter.write_bytes)
        r, w = [(now - last) / (t1 - t0) / 1000.0
                  for now, last in zip(tot, last_tot)]
        read = r
        write=w
        t0 = time.time()

t2 = threading.Thread(target=cal_dis_r_w)

# The program will exit if there are only daemonic threads left.
t2.daemon = True
t2.start()
ax_DIS = fig.add_subplot(2, 2,4)
def init_DIS():
    ax_DIS.set_ylim([0, 5000])
    ax_DIS.set_xlim([0, POINTS])
    ax_DIS.set_autoscale_on(False)
    ax_DIS.set_title('disk_read_write')
    ax_DIS.set_xticks([])
    ax_DIS.set_yticks(range(0, 10000, 1000))
    ax_DIS.grid(True)

init_DIS()

l_Dis_r, = ax_DIS.plot(range(POINTS), DIS_data[0], label='Dis_read kb/s')
l_Dis_w, = ax_DIS.plot(range(POINTS), DIS_data[1], label='Dis_write kb/s')
ax_DIS.legend(loc='upper center', ncol=2, prop=font_manager.FontProperties(size=10))
bg = fig.canvas.copy_from_bbox(ax_DIS.bbox)


def cal_record():
    while(True):
        time.sleep(2)
        lis=[{'CPU_data':CPU_data},{'Memory_data':Memory_data},{'NET_data':NET_data},{'DIS_data':DIS_data}]
        data = json.dumps(lis, ensure_ascii=False)
        file_name = 'file.txt'
        with open(file_name,'w')as f:
            f.write(data)

t3 = threading.Thread(target=cal_record)

# The program will exit if there are only daemonic threads left.
t3.daemon = True
t3.start()


def OnTimer():
    global l_CPUs, CPU_data, cpu_count, l_MEM, Memory_data, l_Net_up, l_Net_down, NET_data, up, down,DIS_data,l_Dis_r,l_Dis_w,read,write
    tmp = get_cpu_usage()
    for i in range(cpu_count):
        CPU_data[i] = CPU_data[i][1:] + [tmp[i]]
        l_CPUs[i].set_ydata(CPU_data[i])
        ax_CPU.draw_artist(l_CPUs[i])
    ax_CPU.figure.canvas.draw()

    tmp = get_memory()
    Memory_data = Memory_data[1:] + [tmp]
    l_MEM.set_ydata(Memory_data)
    ax_MEM.draw_artist(l_MEM)
    ax_MEM.figure.canvas.draw()

    NET_data[0] = NET_data[0][1:] + [up]
    NET_data[1] = NET_data[1][1:] + [down]
    l_Net_up.set_ydata(NET_data[0])
    ax_NET.draw_artist(l_Net_up)
    l_Net_down.set_ydata(NET_data[1])
    ax_NET.draw_artist(l_Net_down)
    ax_NET.figure.canvas.draw()

    DIS_data[0]=DIS_data[0][1:]+[read]
    DIS_data[1]=DIS_data[1][1:]+[write]
    l_Dis_r.set_ydata(DIS_data[0])
    ax_DIS.draw_artist(l_Dis_r)
    l_Dis_w.set_ydata(DIS_data[1])
    ax_DIS.draw_artist(l_Dis_w)
    ax_DIS.figure.canvas.draw()


timer = fig.canvas.new_timer(interval=100)
timer.add_callback(OnTimer)
timer.start()
plt.show()


