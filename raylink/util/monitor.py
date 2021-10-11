import raylink
import psutil
import schedule
import threading
import time
from tabulate import tabulate
from psutil._common import bytes2human
import subprocess as sp
import tracemalloc


class HWM(object):
    @staticmethod
    def add_cap(cap, log):
        log = '-' * 20 + f'\n{cap}\n' + '-' * 20 + '\n' + log + '\n'
        return log

    @staticmethod
    def cpu():
        log = [['Usage', str(psutil.cpu_percent())],
               ['Load Avg', ', '.join([str(round(i, 2)) for i in psutil.getloadavg()])]]
        log = tabulate(log, tablefmt='presto', colalign=['left', 'right'])
        log = HWM.add_cap('CPU', log)
        return log

    @staticmethod
    def mem():
        mem = psutil.virtual_memory()
        log = []
        for name in mem._fields:
            value = getattr(mem, name)
            if name != 'percent':
                value = bytes2human(value)
            log.append([name.capitalize(), value])
        log = tabulate(log, tablefmt='presto', colalign=['left', 'right'])
        log = HWM.add_cap('Memory', log)
        return log

    @staticmethod
    def disk():
        disk_info = sp.getoutput('df -h')
        lines = disk_info.split('\n')
        headers = list(filter(lambda x: x != '', lines[0].split(' ')))
        data = []
        for l in lines[1:]:
            data.append(list(filter(lambda x: x != '', l.split(' '))))
        log = tabulate(data, headers=headers, tablefmt='presto')
        log = HWM.add_cap('Disk', log)
        return log

    @staticmethod
    def proc():
        log = ''
        attrs = ['pid', 'ppid', 'cpu_percent', 'memory_percent', 'name', 'cmdline', 'memory_info']
        headers = ['pid', 'ppid', 'cpu', 'mem', 'name', 'cmdline']
        mem_fields = []
        keys = None
        data = []
        count = 0
        for proc in psutil.process_iter():
            try:
                if proc.memory_info() is None:
                    continue
            except:
                continue
            d = proc.as_dict(attrs=attrs)
            d['cmdline'] = ' '.join(d['cmdline'])
            d['cmdline'].strip()
            if len(d['cmdline']) > 30:
                d['cmdline'] = d['cmdline'][:30] + '...'
            d['name'].strip()
            if len(d['name']) > 15:
                d['name'] = d['name'][:15] + '...'
            d['cpu'] = round(d.pop('cpu_percent'), 2)
            d['mem'] = round(d.pop('memory_percent'), 2)
            mem = d.pop('memory_info')
            for name in mem._fields:
                if count == 0:
                    mem_fields.append(name)
                value = getattr(mem, name)
                value = bytes2human(value)
                d[name] = value
            if keys is None:
                keys = headers + mem_fields
            data.append(dict(zip(keys, [d[k] for k in keys])))
            count += 1
        cpu_data = sorted(data, key=lambda x: x['cpu'], reverse=True)[:5]
        _log = tabulate([list(d.values()) for d in cpu_data], headers=keys,
                        tablefmt='presto', colalign=['left', 'right'])
        log += HWM.add_cap('CPU TOP 5', _log)
        mem_data = sorted(data, key=lambda x: x['mem'], reverse=True)[:5]
        _log = tabulate([list(d.values()) for d in mem_data], headers=keys, tablefmt='presto')
        log += HWM.add_cap('MEM TOP 5', _log)
        return log


class TM(object):
    def __init__(self):
        self.snapshot = None

    def start(self):
        tracemalloc.start()

    def stat(self):
        log = ''
        snapshot = tracemalloc.take_snapshot()
        top_stats = snapshot.statistics('lineno')
        log += "[ Top 10 ]\n"
        for stat in top_stats[:10]:
            log += f'{stat}\n'
        if self.snapshot:
            top_stats = snapshot.compare_to(self.snapshot, 'lineno')
            log += "[ Top 10 differences ]\n"
            for stat in top_stats[:10]:
                log += f'{stat}\n'
        self.snapshot = snapshot
        return log


class MemSnapshot(object):
    def __init__(self, memory_info, code_info):
        self.memory_info = memory_info
        self.code_info = code_info
        self.attrs = memory_info._fields

    @staticmethod
    def __calc(al_func, c_func):
        def wrapper(a, b):
            values = []
            for _a in a.attrs:
                a_v = getattr(a.memory_info, _a)
                b_v = getattr(b.memory_info, _a)
                res = al_func(a_v, b_v)
                values.append(res)
            return MemSnapshot(type(a.memory_info)(*values), c_func(a.code_info, b.code_info))

        return wrapper

    def __sub__(self, other):
        return self.__calc(lambda a, b: a - b, lambda a, b: f'{b} -> {a}')(self, other)

    def __add__(self, other):
        assert self.code_info == other.code_info
        return self.__calc(lambda a, b: a + b, lambda a, b: f'{a}')(self, other)

    def __str__(self):
        s = []
        for a in self.attrs:
            value = getattr(self.memory_info, a)
            if value < 0:
                value = '-' + bytes2human(abs(value))
            else:
                value = bytes2human(value)
            s.append(f'{a}={value}')
        return ', '.join(s)


class ProcMem(object):
    def __init__(self):
        self.p = psutil.Process()
        self.p.memory_info()
        self.last_snapshot = None
        self.diff_stat = {}

    def snapshot(self, first=False):
        import sys
        frame = sys._getframe(1)
        code_info = f'{frame.f_code.co_filename}:{frame.f_lineno}'
        mem = self.p.as_dict(attrs=['memory_info'])['memory_info']
        snapshot = MemSnapshot(mem, code_info)
        if not first and self.last_snapshot:
            diff = snapshot - self.last_snapshot
            if diff.code_info not in self.diff_stat:
                self.diff_stat[diff.code_info] = diff
            else:
                self.diff_stat[diff.code_info] += diff
        self.last_snapshot = snapshot

    def stat(self):
        stat = []
        for k, v in self.diff_stat.items():
            stat.append(f'{k} {v}')
        return '\n'.join(stat)


class Telescreen(raylink.OutlineNode):
    TYPE = 'telescreen'

    def setup(self):
        schedule.every(1).minutes.do(self.on_tick)

        def run():
            while True:
                schedule.run_pending()
                time.sleep(1)

        job_thread = threading.Thread(target=run, daemon=True)
        job_thread.start()

    def on_tick(self):
        log = [HWM.cpu(), HWM.mem(), HWM.disk(), HWM.proc()]
        self._llogger.debug('\n' + ''.join(log))


class OldBro(raylink.SuperVillain):
    TYPE = 'oldbro'

    def setup(self):
        raylink.SuperVillain.setup(self, Telescreen)
