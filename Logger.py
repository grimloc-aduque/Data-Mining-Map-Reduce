
import threading

from FailPolicy import FailPolicy


class Logger:
    def log(msg:str):
        if FailPolicy.master_did_fail:
            return
        thread_id = str(threading.get_native_id())[-2:]
        print(f'Thread {thread_id}: {msg}')