
from time import sleep

from FailPolicy import FailPolicy
from FileManager import FileManager
from Logger import Logger


class Reducer:

    all_reduce_ids = []

    maps_ids:list
    words_counts:dict
    reduce_id:str

    has_failed:bool
    terminate_signal:bool

    def __init__(self, is_last_reducer=False):
        self.is_last_reducer = is_last_reducer
        self.maps_ids = None
        self.words_counts = {}
        self.reduce_id = None
        self.has_failed = False
        self.terminate_signal = False

    def run(self):
        while True:
            if FailPolicy.master_did_fail:
                break
            if self.asked_to_terminate() and self.maps_ids == None:
                break
            if self.waiting_for_maps():
                continue

            Logger.log(f'Reducer - Reducing Maps {self.maps_ids}')

            # Fail Policy
            if FailPolicy.failure_on_reduce() and not self.is_last_reducer:
                Logger.log(f'Reducer - Failure while Reducing Maps {self.maps_ids}')
                self.has_failed = True
                while self.failure_detected():
                    sleep(0.5)
                continue
            
            # Word Reducing
            self.words_counts = {}
            for map_id in self.maps_ids:
                self.concatenate_counts(map_id)
            self.accumulate_counts()
            self.save_result()
            self.maps_ids = None

    # State/Signals

    def asked_to_terminate(self):
        return self.terminate_signal == True

    def waiting_for_maps(self):
        return self.maps_ids == None

    def terminate(self):
        self.terminate_signal = True


    # Maps

    def assign_maps(self, maps_ids:list):
        if maps_ids != []:
            self.maps_ids = maps_ids

    def concatenate_counts(self, map_id:str):
        if self.is_last_reducer:
            map_path = FileManager.get_reduce_path(map_id)
        else:
            map_path = FileManager.get_map_path(map_id)
        map_file = open(map_path)
        for line in map_file:
            word, count = line.split()
            count = int(count)
            if word not in self.words_counts:
                self.words_counts[word] = [count]
            else:
                self.words_counts[word].append(count)

    def accumulate_counts(self):
        for word, counts in self.words_counts.items():
            self.words_counts[word] = sum(counts)

    # Result

    def save_result(self):
        words_counts_str = [f'{word} {count}' for word, count in self.words_counts.items()]
        words_counts_str = '\n'.join(words_counts_str)
        self.reduce_id = '-'.join([str(map_id) for map_id in self.maps_ids])
        reduce_path = FileManager.get_reduce_path(self.reduce_id)
        reduce_file = open(reduce_path, 'w+')
        reduce_file.write(words_counts_str)
        reduce_file.close()
        Reducer.all_reduce_ids.append(self.reduce_id)

    # Failures

    def failure_detected(self):
        return self.has_failed == True

    def restart_node(self):
        self.has_failed = False

