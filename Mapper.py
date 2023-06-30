
from time import sleep

from FailPolicy import FailPolicy
from FileManager import FileManager
from Logger import Logger


class Mapper:

    has_failed:bool
    is_waiting:bool
    terminate_signal:bool
    chunk_id:str

    def __init__(self):
        self.has_failed = False
        self.is_waiting = True
        self.terminate_signal = False
        self.chunk_id = None

    def run(self):
        while True:
            if FailPolicy.master_did_fail:
                break
            if self.asked_to_terminate() and self.is_waiting:
                break
            if self.waiting_for_chunk():
                continue

            Logger.log(f'Mapper - Mapping Chunk {self.chunk_id}')

            # Fail Policy
            if FailPolicy.failure_on_map():
                Logger.log(f'Mapper - Failure while Mapping Chunk {self.chunk_id}')
                self.has_failed = True
                while self.failure_detected():
                    sleep(0.5)
                continue

            # Word Mapping
            word_maping = self.process_chunk()
            self.save_result(word_maping)
            self.is_waiting = True


    # States/Signals

    def asked_to_terminate(self):
        return self.terminate_signal == True

    def waiting_for_chunk(self):
        return self.is_waiting

    def terminate(self):
        self.terminate_signal = True

    # Chunk

    def assign_chunk(self, chunk_id:str):
        self.chunk_id = chunk_id
        self.is_waiting = False

    def process_chunk(self):
        chunk_path = FileManager.get_chunk_path(self.chunk_id)
        file = open(chunk_path)
        word_maping:list = []
        for line in file:
            words = line.split()
            for word in words:
                word_maping.append(f'{word} 1')
        return word_maping

    # Results

    def save_result(self, word_list:list):
        word_list_str = '\n'.join(word_list)
        map_path = FileManager.get_map_path(self.chunk_id)
        map_file = open(map_path, 'w+')
        map_file.write(word_list_str)
        map_file.close()

    # Failures

    def failure_detected(self):
        return self.has_failed == True

    def restart_node(self):
        self.has_failed = False
