
import sys
from threading import Lock, Thread
from time import sleep, time

from BranchCoordinator import BranchCoordinator
from FailPolicy import FailPolicy
from FileManager import FileManager
from Logger import Logger
from Reducer import Reducer


class Coordinator:
    chunk_id_lock:Lock = Lock()

    chunks_ids:list = []
    branch_threads:list[Thread] = []

    # Full Pipeline

    def run():
        try:
            start = time()
            Coordinator.generate_chunks()
            Coordinator.launch_branches()
            Coordinator.wait_branches_termination()
            words_counts = Coordinator.apply_last_reducer()
            words_counts = Coordinator.sort_words_counts(words_counts)
            Coordinator.save_output(words_counts)
            end = time()
            Logger.log(f'Elapsed Time: {round(end - start, 2)}[s]')
        except Exception as e:
            print(e)


    # Chunks

    def generate_chunks():
        input_path = FileManager.get_input_path()
        Logger.log(f'Coordinator - Generating Chunks from {input_path}')
        with open(input_path) as file:
            chunk = ''
            chunk_id = 0
            max_chunck_size = 2 * 10**7
            for line in file:
                if sys.getsizeof(chunk) < max_chunck_size:
                    chunk += '\n' + line
                else:
                    Coordinator.save_chunk(chunk, chunk_id)
                    chunk = line
                    chunk_id += 1
            Coordinator.save_chunk(chunk, chunk_id)

    def save_chunk(chunk:str, chunk_id:int):
        Coordinator.chunks_ids.append(chunk_id)
        chunk_path = FileManager.get_chunk_path(chunk_id)
        chunk_file = open(chunk_path, 'w+')
        chunk_file.write(chunk)
        chunk_file.close()

    def have_chunks():
        return len(Coordinator.chunks_ids) != 0

    def get_next_chunks_ids(num_ids:int):
        Coordinator.chunk_id_lock.acquire()
        chunks_ids = []
        for i in range(num_ids):
            if not Coordinator.have_chunks():
                break
            chunks_ids.append(Coordinator.chunks_ids.pop())
        Coordinator.chunk_id_lock.release()
        return chunks_ids
        

    # Branches

    def launch_branches():
        Logger.log('Coordinator - Launching MapReduce Branches')
        num_branches = 2
        for i in range(num_branches):
            branch_coordinator:BranchCoordinator = BranchCoordinator()
            branch_thread = Thread(target=branch_coordinator.run)
            branch_thread.start()
            Coordinator.branch_threads.append(branch_thread)
        
    def wait_branches_termination():
        if FailPolicy.failure_on_coordinator():
            FailPolicy.master_did_fail = True
            sleep(6)
            raise Exception('Coordinator Node Failed\nMapReduce Aborted')

        for branch_thread in Coordinator.branch_threads:
            branch_thread.join()


    # Reducer

    def apply_last_reducer():
        Logger.log('Coordinator - Launching last Reducer')
        reducer:Reducer = Reducer(is_last_reducer=True)
        reducer.assign_maps(Reducer.all_reduce_ids)
        reducer_thread = Thread(target=reducer.run)
        reducer_thread.start()
        reducer.terminate()
        reducer_thread.join()
        return reducer.words_counts

    def sort_words_counts(words_counts:dict):
        return dict(sorted(words_counts.items(), key=lambda word_count:-word_count[1]))

    # Output

    def save_output(sorted_words_counts:dict):
        output_path = FileManager.get_output_path()
        output_file = open(output_path, 'w+')
        for word, count in sorted_words_counts.items():
            output_file.write(f'{word} {count}\n')
        output_file.close()

        Logger.log(f'Coordinator - Words Counts written to {output_path}')

