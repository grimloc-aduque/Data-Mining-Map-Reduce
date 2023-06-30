
from threading import Thread

from Logger import Logger
from Mapper import Mapper
from Reducer import Reducer


class BranchCoordinator:
    num_mappers = 3
    mappers:list[Mapper]
    maps_threads:list[Thread]
    maps_ids:list[int]
    reducer:Reducer

    def __init__(self):
        self.mappers = []
        self.maps_ids = []
        self.maps_threads = []
        self.init_mappers()
        self.init_reducer()
    
    # Thread Initialization

    def init_mappers(self):
        for i in range(self.num_mappers):
            mapper = Mapper()
            self.mappers.append(mapper)
            map_thread = Thread(target=mapper.run, args=())
            map_thread.start()
            self.maps_threads.append(map_thread)

    def init_reducer(self):
        self.reducer = Reducer()
        self.reducer_thread = Thread(target=self.reducer.run, args=())
        self.reducer_thread.start()

    # Branch Pipeline

    def run(self):
        Logger.log('BranchCoordinator - MapReduce Branch Started')
        from Coordinator import Coordinator

        pending_reduce_tasks = 0

        # Mappers and Reducers working 

        while Coordinator.have_chunks():
            if self.all_mappers_wating():
                self.assign_chunks_to_mappers()
                pending_reduce_tasks += 1

            self.reassign_failed_mappers()
            
            if pending_reduce_tasks>1:
                if self.reducer.waiting_for_maps():
                    self.assign_task_to_reducer()
                    pending_reduce_tasks -= 1

            self.restart_failed_reducer()
            

        while not self.all_mappers_wating():
            self.reassign_failed_mappers()

        self.signal_mappers_termination()

        # Only Reducer working

        while pending_reduce_tasks > 0:
            if self.reducer.waiting_for_maps():
                self.assign_task_to_reducer()
                pending_reduce_tasks -= 1
            self.restart_failed_reducer()

        while self.reducer.failure_detected():
            self.restart_failed_reducer()

        self.signal_reducer_termination()
        

    # Mappers

    def all_mappers_wating(self):
        for mapper in self.mappers:
            if not mapper.waiting_for_chunk():
                return False
        return True

    def assign_chunks_to_mappers(self):
        from Coordinator import Coordinator
        chunks_ids = Coordinator.get_next_chunks_ids(self.num_mappers)
        Logger.log(f'BranchCoordinator - Assigning Chunks: {chunks_ids}')
        for i in range(len(chunks_ids)):
            mapper = self.mappers[i]
            mapper.assign_chunk(chunks_ids[i])
        self.maps_ids += chunks_ids

    def reassign_failed_mappers(self):
        for mapper in self.mappers:
            if mapper.failure_detected():
                Logger.log(f'BranchCoordinator - Reassigning Mapper for Chunk {mapper.chunk_id}')
                mapper.restart_node()
                mapper.assign_chunk(mapper.chunk_id)

    def signal_mappers_termination(self):
        for mapper in self.mappers:
            mapper.terminate()
        for map_thread in self.maps_threads:
            map_thread.join()

    # Reducer

    def assign_task_to_reducer(self):
        maps_ids = []
        for i in range(self.num_mappers):
            if len(self.maps_ids) != 0:
                maps_ids.append(self.maps_ids.pop(0))
        self.reducer.assign_maps(maps_ids)

    def restart_failed_reducer(self):
        if self.reducer.failure_detected():
            Logger.log(f'BranchCoordinator - Restarting Reducer for Maps {self.reducer.maps_ids}')
            self.reducer.restart_node()
            self.reducer.assign_maps(self.reducer.maps_ids)

    def signal_reducer_termination(self):
        self.reducer.terminate()
        self.reducer_thread.join()
