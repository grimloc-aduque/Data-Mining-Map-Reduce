
import os
import shutil


class FileManager:
    files_path = './Files'
    chunks_folder = 'Chunks'
    map_folder = 'Map'
    reduce_folder = 'Reduce'

    input_file = 'input.txt'
    output_file = 'output.txt'

    # Folder Structure

    def build_folder_structure():
        FileManager.create_folder(f'{FileManager.files_path}/{FileManager.chunks_folder}')
        FileManager.create_folder(f'{FileManager.files_path}/{FileManager.map_folder}')
        FileManager.create_folder(f'{FileManager.files_path}/{FileManager.reduce_folder}')

    def create_folder(folder_path:str):
        shutil.rmtree(folder_path, ignore_errors=True)
        os.mkdir(folder_path)

    # Files Paths

    def get_input_path():
        return f'{FileManager.files_path}/{FileManager.input_file}'

    def get_chunk_path(chunk_id:int):
        return f'{FileManager.files_path}/{FileManager.chunks_folder}/chunk_{chunk_id}.txt'

    def get_map_path(map_id:int):
        return f'{FileManager.files_path}/{FileManager.map_folder}/map_{map_id}.txt'

    def get_reduce_path(reduce_id:int):
        return f'{FileManager.files_path}/{FileManager.reduce_folder}/reduce_{reduce_id}.txt'

    def get_output_path():
        return f'{FileManager.files_path}/{FileManager.output_file}'
