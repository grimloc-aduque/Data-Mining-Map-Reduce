
from Coordinator import Coordinator
from FileManager import FileManager

if __name__ == '__main__':
    FileManager.build_folder_structure()
    Coordinator.run()
