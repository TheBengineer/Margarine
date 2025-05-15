import os
import re
import pathlib
import shutil
from datetime import datetime

root_folder = os.environ.get("MARGARINE_ROOT")
print(root_folder)


def merge_and_delete(main_folder, duplicate_folder):
    for top, folders, files_name in os.walk(duplicate_folder):
        for file in files_name:
            file_path = os.path.join(top, file)
            main_file_path = os.path.join(main_folder, file)
            main_file_size = os.path.getsize(main_file_path)
            duplicate_file_size = os.path.getsize(file_path)
            main_file_mtime = os.path.getmtime(main_file_path)
            duplicate_file_mtime = os.path.getmtime(file_path)
            main_file_ctime = os.path.getctime(main_file_path)
            duplicate_file_ctime = os.path.getctime(file_path)
            if duplicate_file_size != main_file_size:
                print(f"File size mismatch: {file_path}")
                continue
            if duplicate_file_mtime != main_file_mtime:
                print(f"File modified time mismatch: {file_path}\n{datetime.fromtimestamp(duplicate_file_mtime)} vs {datetime.fromtimestamp(main_file_mtime)}")
                if duplicate_file_mtime < main_file_mtime:
                    os.remove(file_path)
                    continue
                else:
                    shutil.move(file_path, main_file_path)
            if duplicate_file_ctime != main_file_ctime:
                print(f"File created time mismatch: {file_path}\n{datetime.fromtimestamp(duplicate_file_ctime)} vs {datetime.fromtimestamp(main_file_ctime)}")
                continue
            print(f"Copying {file_path} to {main_file_path}")

            # Copy maintaining timestamps
            # shutil.copy2(file_path, main_folder)
            # # Delete duplicate file
            # os.remove(file_path)


def main():
    folder_lookup = {}
    scanned_folders = 0
    for top, folders, files_name in os.walk(root_folder):
        for folder in folders:
            folder_name = os.path.join(top, folder)
            # remove '(1)' from folder name if present
            folder_name_universal = re.sub(r" \(\d+\)", "", folder_name)
            if folder_name_universal not in folder_lookup:
                folder_lookup[folder_name_universal] = []
            folder_lookup[folder_name_universal] += [folder_name]
            if scanned_folders % 1000 == 0:
                print(f"Scanned {scanned_folders} folders: {folder_name}")
            scanned_folders += 1
        for file in files_name:
            pass
    for folder_name, folder_list in folder_lookup.items():
        if len(folder_list) > 1:
            print(f"Duplicate folder name: {folder_name}")
            for folder in folder_list:
                print(f"    {folder}")
            duplicate_folders = [folder for folder in folder_list if folder != folder_name]
            for duplicate_folder in duplicate_folders:
                merge_and_delete(folder_name, duplicate_folder)


if __name__ == "__main__":
    main()
