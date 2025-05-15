import os
import re
import pathlib
import subprocess
import shutil
from datetime import datetime

root_folder = os.environ.get("MARGARINE_ROOT")
print(root_folder)


def delete_empty_folders(root):
    deleted = set()

    for current_dir, subdirs, files in os.walk(root, topdown=False):

        still_has_subdirs = False
        for subdir in subdirs:
            if os.path.join(current_dir, subdir) not in deleted:
                still_has_subdirs = True
                break

        if not any(files) and not still_has_subdirs:
            os.rmdir(current_dir)
            deleted.add(current_dir)

    return deleted


def merge_and_delete(main_folder, duplicate_folder):
    error = 0
    errored_files = []
    for top, folders, files_name in os.walk(duplicate_folder):
        for file in files_name:
            dup_file_path = os.path.join(top, file)
            dup_folder_path = os.path.dirname(dup_file_path)
            relative_folder_name = os.path.relpath(dup_folder_path, duplicate_folder)
            if not relative_folder_name == ".":
                main_folder_path = os.path.join(main_folder, relative_folder_name) + "\\"
            else:
                main_folder_path = main_folder
            main_file_path = os.path.join(main_folder_path, file)
            if not os.path.exists(main_file_path):
                print(f"[O] Copying {dup_file_path} to {main_file_path}")
                if not os.path.exists(main_folder_path):
                    os.makedirs(os.path.dirname(main_folder_path), exist_ok=True)
                shutil.move(dup_file_path, main_file_path)
                continue
            main_file_size = os.path.getsize(main_file_path)
            dup_file_size = os.path.getsize(dup_file_path)
            main_file_mtime = os.path.getmtime(main_file_path)
            dup_file_mtime = os.path.getmtime(dup_file_path)
            main_file_ctime = os.path.getctime(main_file_path)
            dup_file_ctime = os.path.getctime(dup_file_path)
            if dup_file_size != main_file_size:
                subprocess.Popen(rf'explorer /select,"{main_file_path}"')
                subprocess.Popen(rf'explorer /select,"{dup_file_path}"')
                print(f"File size mismatch: {dup_file_path}")
                error += 1
                errored_files.append(dup_file_path)
                continue
            print(f"File timestamp mismatch: {dup_file_path}\n"
                  f"{datetime.fromtimestamp(dup_file_mtime)} vs {datetime.fromtimestamp(main_file_mtime)}\n"
                  f"{datetime.fromtimestamp(dup_file_ctime)} vs {datetime.fromtimestamp(main_file_ctime)}")
            if dup_file_ctime < main_file_ctime or dup_file_mtime < main_file_mtime:
                print(f"[N] Copying {dup_file_path} to {main_file_path}")
                os.remove(dup_file_path)
                continue
            else:
                print(f"[O] Copying {dup_file_path} to {main_file_path}")
                if not os.path.exists(dup_folder_path):
                    os.makedirs(os.path.dirname(dup_folder_path), exist_ok=True)
                shutil.move(dup_file_path, main_file_path)
    if not error:
        delete_empty_folders(duplicate_folder)
        print(
            f"Deleted duplicate folder {duplicate_folder} and merged files into {main_folder}"
        )
    else:
        print(f"Error merging files in {duplicate_folder}")
        for file in errored_files:
            print(f"    {file}")


def main():
    folder_lookup = {}
    scanned_folders = 0
    to_scan = str(root_folder).replace("'","")
    print(f"Scanning {to_scan}")
    for top, folders, files_name in os.walk(to_scan):
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
    print(f"Scanned {scanned_folders} folders")
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
