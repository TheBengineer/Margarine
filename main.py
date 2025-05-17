import json
import os
import re
import subprocess
from multiprocessing import Pool, cpu_count
import shutil
from datetime import datetime
import hashlib

root_folder = os.environ.get("MARGARINE_ROOT")


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


def load_comparison_matrix():
    comparison_matrix = {}
    if os.path.exists("comparison_matrix.json"):
        with open("comparison_matrix.json", "r") as f:
            comparison_matrix = json.load(f)
    return comparison_matrix


def scan_file(file_path):
    if not os.path.isfile(file_path):
        return f"File does not exist: {file_path}"
    time_create = os.path.getctime(file_path)
    time_modify = os.path.getmtime(file_path)
    file_size = os.path.getsize(file_path)
    with open(file_path, "rb") as f:
        try:
            fast_hash = hashlib.file_digest(f, "md5").hexdigest()
        except Exception as e:
            return f"Error reading {file_path}"
    return [fast_hash, file_path, file_size, time_create, time_modify]


def build_comparison_matrix(folder_to_scan):
    comparison_matrix = load_comparison_matrix()
    expected_files = set()
    for key in comparison_matrix:
        for file_path, file_size, time_create, time_modify in comparison_matrix[key]:
            expected_files.add(file_path)
    scanned_files = len(comparison_matrix)
    checked_files = 0
    scanned_bytes = 0
    last_checked_files = 0
    last_scanned_bytes = 0
    files_to_ignore = set()
    for key in comparison_matrix:
        for file_path, file_size, time_create, time_modify in comparison_matrix[key]:
            scanned_bytes += file_size
            files_to_ignore.add(file_path)
    print(f"Loaded {len(expected_files)}, {scanned_bytes / 1_000_000_000.0:.3f}GB "
          f"entries from exising comparison matrix")
    num_cores = cpu_count()
    pool = Pool(num_cores)
    print(f"Started {num_cores} worker processes")
    for top, folders, files in os.walk(folder_to_scan):
        filenames = []
        for file in files:
            filename = os.path.join(top, file)
            filenames += [filename]
            if filename in expected_files:
                expected_files.remove(filename)
        checked_files += len(filenames)
        files_to_scan = [file for file in filenames if file not in files_to_ignore]
        results = pool.map(scan_file, files_to_scan)
        filtered_results = []
        for result in results:
            if isinstance(result, list):
                filtered_results += [result]
            else:
                print(f"Error: {result}")
        for fast_hash, file_path, file_size, time_create, time_modify in filtered_results:
            if fast_hash not in comparison_matrix:
                comparison_matrix[fast_hash] = []
            comparison_matrix[fast_hash] += [[file_path, file_size, time_create, time_modify]]
            scanned_files += 1
            scanned_bytes += file_size
        if checked_files - last_checked_files > 1000:
            print(f"Scanned {checked_files},{scanned_files} files, {scanned_bytes / 1_000_000_000.0:.3f}GB")
            last_checked_files = checked_files
        if scanned_bytes - last_scanned_bytes > 10_000_000_000:
            with open("comparison_matrix.json", "w") as f2:
                json.dump(comparison_matrix, f2, indent=1)
            print(f"Saved comparison matrix to comparison_matrix.json")
            last_scanned_bytes = scanned_bytes
    to_delete = []
    for fast_hash in comparison_matrix:
        comparison_matrix[fast_hash] = [file_data for file_data in comparison_matrix[fast_hash] if
                                        file_data[0] not in expected_files]
        if not comparison_matrix[fast_hash]:
            to_delete += [fast_hash]
    for fast_hash in to_delete:
        del comparison_matrix[fast_hash]

    with open("comparison_matrix.json", "w") as f2:
        json.dump(comparison_matrix, f2, indent=1)
    return comparison_matrix


def load_ignore_list():
    ignore_list = set()
    if os.path.exists("ignore_list.txt"):
        with open("ignore_list.txt", "r") as f:
            for line in f:
                ignore_list.add(line.strip())
    return ignore_list


def load_ignore_macros():
    ignore_list = set()
    if os.path.exists("ignore_macros.txt"):
        with open("ignore_macros.txt", "r") as f:
            for line in f:
                ignore_list.add(line.strip())
    return ignore_list


def load_auto_delete():
    ignore_list = set()
    if os.path.exists("auto_delete.txt"):
        with open("auto_delete.txt", "r") as f:
            for line in f:
                ignore_list.add(line.strip())
    return ignore_list


def write_ignore_list(ignore_list):
    with open("ignore_list.txt", "w") as f:
        for line in ignore_list:
            f.write(line + "\n")


def cleanup_duplicate_files(comparison_matrix):
    ignore_list = load_ignore_list()
    ignore_macros = load_ignore_macros()
    auto_delete = load_auto_delete()
    duplicate_files = []
    filenames = {}
    for fast_hash, file_list in comparison_matrix.items():
        if len(file_list) > 1:
            for file_path, file_size, time_create, time_modify in file_list:
                duplicate_files += [[fast_hash, file_path, file_size, time_create, time_modify]]
        for file_path, file_size, time_create, time_modify in file_list:
            filename = os.path.basename(file_path)
            if filename not in filenames:
                filenames[filename] = 0
            filenames[filename] += 1
    duplicate_filenames = sorted(list(filenames.items()), key=lambda x: x[1], reverse=True)
    print(f"Found {len(duplicate_files)} duplicate files")
    duplicate_files_grouped = {}
    for short_hash, file_path, file_size, time_create, time_modify in duplicate_files:
        if short_hash not in duplicate_files_grouped:
            duplicate_files_grouped[short_hash] = []
        duplicate_files_grouped[short_hash] += [[file_path, file_size, time_create, time_modify]]
    for short_hash in duplicate_files_grouped:
        files = duplicate_files_grouped[short_hash]
        for file in files:
            for macro in auto_delete:
                if macro in file[0]:
                    for file2 in files:
                        print(file2)
                    print(f"Removing {file[0]} because of rule: '{macro}'")
                    os.remove(file[0])
                    duplicate_files_grouped[short_hash].remove(file)
            for ignore_macro in ignore_macros:
                if ignore_macro in file[0]:
                    print(f"Ignoring {file[0]}")
                    ignore_list.add(file[0])
                    duplicate_files_grouped[short_hash].remove(file)

    write_ignore_list(ignore_list)

    for short_hash in duplicate_files_grouped:
        files = duplicate_files_grouped[short_hash]
        if len(files) == 1:
            continue
        print(f"Opening {len(files)}")
        for file in files:
            print(f"Opening {file[0]}")
            os.popen(rf'explorer /select,"{file[0]}"')
        print("Adding remaining files to ignore list")
        for file in files:
            ignore_list.add(file[0])
        write_ignore_list(ignore_list)


def main():
    folder_lookup = {}
    scanned_folders = 0
    to_scan = str(root_folder).replace("'", "")
    print(f"Scanning {to_scan}")
    comparison_matrix = build_comparison_matrix(to_scan)
    cleanup_duplicate_files(comparison_matrix)
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
