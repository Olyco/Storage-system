import psycopg2
import os
import time
import math
from storage_system import StorageSystem, DATA_FOLDER, DEDUPLICATED_FOLDER, DUPLICATED_FOLDER, STORAGE_FOLDER

T = True
F = False

MD5 = "MD5" # output - 16 bytes
SHA1 = "SHA128" # output - 20 bytes
SHA256 = "SHA256" # output - 32 bytes
SHA512 = "SHA512" # output - 64 bytes
SHA224 = "SHA3_224" # output - 28 bytes

ID_SIZE = 3
SEGMENT_SIZE = 15
STORAGE_SIZE= 5000
HASH_FUN = MD5

STORAGE_SIZES = [100, 500, 1000, 2000, 5000, 10000, 15000, 25000]
SEGMENT_SIZES = [4, 10, 25, 50, 100, 250]
HASH_FUNS = [MD5, SHA1, SHA256, SHA512, SHA224]

TEST_NUM = 1
RESULT_FOLDER = "results"

avg_time_compress = {}
avg_time_decompess = {}
reused_hashes = {}
hash_inc_amount = {}
dublicated_amount = {}
decompress_errors = {}


def compare_files(file):
    with open(DATA_FOLDER + "/" + file, "rb") as f1, open(DUPLICATED_FOLDER + "/" + file, "rb") as f2:
        seg1 = f1.read(1000)
        seg2 = f2.read(1000)
        err_count = 0 # in KB
        while seg1 and seg2:
            if seg1 != seg2:
                err_count += 1
            seg1 = f1.read(1000)
            seg2 = f2.read(1000)
        if seg1 or seg2:
            print("COMPARISON ERROR: Files has different lengths")
        print(f"Num of differences: {err_count}")


# read/write dependency on storage files size
def storage_size_write_read_test():
    conn = psycopg2.connect(dbname=os.getenv("DB_NAME"), user=os.getenv("USER"), password=os.getenv("PASSWORD"), host=os.getenv("HOST"), port=os.getenv("PORT"))
    with conn.cursor() as cursor:
        print("Connected to DB")

        sizes = STORAGE_SIZES
        files = os.listdir(DATA_FOLDER)[:3]

        all_runs_write = []
        all_runs_read = []

        for i in range(TEST_NUM):
            single_run_write = []
            single_run_read = []
            print(f"Current test run: {i}")
            for size in sizes:
                size_str_write = []
                size_str_read = []

                print(f"Current storage files size: {size}")
                StorageSystem.free_db(cursor=cursor, connection=conn)
                SS = StorageSystem(id_size=ID_SIZE, seg_size=100, storage_size=size, hash_fun=MD5, cursor=cursor, connection=conn)
                for file in files:
                    timer_start = time.time()
                    deduplicated_name = SS.deduplicate_file(file)
                    timer_end = time.time()
                    size_str_write.append(timer_end - timer_start)

                    timer_start = time.time()
                    SS.duplicate_file(deduplicated_name)
                    timer_end = time.time()
                    size_str_read.append(timer_end - timer_start)

                    compare_files(file)
                single_run_write.append(size_str_write)
                single_run_read.append(size_str_read)

            all_runs_write.append(single_run_write)
            all_runs_read.append(single_run_read)

        print(all_runs_write)
        
        with open(RESULT_FOLDER + "/" + "storage_size_write.txt", "w") as write_res, open(RESULT_FOLDER + "/" + "storage_size_read.txt", "w") as read_res:
            for file in files:
                write_res.write(f" {file}")
                read_res.write(f" {file}")
            write_res.write("\n")
            read_res.write("\n")
            for j, size in enumerate(sizes):                
                for k, file in enumerate(files):
                    avg = sum([all_runs_write[i][j][k] for i in range(TEST_NUM)]) / len(all_runs_write)
                    #print(f"({size}, {file}) average write time = {avg}")
                    write_res.write(f" {avg:.3f}")

                    avg = sum([all_runs_read[i][j][k] for i in range(TEST_NUM)]) / len(all_runs_read)
                    #print(f"({size}, {file}) average read time = {avg}")
                    read_res.write(f" {avg:.3f}")

                write_res.write("\n")
                read_res.write("\n")
    conn.close()


def get_folder_size(start_path = '.'):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            # skip if it is symbolic link
            if not os.path.islink(fp):
                total_size += os.stat(fp).st_size

    return total_size


# read/write dependency on segment size
def seg_size_write_read_test():
    conn = psycopg2.connect(dbname=os.getenv("DB_NAME"), user=os.getenv("USER"), password=os.getenv("PASSWORD"), host=os.getenv("HOST"), port=os.getenv("PORT"))
    with conn.cursor() as cursor:
        print("Connected to DB")

        sizes = SEGMENT_SIZES
        files = os.listdir(DATA_FOLDER)
        files_sizes = []
        for file in files:
            files_sizes.append(os.stat(DATA_FOLDER + "/" + file).st_size)

        all_runs_write = []
        all_runs_read = []
        coefs = []
        percents = []

        for i in range(TEST_NUM):
            single_run_write = []
            single_run_read = []
            print(f"Current test run: {i}")
            
            for size in sizes:
                storage_size = 0
                size_str_write = []
                size_str_read = []
                coef_str = []
                percent_str = []

                print(f"Current segment size: {size}")
                StorageSystem.free_db(cursor=cursor, connection=conn)
                SS = StorageSystem(id_size=ID_SIZE, seg_size=size, storage_size=2000, hash_fun=MD5, cursor=cursor, connection=conn)
                for j, file in enumerate(files):
                    timer_start = time.time()
                    deduplicated_name = SS.deduplicate_file(file)
                    timer_end = time.time()
                    print(f"deduplication time: {timer_end - timer_start}")
                    size_str_write.append(timer_end - timer_start)
                    if i == 0: # only for the first run
                        cur_file_storage = get_folder_size(STORAGE_FOLDER) - storage_size
                        storage_size += cur_file_storage
                        #print(f"size of cur file in storage: {cur_file_storage}")
                        #print(f"storage size with added file: {storage_size}")
                        after = os.stat(DEDUPLICATED_FOLDER + "/" + deduplicated_name).st_size
                        cur_coef = round((after + cur_file_storage) / files_sizes[j], 2)
                        coef_str.append(cur_coef)

                        seg_num_in_file = math.ceil(files_sizes[j] / size)
                        reused_num = int(seg_num_in_file - cur_file_storage / size)
                        reused_p = round(reused_num / seg_num_in_file * 100, 2)
                        #print(f"num of segments in file: {seg_num_in_file}")
                        #print(f"num of reused segments: {reused_num}")
                        print(f"compression coefficient: {cur_coef}")
                        print(f"reused segments percent: {reused_p}")
                        percent_str.append(reused_p)
                    

                    timer_start = time.time()
                    SS.duplicate_file(deduplicated_name)
                    timer_end = time.time()
                    print(f"duplication time: {timer_end - timer_start}")
                    size_str_read.append(timer_end - timer_start)

                    compare_files(file)
                single_run_write.append(size_str_write)
                single_run_read.append(size_str_read)
                if i == 0:
                    coefs.append(coef_str)
                    percents.append(percent_str)

            all_runs_write.append(single_run_write)
            all_runs_read.append(single_run_read)
        
        with open(RESULT_FOLDER + "/" + "seg_size_write.txt", "w") as write_res, \
            open(RESULT_FOLDER + "/" + "seg_size_read.txt", "w") as read_res, \
            open(RESULT_FOLDER + "/" + "coefficient.txt", "w") as coef, \
            open(RESULT_FOLDER + "/" + "percent.txt", "w") as percent:
            for file in files:
                write_res.write(f" {file}")
                read_res.write(f" {file}")
                coef.write(f" {file}")
                percent.write(f" {file}")
            write_res.write("\n")
            read_res.write("\n")
            coef.write("\n")
            percent.write("\n")
            for j, size in enumerate(sizes):                
                for k, file in enumerate(files):
                    avg = sum([all_runs_write[i][j][k] for i in range(TEST_NUM)]) / len(all_runs_write)
                    #print(f"({size}, {file}) average write time = {avg}")
                    write_res.write(f" {avg:.3f}")

                    avg = sum([all_runs_read[i][j][k] for i in range(TEST_NUM)]) / len(all_runs_read)
                    #print(f"({size}, {file}) average read time = {avg}")
                    read_res.write(f" {avg:.3f}")

                    coef.write(f" {coefs[j][k]}")
                    percent.write(f" {percents[j][k]}")

                write_res.write("\n")
                read_res.write("\n")
                coef.write("\n")
                percent.write("\n")
    conn.close()

def hash_fun_write_read_test():
    conn = psycopg2.connect(dbname=os.getenv("DB_NAME"), user=os.getenv("USER"), password=os.getenv("PASSWORD"), host=os.getenv("HOST"), port=os.getenv("PORT"))
    with conn.cursor() as cursor:
        print("Connected to DB")

        funs = HASH_FUNS
        files = os.listdir(DATA_FOLDER)[7:10]

        all_runs_write = []
        all_runs_read = []

        for i in range(TEST_NUM):
            single_run_write = []
            single_run_read = []
            print(f"Current test run: {i}")
            for fun in funs:
                size_str_write = []
                size_str_read = []

                print(f"Current hash function: {fun}")
                StorageSystem.free_db(cursor=cursor, connection=conn)
                SS = StorageSystem(id_size=ID_SIZE, seg_size=25, storage_size=5000, hash_fun=fun, cursor=cursor, connection=conn)
                for file in files:
                    timer_start = time.time()
                    deduplicated_name = SS.deduplicate_file(file)
                    timer_end = time.time()
                    size_str_write.append(timer_end - timer_start)

                    timer_start = time.time()
                    SS.duplicate_file(deduplicated_name)
                    timer_end = time.time()
                    size_str_read.append(timer_end - timer_start)

                    compare_files(file)
                single_run_write.append(size_str_write)
                single_run_read.append(size_str_read)

            all_runs_write.append(single_run_write)
            all_runs_read.append(single_run_read)
        
        with open(RESULT_FOLDER + "/" + "hash_fun_write.txt", "w") as write_res, open(RESULT_FOLDER + "/" + "hash_fun_read.txt", "w") as read_res:
            for file in files:
                write_res.write(f" {file}")
                read_res.write(f" {file}")
            write_res.write("\n")
            read_res.write("\n")
            for j, fun in enumerate(funs):                
                for k, file in enumerate(files):
                    avg = sum([all_runs_write[i][j][k] for i in range(TEST_NUM)]) / len(all_runs_write)
                    #print(f"({size}, {file}) average write time = {avg}")
                    write_res.write(f" {avg:.3f}")

                    avg = sum([all_runs_read[i][j][k] for i in range(TEST_NUM)]) / len(all_runs_read)
                    #print(f"({size}, {file}) average read time = {avg}")
                    read_res.write(f" {avg:.3f}")

                write_res.write("\n")
                read_res.write("\n")
    conn.close()


def main(): # КОНФИГУРАЦИЯ ЗАПУСКА

    storage_size_test = F
    seg_size_test = F
    hash_fun_test = F

    read = T
    write = T
    name = "cat1.jpg"
    deduplicated_name = "cat1.jpg_3_bytes_id_50_bytes_seg_MD5.bin"

    manual = F

    #print(os.listdir(DATA_FOLDER))
    #print(SEGMENT_SIZES[1:2])

    if storage_size_test:
        storage_size_write_read_test()

    if seg_size_test:
        seg_size_write_read_test()

    if hash_fun_test:
        hash_fun_write_read_test()

    if read or write:
        conn = psycopg2.connect(dbname=os.getenv("DB_NAME"), user=os.getenv("USER"), password=os.getenv("PASSWORD"), host=os.getenv("HOST"), port=os.getenv("PORT"))
        with conn.cursor() as cursor:
            if write:
                StorageSystem.free_db(cursor=cursor, connection=conn)
            if manual:
                SS = StorageSystem(id_size=ID_SIZE, seg_size=50, storage_size=STORAGE_SIZE, hash_fun=HASH_FUN, cursor=cursor, connection=conn)
            else:
                SS = StorageSystem(id_size=ID_SIZE, seg_size=SEGMENT_SIZE, storage_size=STORAGE_SIZE, hash_fun=HASH_FUN, cursor=cursor, connection=conn)
            if write:
                deduplicated_name = SS.deduplicate_file(name)
                print(f"{deduplicated_name} stored.\n")
            if read:
                SS.duplicate_file(deduplicated_name)
                compare_files(StorageSystem.get_duplicated_file_name(deduplicated_name))
        conn.close()


if __name__ == '__main__':
    main()