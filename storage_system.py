import hashlib
import os
import time
import datetime

NONE = "none"
MD5 = "MD5" # output - 16 bytes
SHA1 = "SHA128" # output - 20 bytes
SHA256 = "SHA256" # output - 32 bytes
SHA512 = "SHA512" # output - 64 bytes
SHA224 = "SHA3_224" # output - 28 bytes

ID_SIZE = 3
SEGMENT_SIZE = 10
STORAGE_SIZE= 100

DATA_FOLDER = "data"
DEDUPLICATED_FOLDER = "deduplicated"
DUPLICATED_FOLDER = "duplicated"
STORAGE_FOLDER = "storage"

class StorageSystem:
    def __init__(self, id_size, seg_size, storage_size, hash_fun, cursor, connection):
        self.cur_storage_file = ''
        self.cur_storage_pos = 0
        self.id_size = id_size
        self.seg_size = seg_size
        self.storage_size = storage_size
        self.hash_fun = hash_fun
        
        self.cursor = cursor
        self.conn = connection

        self.find_latest_storage_file()
        self.create_table()
    
    def generate_storage_file(self):
        time.sleep(0.000001)
        now = datetime.datetime.now()
        now_str = now.strftime("%d%m%y_%H%M%S_%f")
        new_name = "storage" + "_" + now_str + ".bin"
        self.cur_storage_file = new_name
        self.cur_storage_pos = 0

    def find_latest_storage_file(self):
        path = os.path.dirname(os.path.realpath(__file__)) + "/" + STORAGE_FOLDER
        files = os.listdir(STORAGE_FOLDER)
        files = [os.path.join(path, file) for file in files]
        if not files:
            self.generate_storage_file()
        else:
            latest = max(files, key=os.path.getctime)
            self.cur_storage_file = latest.rpartition("\\")[2]
            self.cur_storage_pos = int(os.path.getsize(latest) / self.seg_size)

    def create_table(self):
            create_query = f"""CREATE TABLE IF NOT EXISTS hash_table(
                id SERIAL PRIMARY KEY,
                hash TEXT,
                file_name TEXT,
                position INT,
                rep_num INT,
                cut CHARACTER VARYING({len(str(self.seg_size - 1))}) DEFAULT '0'
            );"""
            self.cursor.execute(create_query)
            self.conn.commit()

    @staticmethod
    def free_db(cursor, connection):
        cursor.execute("""DROP TABLE IF EXISTS hash_table;""")
        connection.commit()

        path = os.path.dirname(os.path.realpath(__file__)) + "/" + STORAGE_FOLDER
        files = os.listdir(STORAGE_FOLDER)
        files = [os.path.join(path, file) for file in files]
        for file in files:
            os.remove(file)
    
    def get_deduplicated_file_name(self, file):
        new_name = file + "_" + f"{self.id_size}_bytes_id_{self.seg_size}_bytes_seg_{self.hash_fun}.bin"
        return new_name
    
    @staticmethod
    def get_duplicated_file_name(file):
        new_name = file.split("_", maxsplit=1)[0]
        return new_name

    def get_hash(self, bytes) -> bytes:
        if (self.hash_fun == MD5):
            return hashlib.md5(bytes).hexdigest()
        elif (self.hash_fun == SHA1):
            return hashlib.sha1(bytes).hexdigest()
        elif (self.hash_fun == SHA256):
            return hashlib.sha256(bytes).hexdigest()
        elif (self.hash_fun == SHA512):
            return hashlib.sha512(bytes).hexdigest()
        elif (self.hash_fun == SHA224):
            return hashlib.sha3_224(bytes).hexdigest()
        else:
            print("ERROR: Invalid hash function.")
            exit(-1)


    def deduplicate_file(self, file):
        if not os.path.exists(DATA_FOLDER + "/" + file):
            print("ERROR: File doesn't exists.")
            exit(-1)

        print(f"Deduplicate file: {file}")
        deduplacated_file_name = self.get_deduplicated_file_name(file)
        with open(DATA_FOLDER + "/" + file, "rb") as input, \
             open(DEDUPLICATED_FOLDER + "/" + deduplacated_file_name, "wb") as output:
            seg = input.read(self.seg_size)
            
            while seg:
                hash = self.get_hash(seg)
                cut = '0'
                self.cursor.execute("SELECT * FROM hash_table WHERE hash = %s;", (hash, ))
                db_str = self.cursor.fetchone()

                if db_str is None: # does not exist in db
                    # check if there is enough place in current file
                    if self.cur_storage_pos * self.seg_size + self.seg_size > self.storage_size:
                        self.generate_storage_file()
                        
                    # write segment to storage
                    storage_name = STORAGE_FOLDER + "/" + self.cur_storage_file                    
                    with open(storage_name, "ab") as storage:
                        if len(seg) < self.seg_size:
                            cut = str(self.seg_size - len(seg))
                            zeros = bytes([0x00 for _ in range(self.seg_size - len(seg))])
                            seg = zeros + seg
                        storage.write(seg)           

                    # add hash to hash_table
                    self.cursor.execute("""INSERT INTO hash_table (hash, file_name, position, rep_num, cut) VALUES(%s,%s,%s,%s,%s) RETURNING id;""",
                                (hash, self.cur_storage_file, self.cur_storage_pos, 1, cut))
                    
                    self.cur_storage_pos += 1

                    self.conn.commit()
                    id = self.cursor.fetchone()[0]
                else: # increment repetition num
                    id = db_str[0]
                    rep_num = db_str[4]
                    self.cursor.execute("""UPDATE hash_table SET rep_num = %s WHERE id = %s;""", (rep_num + 1, id))
                    self.conn.commit()

                output.write(id.to_bytes(self.id_size, byteorder='big'))
                seg = input.read(self.seg_size)
        
        return deduplacated_file_name



    def duplicate_file(self, file_name):
        # check if file exists
        if not os.path.exists(DEDUPLICATED_FOLDER + "/" + file_name):
            print("ERROR: File doesn't exists.")
            exit(-1)
        # check if table exists
        self.cursor.execute("SELECT EXISTS(SELECT * from information_schema.tables where table_name='hash_table')")
        if not self.cursor.fetchone()[0]:
            print("ERROR: Table doesn't exists.")
            exit(-1)

        print(f"Duplicate file: {file_name}")
        with open(DEDUPLICATED_FOLDER + "/" + file_name, "rb") as file, \
             open(DUPLICATED_FOLDER + "/" + self.get_duplicated_file_name(file_name), "wb") as output:
            id_bytes = file.read(self.id_size)
            while id_bytes:
                id = int.from_bytes(id_bytes, byteorder='big')

                # find id in database
                self.cursor.execute("SELECT * FROM hash_table WHERE id = %s;", (id, ))
                db_str = self.cursor.fetchone()
                if db_str:
                    # read segment from storage
                    with open(STORAGE_FOLDER + f"/{db_str[2]}", "rb") as f:
                        f.seek(db_str[3] * self.seg_size + int(db_str[5]))
                        seg_bytes = f.read(self.seg_size)
                    output.write(seg_bytes)
                else:
                    print("ERROR: No such file in database.")
                    exit(-1)
                id_bytes = file.read(self.id_size)