import argparse
import hashlib
import os
import xmlrpc.client

def update_local_file_index(filename):
    pass

def download_remote_blocks():
    pass

def needUpdateFileInfo(filename,Remote_FileInfoMap, local_file_changed_version_map):
    if not filename in Remote_FileInfoMap.keys():
        return True
    changed, version = local_file_changed_version_map[filename]
    if changed and version == Remote_FileInfoMap[filename][0]:
        return True
    return False

def needDownloadFileInfo(filename,Remote_FileInfoMap, local_file_changed_version_map):
    if not filename in local_file_changed_version_map.keys():
        return True
    _, version_local = local_file_changed_version_map[filename]
    version_remote = Remote_FileInfoMap[filename][0]
    if version_local < version_remote:
        return True
    return False


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SurfStore client")
    parser.add_argument('hostport', help='host:port of the server')
    parser.add_argument('basedir', help='The base directory')
    parser.add_argument('blocksize', type=int, help='Block size')
    args = parser.parse_args()

    all_client_file_names = os.listdir(args.basedir)


    FileInfoMap = dict()
    if "index.txt" in all_client_file_names:
        with open(args.basedir+"/index.txt","r") as f:
            for line in f:
                line = line.strip().split(" ")
                FileInfoMap[line[0]] = [int(line[1]),line[2:]]

        all_client_file_names.remove("index.txt")

    local_file_changed_version_map = dict() # status of 1. changed 2.version of the current clinet directory

    for index_file in FileInfoMap.keys():
        if not index_file in all_client_file_names:
            ## has deleted
            local_file_changed_version_map[index_file] = (True,FileInfoMap[index_file][0])
            FileInfoMap[index_file] = [FileInfoMap[index_file][0], [0]]


    for file_name in all_client_file_names:
        with open(args.basedir + "/" + file_name, 'rb') as f:
            changed = True
            file_binary = f.read()
            total_block_num = (len(file_binary) - 1) // args.blocksize + 1

            # make same length for in-place comparison
            if not file_name in FileInfoMap.keys():
                FileInfoMap[file_name] = [0, [0] * total_block_num]
            elif total_block_num > len(FileInfoMap[file_name][1]):
                FileInfoMap[file_name][1] += [0] * (total_block_num - len(FileInfoMap[file_name][1]))
            elif total_block_num < len(FileInfoMap[file_name][1]):
                FileInfoMap[file_name][1] = FileInfoMap[file_name][1][:(total_block_num)]
            else:
                changed = False


            for i in range(0, len(file_binary), args.blocksize):
                block = file_binary[i:i + args.blocksize]
                hash_value = hashlib.sha256(block).hexdigest()
                block_num = i // args.blocksize
                if not FileInfoMap[file_name][1][block_num] == hash_value:
                    changed = True
                    FileInfoMap[file_name][1][block_num] = hash_value
            local_file_changed_version_map[file_name] = (changed, FileInfoMap[file_name][0])




    input_host = "http://" + args.hostport if not args.hostport.startswith("http://") else args.hostport
    server = xmlrpc.client.ServerProxy(input_host)


    Remote_FileInfoMap = server.surfstore.getfileinfomap()
    FilesToDownload = set(Remote_FileInfoMap.keys())


    for local_file in local_file_changed_version_map.keys():
        if needUpdateFileInfo(local_file,Remote_FileInfoMap, local_file_changed_version_map):
            if not FileInfoMap[local_file][1] == [0]:
                with open(args.basedir + "/" + local_file, 'rb') as f:
                    file_binary = f.read()
                    for i in range(0, len(file_binary), args.blocksize):
                        block = file_binary[i:i + args.blocksize]
                        hash_value = hashlib.sha256(block).hexdigest()
                        if not server.surfstore.hasblock(hash_value):
                            server.surfstore.putblock(block)

            if server.surfstore.updatefile(local_file, FileInfoMap[local_file][0]+1, FileInfoMap[local_file][1]):
                #print(local_file)
                FileInfoMap[local_file][0] += 1
                if local_file in FilesToDownload:
                    FilesToDownload.remove(local_file)
            else:
                Remote_FileInfoMap[local_file] = server.surfstore.getonefileinfomap(local_file)



    for remote_file in Remote_FileInfoMap.keys():
        if needDownloadFileInfo(remote_file,Remote_FileInfoMap, local_file_changed_version_map):
            FileInfoMap[remote_file] = Remote_FileInfoMap[remote_file]
            hash_list = FileInfoMap[remote_file][1]
            if hash_list == [0]:
                if os.path.exists(args.basedir + "/" + remote_file):
                    os.remove(args.basedir + "/" + remote_file)
            else:
                with open(args.basedir + "/" + remote_file, 'wb') as f:
                    for hash_value in hash_list:
                        block = server.surfstore.getblock(hash_value).data
                        f.write(block)

    with open(args.basedir + "/" + "index.txt", "w") as f:
        to_write = [k+" "+str(v[0])+" "+" ".join([str(i) for i in v[1]]) for k, v in FileInfoMap.items()]
        f.write("\n".join(to_write))





