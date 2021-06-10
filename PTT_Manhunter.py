from optparse import OptionParser
from os import listdir
from os.path import isfile, join
from shutil import copyfile
from multiprocessing import Process
from multiprocessing import Pool
import multiprocessing
import json
import time
from parsing_config import parse_cfg_file_to_list


DDP_OPP = 0
KMT_OPP = 1
POLITICS = 2
NO_POLITICS = 3

DDP_opposition_cfg = parse_cfg_file_to_list("./Config/DDP_opposition.cfg")
KMT_opposition_cfg = parse_cfg_file_to_list("./Config/KMT_opposition.cfg")
Politics_related_cfg = parse_cfg_file_to_list("./Config/Politics_related.cfg")

class Pool_Args:
    def __init__(self, file_name, PTT_DB_Path):
        self.file_name = file_name
        self.PTT_DB_Path = PTT_DB_Path
class Pool_Args_Custom:
    def __init__(self, file_name, PTT_DB_Path, Custom_cfg):
        self.file_name = file_name
        self.PTT_DB_Path = PTT_DB_Path
        self.Custom_cfg = Custom_cfg

def find_keyword_in_json_file(arguments):
    json_str = open("./{}/{}".format(arguments.PTT_DB_Path, arguments.file_name), "r", encoding="UTF-8")
    PTT_DB_hash = json.loads(json_str.read())
    output = []
    DDP_opposition_dict = {}
    KMT_opposition_dict = {}
    Politics_related_dict = {}
    for article_idx in range(len(PTT_DB_hash)):
        if PTT_DB_hash[article_idx]["title"].find("Re: ") == -1:
            position = identify_position(PTT_DB_hash[article_idx]["title"])
            author = remove_author_nickname(PTT_DB_hash[article_idx]["author"])

            if position is DDP_OPP:
                if author in DDP_opposition_dict:
                    DDP_opposition_dict[author] += 1
                else:
                    DDP_opposition_dict[author] = 1
            elif position is KMT_OPP:
                if author in KMT_opposition_dict:
                    KMT_opposition_dict[author] += 1
                else:
                    KMT_opposition_dict[author] = 1        
            elif position is POLITICS:
                if author in Politics_related_dict:
                    Politics_related_dict[author] += 1
                else:
                    Politics_related_dict[author] = 1

        for push_idx in range(len(PTT_DB_hash[article_idx]["push_list"])):
            position = identify_position(PTT_DB_hash[article_idx]["push_list"][push_idx]["content"])
            author = PTT_DB_hash[article_idx]["push_list"][push_idx]["user"]
            if position is DDP_OPP:
                if author in DDP_opposition_dict:
                    DDP_opposition_dict[author] += 1
                else:
                    DDP_opposition_dict[author] = 1
            elif position is KMT_OPP:
                if author in KMT_opposition_dict:
                    KMT_opposition_dict[author] += 1
                else:
                    KMT_opposition_dict[author] = 1        
            elif position is POLITICS:
                if author in Politics_related_dict:
                    Politics_related_dict[author] += 1
                else:
                    Politics_related_dict[author] = 1
    output.append(DDP_opposition_dict)
    output.append(KMT_opposition_dict)
    output.append(Politics_related_dict)
    return output

def find_keyword_in_json_file_custom(arguments):
    json_str = open("./{}/{}".format(arguments.PTT_DB_Path, arguments.file_name), "r", encoding="UTF-8")
    PTT_DB_hash = json.loads(json_str.read())
    output = []
    Custom_dict = {}
    
    Custom_cfg = arguments.Custom_cfg

    for article_idx in range(len(PTT_DB_hash)):
        author = remove_author_nickname(PTT_DB_hash[article_idx]["author"])
        if PTT_DB_hash[article_idx]["title"].find("Re: ") == -1:            
            for Custom_word in Custom_cfg:
                if PTT_DB_hash[article_idx]["title"].find(Custom_word) != -1:
                    if author not in Custom_dict:
                        Custom_dict[author] = []
                    Custom_dict[author].append(PTT_DB_hash[article_idx]["title"])

        for push_idx in range(len(PTT_DB_hash[article_idx]["push_list"])):
            author = PTT_DB_hash[article_idx]["push_list"][push_idx]["user"]
            for Custom_word in Custom_cfg:
                if PTT_DB_hash[article_idx]["push_list"][push_idx]["content"].find(Custom_word) != -1:
                    if author not in Custom_dict:
                        Custom_dict[author] = []
                    Custom_dict[author].append(PTT_DB_hash[article_idx]["push_list"][push_idx]["content"])

    return Custom_dict

def identify_position(content):
    for DDP_opp_word in DDP_opposition_cfg:
        if content.find(DDP_opp_word) != -1:
            return DDP_OPP
    for KMT_opp_word in KMT_opposition_cfg:
        if content.find(KMT_opp_word) != -1:
            return KMT_OPP
    for Politics_word in Politics_related_cfg:
        if content.find(Politics_word) != -1:
            return POLITICS
    return NO_POLITICS

def remove_author_nickname(author):
    return author[0:author.find(" (")]

def merge_dict(result, being_merged_dict):
    for key, value in being_merged_dict.items():
        if key in result:
            result[key] += value
        else:
            result[key] = value
def merge_dict_custom(result, being_merged_dict):
    for key, value in being_merged_dict.items():
        if key in result:
            for element in being_merged_dict[key]:
                result[key].append(element)
        else:
            result[key] = value

if __name__ == "__main__":
    PTT_DB_Path = "./PTT_DB"
    cpu_count = multiprocessing.cpu_count()
    DDP_opposition_result = {}
    KMT_opposition_result = {}
    Politics_related_result = {}
    Custom_result = {}
    result_buf = []
    parser = OptionParser()
    parser.add_option("-c", "--cpu_count", dest="cpu_count", type="int",
                      help="set cpu count, range is 1 to {}".format(cpu_count), default=cpu_count)
    parser.add_option("-d", "--db_folder", dest="db_folder", type="string",
                      help="set DB folder", default="")
    parser.add_option("-f", "--file_config", dest="custom_config", type="string",
                      help="set custom config file", default="")
    (options, args) = parser.parse_args()
    if options.db_folder != "":
        PTT_DB_Path = options.db_folder
    if options.cpu_count < 1 or options.cpu_count > cpu_count:
        print("Wrong Input of CPU_COUNT, use default value {}".format(cpu_count))
    else:
        cpu_count = options.cpu_count
    start = time.time()

    file_name_list = [f for f in listdir(PTT_DB_Path) if isfile(join(PTT_DB_Path, f))]
    print("CPU Count: {}".format(cpu_count))
    if options.custom_config == "":
        if cpu_count == 1:
            for file_name in file_name_list:
                arguments = Pool_Args(file_name = file_name, PTT_DB_Path = PTT_DB_Path)
                result_buf = find_keyword_in_json_file(arguments)
                merge_dict(DDP_opposition_result, result_buf[DDP_OPP])
                merge_dict(KMT_opposition_result, result_buf[KMT_OPP])
                merge_dict(Politics_related_result, result_buf[POLITICS])
        else:
            arguments_list = []
            for file_name in file_name_list:
                arguments = Pool_Args(file_name=file_name, PTT_DB_Path=PTT_DB_Path)
                arguments_list.append(arguments)
            with Pool(cpu_count) as p:
                result_buf = p.map(find_keyword_in_json_file, arguments_list)
            for result in result_buf:
                merge_dict(DDP_opposition_result, result[DDP_OPP])
                merge_dict(KMT_opposition_result, result[KMT_OPP])
                merge_dict(Politics_related_result, result[POLITICS])
        DDP_opposition_result = sorted(DDP_opposition_result.items(), key=lambda x: x[1], reverse=True)
        KMT_opposition_result = sorted(KMT_opposition_result.items(), key=lambda x: x[1], reverse=True)
        Politics_related_result = sorted(Politics_related_result.items(), key=lambda x: x[1], reverse=True)
    else:
        arguments_list = []
        Custom_cfg = parse_cfg_file_to_list(options.custom_config)
        for file_name in file_name_list:
            arguments = Pool_Args_Custom(file_name=file_name, PTT_DB_Path=PTT_DB_Path, Custom_cfg=Custom_cfg)
            arguments_list.append(arguments)
        with Pool(cpu_count) as p:
            result_buf = p.map(find_keyword_in_json_file_custom, arguments_list)
        for result in result_buf:
            merge_dict_custom(Custom_result, result)
        f = open("./Custom_result.txt", "w", encoding="UTF-8")
        for key in Custom_result:
            f.write("User: {}\n".format(key))
            content_idx = 1
            for content in Custom_result[key]:
                f.write("{}: {}\n".format(content_idx, content))
                content_idx += 1
        print("Output the result at Custom_result.txt!")
        f.close()
    print("Done: {} s".format(time.time() - start))
    if options.custom_config == "":
        f = open("./DDP_result.csv", "w")
        
        for key, value in DDP_opposition_result:
            f.write("{},{}\n".format(key, value))
        f.close()
        f = open("./KMT_result.csv", "w")
        for key, value in KMT_opposition_result:
            f.write("{},{}\n".format(key, value)) 
        f.close()
        f = open("./Politics_result.csv", "w")
        for key, value in Politics_related_result:
            f.write("{},{}\n".format(key, value))
        f.close()