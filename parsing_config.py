def parse_cfg_file_to_list(file_name):
    cfg_file = open(file_name, "r", encoding="UTF-8")
    cfg_list = cfg_file.readlines()
    output_list = []
    for cfg in cfg_list:
        cfg = cfg.strip()
        if cfg != "":
            output_list.append(cfg)
    return output_list