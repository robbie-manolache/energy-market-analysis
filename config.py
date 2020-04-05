import os
import json

def use(config_file):
    """
    """
    with open(config_file) as rf:
        config = json.load(rf)
    
    config_template = {}
    for k, v in config.items():
        os.environ[k] = v
        config_template[k] = ""
        print("Value for %s has been set!"%(k))

    with open("config_template.json", "w") as wf:
        json.dump(config_template, wf)
    
