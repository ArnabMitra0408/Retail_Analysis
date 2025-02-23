import yaml
import pandas as pd
import numpy as np
import os

def read_params(config_path:str) -> dict:
    with open(config_path) as yaml_file:
        config = yaml.safe_load(yaml_file)
    return config