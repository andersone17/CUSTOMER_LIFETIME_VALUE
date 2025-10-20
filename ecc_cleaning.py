# File to Clean and Stack ECC Data

import pandas as pd
import numpy as np
import os

frames = []
files = os.listdir(r"DATA\SOV\ZORDERLIST")

df = pd.read_excel(f"DATA\\SOV\\ZORDERLIST\\{files[0]}")









for file in files:
    df = pd.read_excel(f"DATA\\SOV\\ZORDERLIST\\{file}")
    frames.append(df)

ecc = pd.concat(frames)