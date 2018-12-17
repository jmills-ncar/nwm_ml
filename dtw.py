from tslearn.metrics import dtw_path
import numpy as np
import pandas as pd

peak = list(range(21))
seq1 = peak
seq2 = peak[1:]
seq2.append(0)

df = pd.DataFrame({'seq1':seq1,'seq2',seq2})
path, sim = dtw_path(seq1,seq2)