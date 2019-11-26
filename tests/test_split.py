import pandas as pd
import numpy as np
from parapply import parapply, split_srs, split_df
import types

test_srs = pd.Series(
    data=np.random.random(size=(1000, )),
    index=range(1000)
)

def test_split_srs_type():
    split_gen = split_srs(test_srs, 10)
    
    # Ensure the output is a generator
    assert isinstance(split_gen, types.GeneratorType)

def test_split_srs_items():
    split_gen = split_srs(test_srs, 10)
    for i in split_gen:
        # Ensure each item is a Series
        assert isinstance(i, pd.Series)

def test_split_srs_outputs():
    split_gen = split_srs(test_srs, 10)
    
    # Perform some function
    output = [i *2 for i in split_gen]
    output_srs = pd.concat(output)
    
    assert output_srs.index.equals(test_srs.index)

test_df = pd.DataFrame(
    data=np.random.random(size=(1000, )),
    index=range(1000)
)
    
def test_split_df_type():
    split_gen = split_df(test_df, 10, axis=0)
    
    # Ensure the output is a generator
    assert isinstance(split_gen, types.GeneratorType)
    
def test_split_df_items():
    split_gen = split_df(test_df, 10, axis=0)
    for i in split_gen:
        # Ensure each item is a Series
        assert isinstance(i, pd.DataFrame)

def test_split_df_outputs():
    split_gen = split_df(test_df, 10, axis=0)
    
    # Perform some function
    output = [i *2 for i in split_gen]
    output_df = pd.concat(output, axis=0)
    
    assert output_df.index.equals(test_df.index)
