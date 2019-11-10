import pandas as pd
import numpy as np
import string
from parapply import parapply

def test_srs_apply():
    def simple_multiply(x):
        return x * 3
    
    test_srs = pd.Series(
        data=np.random.random(size=(1000, )),
        index=range(1000)
    )

    sample_out = test_srs.apply(simple_multiply)
    test_out = parapply(test_srs, simple_multiply)
    
    assert sample_out.shape == test_out.shape
    assert sample_out.index.equals(test_out.index)
    assert sample_out.equals(test_out)