import pandas as pd
import numpy as np
import string
from parapply import parapply

# TODO: Test mixed input datatypes

def test_elementwise_fun():
    """
    This test checks for ufunc application.
    """
    def simple_fun(x):
        return x * 3
    
    ## Test numeric DataFrames
    test_df = pd.DataFrame(data={
        'a': np.random.random(size=(1000, )),
        'b': np.random.random(size=(1000, )),
        'c': np.random.random(size=(1000, ))
    })
    
    # Test row-wise application
    sample_out_numeric = test_df.apply(simple_fun, axis=0)
    test_out_numeric = parapply(test_df, simple_fun, axis=0)
    
    assert sample_out_numeric.shape == test_out_numeric.shape
    assert sample_out_numeric.columns.equals(test_out_numeric.columns)
    assert sample_out_numeric.index.equals(test_out_numeric.index)
    assert sample_out_numeric.equals(test_out_numeric)
    
    # Test column-wise application
    sample_out_numeric = test_df.apply(simple_fun, axis=1)
    test_out_numeric = parapply(test_df, simple_fun, axis=1)
    assert sample_out_numeric.shape == test_out_numeric.shape
    assert sample_out_numeric.columns.equals(test_out_numeric.columns)
    assert sample_out_numeric.index.equals(test_out_numeric.index)
    assert sample_out_numeric.equals(test_out_numeric)
    
    ## Test string DataFrames
    def random_string_generator(strlen):
        return ''.join(np.random.choice(list(string.ascii_lowercase), size=10))
    
    test_df = pd.DataFrame(data={
        'a': [random_string_generator(10) for i in range(1000)],
        'b': [random_string_generator(10) for i in range(1000)],
        'c': [random_string_generator(10) for i in range(1000)]
    })
    
    # Test row-wise
    sample_out_string = test_df.apply(simple_fun, axis=0)
    test_out_string = parapply(test_df, simple_fun, axis=0)
    assert sample_out_string.shape == test_out_string.shape
    assert sample_out_string.columns.equals(test_out_string.columns)
    assert sample_out_string.index.equals(test_out_string.index)
    assert sample_out_string.equals(test_out_string)
    
    # Test column-wise
    sample_out_string = test_df.apply(simple_fun, axis=1)
    test_out_string = parapply(test_df, simple_fun, axis=1)
    assert sample_out_string.shape == test_out_string.shape
    assert sample_out_string.columns.equals(test_out_string.columns)
    assert sample_out_string.index.equals(test_out_string.index)
    assert sample_out_string.equals(test_out_string)

def test_columnwise_fun():
    """
    This test checks for column-wise function application.
    """
    test_df = pd.DataFrame(data={
        'a': np.random.random(size=(1000, )),
        'b': np.random.random(size=(1000, )),
        'c': np.random.random(size=(1000, ))
    })
    
    def three_input_one_output(x1, x2, x3):
        return x2 + x1 + x3
    
    sample_out = test_df.apply(lambda x: three_input_one_output(*x), axis=1)
    test_out = parapply(test_df, lambda x:three_input_one_output(*x), axis=1)
    assert sample_out.shape == test_out.shape
    # Output is a Series, no columns! 
    assert sample_out.index.equals(test_out.index)
    assert sample_out.equals(test_out)
    
def test_rowwise_fun():
    """
    This test checks for row-wise function application.
    """
    
    test_df = pd.DataFrame(data={
        'a': np.random.random(size=(7, )),
        'b': np.random.random(size=(7, )),
        'c': np.random.random(size=(7, )),
        'd': np.random.random(size=(7, )),
    })
    
    def seven_input_one_output(x1, x2, x3, x4, x5, x6, x7):
        return x1 + x2 + x3 + x4 + x5 + x6 + x7
    
    sample_out = test_df.apply(lambda x: seven_input_one_output(*x), axis=0)
    test_out = parapply(test_df, lambda x:seven_input_one_output(*x), axis=0)
    assert sample_out.shape == test_out.shape
    # Output is a Series, no columns!
    assert sample_out.index.equals(test_out.index)
    assert sample_out.equals(test_out)