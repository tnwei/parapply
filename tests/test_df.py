import pandas as pd
import numpy as np
import string
from parapply import parapply

# TODO: Test mixed input datatypes

def test_single_input_single_output():
    # NOTE: This is basically applying to a pd.Series! 
    def one_input_one_output_numeric(x): # Is this name still correct?
        return x * 3
    
    def one_input_one_output_string(x):
        return x * 3
    
    ## Test numeric DataFrames
    test_df = pd.DataFrame(data={
        'a': np.random.random(size=(1000, )),
        'b': np.random.random(size=(1000, )),
        'c': np.random.random(size=(1000, ))
    })
    
    # Test row-wise
    rowwise_sample_out_numeric = test_df.apply(one_input_one_output_numeric, axis=0)
    rowwise_test_out_numeric = parapply(test_df, one_input_one_output_numeric, axis=0)
    
    assert rowwise_sample_out_numeric.shape == rowwise_test_out_numeric.shape
    assert rowwise_sample_out_numeric.columns.equals(rowwise_test_out_numeric.columns)
    assert rowwise_sample_out_numeric.index.equals(rowwise_test_out_numeric.index)
    assert rowwise_sample_out_numeric.equals(rowwise_test_out_numeric)
    
    # Test column-wise
    columnwise_sample_out_numeric = test_df.apply(one_input_one_output_numeric, axis=1)
    columnwise_test_out_numeric = parapply(test_df, one_input_one_output_numeric, axis=1)
    assert columnwise_sample_out_numeric.shape == columnwise_test_out_numeric.shape
    assert columnwise_sample_out_numeric.columns.equals(columnwise_test_out_numeric.columns)
    assert columnwise_sample_out_numeric.index.equals(columnwise_test_out_numeric.index)
    assert columnwise_sample_out_numeric.equals(columnwise_test_out_numeric)
    
    ## Test string DataFrames
    def random_string_generator(strlen):
        return ''.join(np.random.choice(list(string.ascii_lowercase), size=10))
    
    test_df = pd.DataFrame(data={
        'a': [random_string_generator(10) for i in range(1000)],
        'b': [random_string_generator(10) for i in range(1000)],
        'c': [random_string_generator(10) for i in range(1000)]
    })
    
    # Test row-wise
    rowwise_sample_out_string = test_df.apply(one_input_one_output_string, axis=0)
    rowwise_test_out_string = parapply(test_df, one_input_one_output_string, axis=0)
    assert rowwise_sample_out_string.shape == rowwise_test_out_string.shape
    assert rowwise_sample_out_string.columns.equals(rowwise_test_out_string.columns)
    assert rowwise_sample_out_string.index.equals(rowwise_test_out_string.index)
    assert rowwise_sample_out_string.equals(rowwise_test_out_string)
    
    # Test column-wise
    columnwise_sample_out_string = test_df.apply(one_input_one_output_string, axis=1)
    columnwise_test_out_string = parapply(test_df, one_input_one_output_string, axis=1)
    assert columnwise_sample_out_string.shape == columnwise_test_out_string.shape
    assert columnwise_sample_out_string.columns.equals(columnwise_test_out_string.columns)
    assert columnwise_sample_out_string.index.equals(columnwise_test_out_string.index)
    assert columnwise_sample_out_string.equals(columnwise_test_out_string)

def test_rowwise_single_input_multi_output():
    pass

def test_columnwise_single_input_multi_output():
    pass
    
def test_rowwise_multi_input_single_output():
    test_df = pd.DataFrame(data={
        'a': np.random.random(size=(1000, )),
        'b': np.random.random(size=(1000, )),
        'c': np.random.random(size=(1000, ))
    })
    
    def three_input_one_output(x1, x2, x3):
        return x2 + x1 + x3
    
    rowwise_sample_out = test_df.apply(lambda x: three_input_one_output(*x), axis=1)
    rowwise_test_out = parapply(test_df, lambda x:three_input_one_output(*x), axis=1)
    assert rowwise_sample_out.shape == rowwise_test_out.shape
    assert rowwise_sample_out.columns.equals(rowwise_test_out.columns)
    assert rowwise_sample_out.index.equals(rowwise_test_out.index)
    assert rowwise_sample_out.equals(rowwise_test_out)

def test_columnwise_multi_input_single_output():
    test_df = pd.DataFrame(data={
        'a': np.random.random(size=(7, )),
        'b': np.random.random(size=(7, )),
        'c': np.random.random(size=(7, )),
        'd': np.random.random(size=(7, )),
    })
    
    def seven_input_one_output(x1, x2, x3, x4, x5, x6, x7):
        return x1 + x2 + x3 + x4 + x5 + x6 + x7
    
    columnwise_sample_out = test_df.apply(lambda x: seven_input_one_output(*x), axis=0)
    columnwise_test_out = parapply(test_df, lambda x:seven_input_one_output(*x), axis=0)
    assert columnwise_sample_out.shape == columnwise_test_out.shape
    assert columnwise_sample_out.columns.equals(columnwise_test_out.columns)
    assert columnwise_sample_out.index.equals(columnwise_test_out.index)
    assert columnwise_sample_out.equals(columnwise_test_out)

    
def test_rowwise_multi_input_multi_output():
    pass
    
def test_columnwise_multi_input_multi_output():
    pass