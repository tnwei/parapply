import pandas as pd
import numpy as np
from joblib import Parallel, delayed, cpu_count

def split_srs(srs, n_chunks):
    """
    Generator used internally by `parapply` to split 
    `pandas` Series by indices.
    
    Parameters
    ----------
    srs: `pandas` Series to be split into chunks
    n_chunks: Number of chunks to split
    
    Yields
    ------
    chunk: Chunk of original Series, ordered by index
    """
    assert isinstance(n_chunks, int), f'n_chunks needs to be int instead of {type(n_chunks)}'
    assert isinstance(srs, pd.Series), f'srs needs to be pandas.Series instead of {type(pd.Series)}'
    
    max_chunks = len(srs)
    if n_chunks > max_chunks:
        n_chunks = max_chunks    
        
    # Create indices to split
    idxs = [int(i) for i in np.linspace(start=0, stop=max_chunks, num=n_chunks+1)]
    # Setting num=n_chunks+1 guarantees last idx to be the final one

    # Split along indices
    for i in range(n_chunks):
        chunk = srs.iloc[idxs[i]:idxs[i+1]]
        yield chunk

def split_df(df, n_chunks, axis):
    """
    Generator used internally by `parapply` to split 
    pandas DataFrame either by indices or by columns.
    
    Parameters
    ----------
    df: `pandas` DataFrame to be split into chunks
    n_chunks: Number of chunks to split
    axis: Split direction, 0 / 'rows' or 1 / 'columns'
    
    Yields
    ------
    chunk: Chunk of original DataFrame, ordered by indices/columns
        as specified by `axis`
    """
    assert isinstance(n_chunks, int), f'n_chunks needs to be int instead of {type(n_chunks)}'
    assert isinstance(df, pd.DataFrame), f'df needs to be pandas.DataFrame instead of {type(pd.DataFrame)}'
    assert isinstance(axis, int), f'axis needs to be int instead of {type(axis)}'
    
    # Divides df into n_chunks
    if (axis == 0) or (axis == 'rows'):
        max_chunks = len(df)
        if n_chunks > max_chunks:
            n_chunks = max_chunks    
        
        # If split row-wise, split along indices
        idxs = [int(i) for i in np.linspace(start=0, stop=max_chunks, num=n_chunks+1)]
        # Setting num=n_chunks+1 guarantees last idx to be the final one

        for i in range(n_chunks):
            chunk = df.iloc[idxs[i]:idxs[i+1], :]
            yield chunk
                
    elif (axis == 1) or (axis == 'columns'):
        # If split column-wise, split along columns
        max_chunks = len(df.columns)
        if n_chunks > max_chunks:
            n_chunks = max_chunks
        idxs = [int(i) for i in np.linspace(start=0, stop=len(df.columns), num=n_chunks+1)]

        # Yields each chunk by chunk
        # Split by columns does not have the same
        # issue as split by rows when n_chunks
        # ends up splitting single rows
        # pd.concat(axis=1) on a list of Series
        # concats them back into a DataFrame!
        for i in range(n_chunks):
            chunk = df.iloc[:, idxs[i]:idxs[i+1]]
            yield chunk
    else:
        # If not correctly parsed as row-wise or column-wise
        raise ValueError(f'axis specified improperly as {axis}, valid inputs are: 0, 1, "axis", or "columns".')

# Worth noting that:
# using iloc[a:b] means [a, b)
# while using loc[a:b] means [a, b]! 
        
def parapply(obj, fun, axis=0, 
             n_jobs=-1, n_chunks='auto', 
             backend='loky', verbose=0):
    """
    Parallelized version of `apply` for pandas DataFrame 
    and pandas Series objects.
    
    Parameters
    ----------
    obj: DataFrame / Series of interest
    fun: Function of interest
    axis: 0 / 1 or 'rows' / 'columns'
    n_jobs: Max number of concurrent jobs, defaults to 
        -1 for all cpu cores. Parameter to be 
        passed to `joblib` backend. Refer to 
        https://joblib.readthedocs.io/en/latest/generated/joblib.Parallel.html.
    n_chunks: Number of chunks to split the Series / 
        Dataframe into. Defaults to CPU core count from 
        `joblib.cpu_count()`. More chunks means more 
        concurrent jobs can be run at the same time. 
    backend: `joblib` parallelization backend, defaults to 
        'loky'. Refer to https://joblib.readthedocs.io/en/latest/generated/joblib.Parallel.html.
    
    verbose: Verbosity, parameter to be passed to
        `joblib` backend. 
        
    Returns
    -------
    result: DataFrame / Series after `fun` has been applied
    """
    # Parallelization input
    if n_chunks == 'auto':
        n_chunks = cpu_count()
    
    # For Series input
    if type(obj) == type(pd.Series()):
        split_obj_gen = split_srs(obj, n_chunks)
        output = Parallel(n_jobs=n_jobs, verbose=verbose, backend='loky')(map(delayed(lambda x: x.apply(fun)), split_obj_gen))
        result = pd.concat(output, sort=True)
        return result
    
    # For Dataframe input
    elif type(obj) == type(pd.DataFrame()):
        if (axis == 1) or (axis == 'columns'):
            concat_axis = 0
        elif (axis == 0) or (axis == 'rows'):
            concat_axis = 1

        split_obj_gen = split_df(obj, n_chunks, axis=concat_axis)
        output = Parallel(n_jobs=n_jobs, verbose=verbose, backend='loky')(map(delayed(lambda x: x.apply(fun, axis=axis)), split_obj_gen))

        # If the output is condensed to Series instead of DataFrames
        # Only one axis for concat
        if type(output[0]) == type(pd.Series()):
            result = pd.concat(output, sort=True)
            return result
        
        result = pd.concat(output, axis=concat_axis, sort=True)
        return result
