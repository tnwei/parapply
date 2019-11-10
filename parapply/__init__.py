import pandas as pd
import numpy as np
from joblib import Parallel, delayed

# TODO: To make very clear the meaning between rowwise and columnwise, getting confused myself! 
# TODO: Build in checks when parapply would slow things down instead and raise warnings
# TODO: Add checks for n_chunks and n_jobs to make sense w.r.t each others

def split_srs(srs, n_chunks):
    """
    Generator used internally by `parapply` to split 
    pandas Series by indices.
    
    Parameters
    ----------
    srs: `pandas` Series to be split into chunks
    n_chunks: Number of chunks to split
    """
    max_chunks = len(srs)
    if n_chunks > max_chunks:
        n_chunks = max_chunks    
        
    # Create indices to split
    idxs = [int(i) for i in np.linspace(start=0, stop=max_chunks, num=n_chunks+1)]
    # Setting num=n_chunks+1 guarantees last idx to be the final one

    # Split along indices
    for i in range(n_chunks):
        yield srs.iloc[idxs[i]:idxs[i+1]]

def split_df(df, n_chunks, axis):
    """
    Generator used internally by `parapply` to split 
    pandas DataFrame either by indices or by columns.
    
    Parameters
    ----------
    df: 
    n_chunks:
    axis:
    """
    # Divides df into n_chunks
    if (axis == 0) or (axis == 'rows'):
        max_chunks = len(df)
        if n_chunks > max_chunks:
            n_chunks = max_chunks    
        
        # If split row-wise, split along indices
        idxs = [int(i) for i in np.linspace(start=0, stop=max_chunks, num=n_chunks+1)]
        # Setting num=n_chunks+1 guarantees last idx to be the final one

        for i in range(n_chunks):
            yield df.iloc[idxs[i]:idxs[i+1], :]
                
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
            yield df.iloc[:, idxs[i]:idxs[i+1]]
    else:
        # If not correctly parsed as row-wise or column-wise
        raise ValueError(f'axis specified improperly as {axis}, valid inputs are: 0, 1, "axis", or "columns".')

# The really peculiar thing about this is 
# using iloc[a:b] means [a, b)
# while using loc[a:b] means [a, b]! 
        
def parapply(obj, fun, axis=0, n_jobs=8, n_chunks=10, verbose=0):
    """
    Parallelized version of `apply` for pandas DataFrame 
    and pandas Series objects.
    
    Parameters
    ----------
    obj: DataFrame / Series of interest
    fun: Function of interest
    axis: 0 / 1 or 'rows' / 'columns'
    n_jobs: Max number of concurrent jobs, parameter 
        to be passed to `joblib` backend
    n_chunks: Chunks to split the DataFrame / Series into. 
        More chunks means more concurrent jobs can be 
        run at the same time. 
    verbose: Verbosity, parameter to be passed to
        `joblib` backend. 
    """
    # original_axes = obj.index.copy()
    
    # TODO: auto suggest default n_chunks and n_jobs?
    
    if type(obj) == type(pd.Series()):
        split_obj_gen = split_srs(obj, n_chunks)
        output = Parallel(n_jobs=n_jobs, verbose=verbose, backend='loky')(map(delayed(lambda x: x.apply(fun)), split_obj_gen))
        return pd.concat(output, sort=True)
        
    elif type(obj) == type(pd.DataFrame()):
        if (axis == 1) or (axis == 'columns'):
            concat_axis = 0
        elif (axis == 0) or (axis == 'rows'):
            concat_axis = 1

        split_obj_gen = split_df(obj, n_chunks, axis=concat_axis)
        output = Parallel(n_jobs=n_jobs, verbose=verbose, backend='loky')(map(delayed(lambda x: x.apply(fun, axis=axis)), split_obj_gen))

        # If the output is condensed to Series instead of DataFrames
        # We can concat regardless
        if type(output[0]) == type(pd.Series()):
            return pd.concat(output, sort=True)
        
        return pd.concat(output, axis=concat_axis, sort=True)