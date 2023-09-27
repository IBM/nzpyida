import nzpyida
from nzpyida import IdaDataFrame
from typing import List
from nzpyida.analytics.utils import q
import itertools

def concat(objs: List[IdaDataFrame], axis: int=0, join: str='outer', keys: List[str]=None):
    """
    Implement pandas-like interface to concateate IdaDataFrames

    Parameters
    ----------
    objs : List[IdaDataFrame]
        list of IdaDataFrames to be concatenated
    axis : int, optional
        axis to concatenate on
    join : str, optional
        type of join, possible types - 'outer', 'inner'
    keys : List[str], optional
        if List is present then add column with table indetifier
    
    Returns
    -------
    IdaDataFrame
        reult of concataneting IdaDataFrames
    
    Examples
    --------
    >>> from nzpyida.sampledata.iris import iris
    >>> from nzpyida.sampledata.iris import iris as iris2
    >>> iris.reset_index()
    >>> iris2.reset_index()
    >>> iris2.columns = ['id', 'sepal_length', 'sepal_width', 'petal_length', \
        'PETAL_WIDTH', 'CLASS']
    >>> iris2['id'] = iris2['id'] + 50
    >>> iris1_ida = idadb.as_idadataframe(iris, 'IRIS_TEST1', indexer='index')
    >>> iris2_ida = idadb.as_idadataframe(iris2, 'IRIS_TEST2', indexer='id')
    >>> iris_concat = nzpyida.concat([iris1_ida, iris2_ida])
    >>> len(iris1_ida), len(iris2_ida), len(iris_concat)
    (150, 150, 300)
    >>> iris_concat.head()
     	index 	sepal_length 	sepal_width 	petal_length 	petal_width 	species     id      PETAL_WIDTH CLASS
    0 	None 	4.9 	        3.0 	        1.4 	        None 	        None        51      0.2         setosa
    1 	None 	4.6 	        3.1 	        1.5 	        None 	        None        53      0.2         setosa
    2 	None 	4.7 	        3.2 	        1.3 	        None 	        None        52      0.2         setosa
    3 	None 	5.1 	        3.5 	        1.4 	        None 	        None        50      0.2         setosa
    4 	None 	5.9 	        3.0 	        5.1 	        None 	        None        199     1.8         virginica
    >>> iris_concat.tail()
     	index 	sepal_length 	sepal_width 	petal_length 	petal_width 	species 	id      PETAL_WIDTH 	CLASS									
    295 145 	6.7 	        3.0 	        5.2 	        2.3 	        virginica 	None    None 	        None
    296 146 	6.3 	        2.5 	        5.0 	        1.9 	        virginica 	None 	None 	        None
    297 147 	6.5 	        3.0 	        5.2 	        2.0 	        virginica 	None 	None 	        None
    298 148 	6.2 	        3.4 	        5.4 	        2.3 	        virginica 	None 	None 	        None
    299 149 	5.9 	        3.0 	        5.1 	        1.8 	        virginica 	None 	None 	        None
    """
    if not isinstance(objs, List):
        raise ValueError("Argument 'objs' should be a sequnece")
    if len(objs) < 2:
        raise ValueError("Argument 'objs' should contain at least 2 elements")

    idadb = objs[0]._idadb
    idx = None
    if axis == 0:
        if keys:
            case_statments = [f" '{q(keys[i])}' as keys, " if i < len(keys) else f" '{q(keys[-1])}' as keys, "
                               for i in range(len(objs))]
        else:
            case_statments = [""] * len(objs)
        if all(list(obj.columns) == list(objs[0].columns) for obj in objs):
            names_list = [f'{objs[i].internal_state.get_state()} AS TEMP_JOIN_{i} ' for i in range(len(objs))]
            query = "SELECT * FROM " + " UNION ALL SELECT * FROM ".join(names_list)
        else:
            in_columns_table = [obj.columns for obj in objs]

            all_columns = list(dict.fromkeys(itertools.chain.from_iterable(in_columns_table)))
            if join == 'inner':
                output_columns = [q(col) for col in all_columns 
                              if all([col in obj.columns for obj in objs])]
                out_columns_table = [output_columns for _ in objs]
                
            else: 
                out_columns_table = [
                    [q(col) if col in in_columns_list else "null AS " + q(col) 
                        for col in all_columns] 
                            for in_columns_list in in_columns_table]
                
            columns_queries = ['SELECT ' + case_statments[i] + 
                                ', '.join(out_columns_table[i]) + 
                                f" FROM ({objs[i].internal_state.get_state()}) AS TEMP_{i}" 
                                for i in range(len(objs))
                               ]
            query = " UNION ALL ".join(columns_queries)
            if keys:
                idx = "KEYS"
        idadf = nzpyida.IdaDataFrame(idadb, objs[0].tablename, indexer=idx)
        idadf.internal_state._views.append(query)
    elif axis == 1:
        idadf = objs[0] 
        for i in range(1, len(objs)):
            idadf = idadf.join(objs[i], how=join)
    else:
        raise ValueError("Axis must be 0 or 1")
    return idadf


def merge(left: IdaDataFrame, right: IdaDataFrame, how: str='inner', on=None, 
          left_on=None, right_on=None, left_index: bool=False, 
          right_index: bool=False, suffixes: List[str]=["_x", "_y"],
          indicator: bool=False):
    """
    Implement pandas-like interface to merge IdaDataFrames

    Parameters
    ----------
    left : IdaDataFrame
        left IdaDataFrame to merge
    right : IdaDataFrame
        right IdaDataDrame to merge
    how : str, optional
        type of join, possible types - 'outer', 'inner', 'right', 'left', 'cross'
    on : str, optional
        name of column in both IdaDataFrames to do join on
    left_on : str, optional
        name of column in left IdaDataFrame to do join on
    right_on : str, optional
        name of column in right IdaDataFrame to do join on
    left_index : bool, optional
        whether to join on indexer of left IdaDataFrame
    right_index : bool, optional
        whether to join on indexer of right IdaDataFrame
    suffixes: List[str], optional
        list of suffixes to add to columns that are present in both IdaDataFrames
    indicator: bool optional
        whether to add to output IdaDataFrame, column with information
        about the source of a given row
    
    Returns
    -------
    IdaDataFrame
        result of merging IdaDataFrames

    Examples
    --------
    >>> from nzpyida.sampledata.iris import iris
    >>> from nzpyida.sampledata.iris import iris as iris2
    >>> iris.reset_index()
    >>> iris2.reset_index()
    >>> iris2.columns = ['id', 'sepal_length', 'sepal_width', 'petal_length', \
        'PETAL_WIDTH', 'CLASS']
    >>> iris2['id'] = iris2['id'] + 50
    >>> iris1_ida = idadb.as_idadataframe(iris, 'IRIS_TEST1', indexer='index')
    >>> iris2_ida = idadb.as_idadataframe(iris2, 'IRIS_TEST2', indexer='id')
    >>> iris_merge = nzpyida.merge(iris1_ida, iris2_ida, left_on='index', right_on='id')
    >>> iris_merge.tail()
        index 	sepal_length_x 	sepal_width_x 	petal_length_x 	petal_width 	species     id 	sepal_length_y 	sepal_width_y 	petal_length_y 	PETAL_WIDTH     CLASS												
    95 	145 	6.7 	        3.0 	        5.2 	        2.3 	        virginica   145 5.7 	        3.0 	        4.2 	        1.2 	        versicolor
    96 	146 	6.3 	        2.5 	        5.0 	        1.9 	        virginica   146 5.7 	        2.9 	        4.2 	        1.3 	        versicolor
    97 	147 	6.5 	        3.0 	        5.2 	        2.0 	        virginica   147 6.2 	        2.9 	        4.3 	        1.3 	        versicolor
    98 	148 	6.2 	        3.4 	        5.4 	        2.3 	        virginica   148 5.1 	        2.5 	        3.0 	        1.1 	        versicolor
    99 	149 	5.9 	        3.0 	        5.1 	        1.8 	        virginica   149 5.7 	        2.8 	        4.1 	        1.3 	        versicolor
    >>> len(iris_merge)
    100
    >>> iris_merge = nzpyida.merge(iris1_ida, iris2_ida, left_on='index', right_index=True, 
                           how='outer', indicator=True, suffixes=("_a", "_b"))
    >>> iris_merge.head()
        index 	sepal_length_a 	sepal_width_a 	petal_length_a 	petal_width 	species 	id 	sepal_length_b 	sepal_width_b 	petal_length_b 	PETAL_WIDTH CLASS 	INDICATOR
    0 	None 	None 	        None 	        None 	        None 	        None 	        152 	7.1 	        3.0 	        5.9 	        2.1         virginica 	right_only
    1 	None 	None 	        None 	        None 	        None 	        None 	        151 	5.8 	        2.7 	        5.1 	        1.9         virginica 	right_only
    2 	None 	None 	        None 	        None 	        None 	        None 	        154 	6.5 	        3.0 	        5.8 	        2.2         virginica 	right_only
    3 	None 	None 	        None 	        None 	        None 	        None 	        153 	6.3 	        2.9 	        5.6 	        1.8         virginica 	right_only
    4 	None 	None 	        None 	        None 	        None 	        None 	        150 	6.3 	        3.3 	        6.0 	        2.5         virginica 	right_only
    """
    idadb = left._idadb
    suffixes = [suffix if suffix else "" for suffix in suffixes ]
    available_join_types = ["inner", "left", "right", "outer", "cross"]
    common_columns =[col for col in left.columns if col in right.columns]
    on_query = ""
    left_indexer = None
    right_indexer = None

    if how not in available_join_types:
        raise ValueError(f"Invalid value in 'how', should be one of: {available_join_types}")


    if not on and not any([left_on, right_on, left_index, right_index]):
        if how != "cross":
            on = common_columns
            if not on:
                raise ValueError("No common columns to perform merge on.", 
                                 "Merge options: left_on=None, right_on=None,", 
                                 "left_index=False, right_index=False")
    if on:
        if isinstance(on, str):
            if not on in left.columns:
                raise KeyError(f"No column {on} in {left.name} dataframe")
            if not on in right.columns:
                raise KeyError(f"No column {on} in {right.name} dataframe")
            on_query = f" using ({q(on)})"
            left_indexer = on
            right_indexer = on
            on = [on]
        else:
            if not all(on_col in left.columns for on_col in on):
                raise KeyError(f"Not all on columns {on} in {left.name} dataframe")
            if not all(on_col in right.columns for on_col in on):
                raise KeyError(f"Not all on columns {on} in {right.name} dataframe")
            if len(on) == 1:
                on_query = f" using ({q(on[0])})"
            else:
                on_queries = [f"left_table.{q(on_col)} = right_table.{q(on_col)}" 
                              for on_col in on]
                on_query = " on " + " and ".join(on_queries)
            
        

    elif any([left_on, right_on, left_index, right_index]):
        if how == "cross":
            raise ValueError("Can not pass on, right_on, left_on or set right_index=True or left_index=True")
        if left_on:
            if isinstance(left_on, str):
                if not left_on in left.columns:
                    raise KeyError(f"No column {left_on} in {left.name} dataframe")
                left_on = [left_on]
            elif not all(left_on_col in left.columns for left_on_col in left_on):
                raise KeyError(f"Not all columns {left_on} in {left.name} dataframe")
        if right_on:
            if isinstance(right_on, str):
                if not right_on in right.columns:
                    raise KeyError(f"No column {right_on} in {right.name} dataframe")
                right_on = [right_on]
            elif not all(right_on_col in right.columns for right_on_col in right_on):
                raise KeyError(f"Not all columns {right_on} in {right.name} dataframe")
        if left_on and len(left_on) > 1:
            if not right_on or len(right_on) != len(left_on):
                raise ValueError("len(right_on) must equal len(left_on)")
        if right_on and len(right_on) > 1:
            if not left_on or len(left_on) != len(right_on):
                raise ValueError("len(right_on) must equal len(left_on)")
        if (left_on or left_index) and not (right_on or right_index):
            raise ValueError('Must pass "right_on" OR "right_index"')
        if (right_on or right_index) and not (left_on or left_index):
            raise ValueError('Must pass "left_on" OR "left_index"')
        if left_on and left_index:
            raise ValueError('Can only pass argument "left_on" OR "left_index" not both')
        if right_on and right_index:
            raise ValueError('Can only pass argument "right_on" OR "right_index" not both')
        if left_index and not left.indexer:
            raise ValueError(f"'left_index' set to True, but {left.name} has no indexer")
        if right_index and not right.indexer:
            raise ValueError(f"'right_index' set to True, but {right.name} has no indexer")
        if not left_on or len(left_on) == 1:
            left_indexer = left_on[0] if left_on else left.indexer
            right_indexer = right_on[0] if right_on else right.indexer
            if left_indexer == right_indexer:
                on = left_indexer
                on_query = f" using ({q(on)})"
                on = [on]
            else:
                on_query = f" on left_table.{q(left_indexer)} = right_table.{q(right_indexer)}"
        else:
            on_queries = [f"left_table.{q(left_on[i])} = right_table.{q(right_on[i])}" 
                          for i in range(len(left_on))]
            on_query = " on " + " and ".join(on_queries)
        
    lcols = [f"left_table.{q(lcol)}" if lcol not in common_columns 
            else f"left_table.{q(lcol)} AS {q(lcol + suffixes[0])}" 
            for lcol in left.columns]
    rcols = [f"right_table.{q(rcol)}" if rcol not in common_columns 
            else f"right_table.{q(rcol)} AS {q(rcol + suffixes[1])}" 
            for rcol in right.columns]
    nvl_statement = ""
    if on and len(on) == 1: 
        lcols.remove(f"left_table.{q(on[0])} AS {q(on[0] + suffixes[0])}")
        rcols.remove(f"right_table.{q(on[0])} AS {q(on[0] + suffixes[1])}") 
        all_cols = [q(on[0])] + lcols + rcols
    else:  
        if on:
            for on_col in on:
                lcols.remove(f"left_table.{q(on_col)} AS {q(on_col + suffixes[0])}")
                rcols.remove(f"right_table.{q(on_col)} AS {q(on_col + suffixes[1])}")
                nvl_statement += f" nvl(left_table.{q(on_col)},right_table.{q(on_col)}) AS {q(on_col)}, "
        elif left_on and len(left_on) > 1:
            for i in range(len(left_on)):
                if left_on[i] == right_on[i]:
                    lcols.remove(f"left_table.{q(left_on[i])} AS {q(left_on[i] + suffixes[0])}")
                    rcols.remove(f"right_table.{q(right_on[i])} AS {q(right_on[i] + suffixes[1])}")
                    nvl_statement += f" nvl(left_table.{q(left_on[i])},right_table.{q(right_on[i])}) AS {q(left_on[i])}, "
        all_cols = lcols + rcols
    cols = ", ".join(all_cols)

    join_type = {
        "inner": "inner",
        "left": "left outer",
        "right": "right outer",
        "outer": "full outer",
        "cross": "cross"
    }
    if indicator:
        case_statement = ", case when t1=1 and t2=1 then 'both' when t1=1 then 'left_only'" + \
            "else 'right_only' end as indicator"
        select_statement1 = "(select 1 as t1, * from ("
        select_statement2 = "(select 1 as t2, * from ("
        as_statement1 = ") as lt) as left_table "
        as_statement2 = ") as rt) as right_table "
    else:
        case_statement = ""
        select_statement1 = "("
        select_statement2 = "("
        as_statement1 = ") as left_table"
        as_statement2 = ") as right_table"
    query = f"select {nvl_statement} {cols} {case_statement} from" + \
        f"{select_statement1} ({left.internal_state.get_state()}){as_statement1}" + \
        f" {join_type[how]} join {select_statement2}" + \
        f"{right.internal_state.get_state()}{as_statement2}" + on_query

    if how == 'right':
        idx = right_indexer
    elif how == "cross" or how == "outer":
        idx = None
    else:
        idx = left_indexer
    idadf = nzpyida.IdaDataFrame(idadb, left.tablename, indexer=idx)
    idadf.internal_state._views.append(query)
    return idadf

        