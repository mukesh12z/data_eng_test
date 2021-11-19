import run
from run import prepare_data, run_independent_sql
import os
import pathlib
from config import logger
import asyncio
import pytest
from config import cursor

# test function to check if file contains tmp and prepare dictionary 
def test_prepare_data_true():
    path = pathlib.Path('tmp')
    
    if os.path.isdir(path):
        logger.info('tmp dir exists')
    else:
        logger.info('tmp doesnt exist')
        path.mkdir(parents=True, exist_ok=True)
        
        with open('tmp/one.sql','w+') as f: 
            logger.info('writing file'+str(f))
            f.write('tmp.two.sql')
    
    files = pathlib.Path('tmp')
    dict_dependency = {}
    
    logger.info('dict1-'+str(dict_dependency))
    dict1 = prepare_data(files, dict_dependency)
    logger.info('dict2-'+str(dict1))
    
    assert dict1 == {'one.sql': ['tmp.two']}

@pytest.mark.asyncio
async def test_run_independent_sql():
    path = pathlib.Path('tmp')
    dependent_dict = {}
    sql = 'select * from tmp.dummy'
    await run_independent_sql(cursor, sql, path)

    # here need to connect the table and check if the table created or any other check to validate
    # assert the validated value
    assert True