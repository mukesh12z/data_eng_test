from pathlib import Path, PurePath
import re
import os
import time
import asyncio
from config import logger, conn, cursor


#Search for tmp.* tables in script to determine dependency
def find_word(f,file_content, to_search):
            
    list_words = re.findall(to_search,file_content)
    return list_words

def prepare_data(files, dict_dependency):
    
    logger.info('prepare data files'+str(files))
    
    for f in files.iterdir():
        f2 = PurePath(f).name
    
        with open(f,"r") as fi:
            x = fi.read()
            search_string = 'tmp.[a-zA-Z_a-zA-Z]*'
            a = find_word(f,x, search_string)
            dict_dependency[f2] = a
    return dict_dependency


#make separate dictionary so as to run independent dictionary in parallel 
def prepare_dictionaries(dict_dependency, dependent_dict, independent_dict):
    
    for k,v in dict_dependency.items():
    
        if len(v) > 0:
            dependent_dict[k] = v
        else:
            independent_dict[k] = v   
            
    return dependent_dict, independent_dict
        
#execute sql in files which can be run independently and asyncronously
async def process_independent_sql(path, independent_dict):
    
    logger.info('running independent sql')
    for k,v in independent_dict.items():
        k = os.path.join(path,k)
        try:
            with open(k,"r") as fi:
                x = fi.read()
                await asyncio.gather(run_independent_sql(cursor, x, path))
        
        except Exception as e:
            logger.error(e)

async def run_independent_sql(cursor, sql, path):
    
    logger.info('independent sql async run')
    #commenting below part as actual sql need not run
    #cursor.execute(sql)
    await asyncio.sleep(2)

#execute sql in files which should be run syncronously 
def process_dependent_sql(path, dependent_dict):
    
    logger.info('running dependent sql')
    
    for k,v in dependent_dict.items():
        k = os.path.join(path,k)
        try:
            with open(k,"r") as fi:
                x = fi.read()
                run_dependent_sql(cursor, x)
    
        except Exception as e:
            logger.error(e)

def run_dependent_sql(cursor, sql):
    
    logger.info('dependent sql sync run')
    
    #commenting below part as actual sql need not run
    #cursor.execute(sql)
    time.sleep(2)
    
async def main():

    logger.info(' program starting ')

    files = Path('sql/tmp')
    logger.info('files-'+str(files))
    
    dict_dependency = {}
    dependent_dict = {}
    independent_dict = {}  

    # get data of which sql files have dependency and which one don't
    dict_dependency = prepare_data(files, dict_dependency)    
    logger.info('Dependencies for files -'+ str(dict_dependency))
    
    
    #get 2 different data structure of dependent and independent sql files. Independent files can be run asyncronously 
    dependent_dict, independent_dict = prepare_dictionaries(dict_dependency, dependent_dict, independent_dict)
    logger.info('dependent dict-'+str(dependent_dict))
    logger.info('independent dict-'+str(independent_dict))
    
    
    #process dependent and independent sql files separately
    await process_independent_sql(files, independent_dict)
    process_dependent_sql(files, dependent_dict)
    
if __name__ == "__main__":
    
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    