import logging
import psycopg2 

logger = logging.getLogger('server_logger')
logger.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
fh = logging.FileHandler('99app.log')
fh.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
fh.setFormatter(formatter)
# add the handlers to logger
logger.addHandler(fh)

# postgres configuration
conn = psycopg2.connect(user="postgres", password="admin", host="127.0.0.1", port="5432", database="postgres")
cursor = conn.cursor()
