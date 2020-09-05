#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import getopt

import pandas as pd

from sqlalchemy import create_engine

if __name__ == '__main__':
	
	unix_options = 's:e:'
	gnu_options = ['start_dt=', 'end_dt=']

	full_cmd_arg = sys.argv
	arg_list = full_cmd_arg[1:]

	try:
		arguments, values = getopt.getopt(arg_list,
		                                  unix_options,
		                                  gnu_options)
	except getopt.error as err:
		print(str(err))
		sys.exit(2)
    
	start_dt = ''
	end_dt = ''
	for current_arg, current_val in arguments:
		if current_arg in ('-s', '--start_dt'):
			start_dt = current_val
		if current_arg in ('-e', '--end_dt'):
			end_dt = current_val

	db_config = {'user': 'my_user',
	             'pwd': 'my_user_password',
	             'host': 'localhost',
	             'port': 5432,
	             'db': 'zen'}

	connection_string = 'postgresql://{}:{}@{}:{}/{}'\
	                    .format(db_config['user'],
	                     	     db_config['pwd'],
	                     	     db_config['host'],
	                     	     db_config['port'],
	                     	     db_config['db'])

	engine = create_engine(connection_string)

	query = '''
	            SELECT event_id,        
		               age_segment,
		               event,
		               item_id,    
		               item_topic,
		               item_type,    
		               source_id,     
		               source_topic,    
		               source_type,     
		               TO_TIMESTAMP(ts / 1000) AT TIME ZONE 'Etc/UTC' AS dt,    
		               user_id

	            FROM log_raw

	            WHERE TO_TIMESTAMP(ts / 1000) AT TIME ZONE 'Etc/UTC' 
	                  BETWEEN '{}'::TIMESTAMP AND
	                          '{}'::TIMESTAMP 
	        '''.format(start_dt, end_dt)

	log_raw = pd.io.sql.read_sql(query, 
		                         con = engine, 
		                         index_col = 'event_id')

	log_raw['dt'] = log_raw['dt'].dt.round('min')

	print('log_raw...done')

	dash_visits = (log_raw.groupby(['item_topic',
	                               'source_topic',
	                               'age_segment',
	                               'dt'])
	                      .agg({'user_id': 'count'})
	                      .reset_index()
	                      .rename(columns={'user_id': 'visits'})
	               )
	query = '''
	           DELETE FROM dash_visits

	           WHERE dt 
	                 BETWEEN '{}'::TIMESTAMP AND
	                         '{}'::TIMESTAMP
	        '''.format(start_dt, end_dt)
	engine.execute(query)
	
	dash_visits.to_sql(name='dash_visits',
		               con=engine,
		               if_exists='append',
		               index=False)

	print('dash_visits...done')
	
	dash_engagement = (log_raw.groupby(['dt',
		                                'item_topic',
		                                'event',
		                                'age_segment'])
		                      .agg({'user_id': 'nunique'})
		                      .reset_index()
		                      .rename(columns={'user_id':
		                      	               'unique_users'})     
	                  )
	query = '''
	           DELETE FROM dash_engagement

	           WHERE dt 
	                 BETWEEN '{}'::TIMESTAMP AND
	                         '{}'::TIMESTAMP
	        '''.format(start_dt, end_dt)
	engine.execute(query)
	
	dash_engagement.to_sql(name='dash_engagement',
		                   con=engine,
		                   if_exists='append',
		                   index=False)
	

	print('dash_engagement...done')
	print('All done.')