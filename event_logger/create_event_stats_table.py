import sqlite3

conn = sqlite3.connect('event_stats.sqlite') # Create SQLite database 

c = conn.cursor() 

# Create Table 
c.execute('''
          CREATE TABLE event_stats
          (id INTEGER PRIMARY KEY ASC, 
           message_info VARCHAR(255) NOT NULL,
           message_code VARCHAR(10) NOT NULL,
           last_updated VARCHAR(100) NOT NULL)
          ''')

conn.commit()
conn.close()