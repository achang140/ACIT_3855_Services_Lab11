import sqlite3

conn = sqlite3.connect('event_stats.sqlite')

c = conn.cursor()

c.execute('''
          DROP TABLE event_stats
          ''')

conn.commit()
conn.close()