import sqlite3

conn = sqlite3.connect('bookings.sqlite')

c = conn.cursor()
c.execute('''
          DROP TABLE hotel_room
          ''')

c = conn.cursor()
c.execute('''
          DROP TABLE hotel_activity 
          ''')

conn.commit()
conn.close()
