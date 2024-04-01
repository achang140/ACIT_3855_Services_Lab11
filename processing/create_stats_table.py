import sqlite3

conn = sqlite3.connect('stats.sqlite') # Create SQLite database 

c = conn.cursor() 

# Create Table 
c.execute('''
          CREATE TABLE stats
          (id INTEGER PRIMARY KEY ASC, 
           num_hotel_room_reservations INTEGER NOT NULL,
           max_hotel_room_ppl INTEGER NOT NULL,
           num_hotel_activity_reservations INTEGER NOT NULL,
           max_hotel_activity_ppl INTEGER NOT NULL,
           last_updated VARCHAR(100) NOT NULL)
          ''')

conn.commit()
conn.close()