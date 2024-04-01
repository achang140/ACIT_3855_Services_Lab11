import sqlite3

conn = sqlite3.connect('bookings.sqlite') # Create SQLite database 

c = conn.cursor() 

# Create Tables 
c.execute('''
          CREATE TABLE hotel_room
          (id INTEGER PRIMARY KEY ASC, 
           hotel_id VARCHAR(250) NOT NULL,
           customer_id VARCHAR(250) NOT NULL,
           room_id VARCHAR(250) NOT NULL,
           room_type VARCHAR(250) NOT NULL,
           num_of_people INTEGER NOT NULL,
           check_in_date VARCHAR(100) NOT NULL,
           check_out_date VARCHAR(100) NOT NULL,
           timestamp VARCHAR(100) NOT NULL,
           date_created VARCHAR(100) NOT NULL,
           trace_id VARCHAR(250) NOT NULL)
          ''')

c.execute('''
          CREATE TABLE hotel_activity
          (id INTEGER PRIMARY KEY ASC, 
           hotel_id VARCHAR(250) NOT NULL,
           customer_id VARCHAR(250) NOT NULL,
           activity_id VARCHAR(250) NOT NULL,
           activity_name VARCHAR(250) NOT NULL,
           num_of_people INTEGER NOT NULL,
           reservation_date VARCHAR(100) NOT NULL,
           timestamp VARCHAR(100) NOT NULL,
           date_created VARCHAR(100) NOT NULL,
           trace_id VARCHAR(250) NOT NULL)
          ''')

conn.commit()
conn.close()