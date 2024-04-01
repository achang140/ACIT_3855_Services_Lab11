from sqlalchemy import Column, Integer, String, DateTime
from base import Base
import datetime

class HotelRoom(Base):
    """ Hotel Room """

    __tablename__ = "hotel_room"

    id = Column(Integer, primary_key=True)
    hotel_id = Column(String(250), nullable=False)
    customer_id = Column(String(250), nullable=False)
    room_id = Column(String(250), nullable=False)
    room_type = Column(String(250), nullable=False)
    num_of_people = Column(Integer, nullable=False)
    check_in_date = Column(String(100), nullable=False)
    check_out_date = Column(String(100), nullable=False)
    timestamp = Column(String(100), nullable=False)
    date_created = Column(DateTime, nullable=False)
    trace_id = Column(String(250), nullable=False)

    def __init__(self, hotel_id, customer_id, room_id, room_type, num_of_people, check_in_date, check_out_date, timestamp, trace_id):
        """ Initializes a hotel room booking """
        self.hotel_id = hotel_id
        self.customer_id = customer_id
        self.room_id = room_id
        self.room_type = room_type
        self.num_of_people = num_of_people
        self.check_in_date = check_in_date
        self.check_out_date = check_out_date
        self.timestamp = timestamp
        self.date_created = datetime.datetime.now()
        self.trace_id = trace_id

    def to_dict(self):
        """ Dictionary Representation of a hotel room booking """
        dict = {} 
        # dict["id"] = self.id 
        dict["hotel_id"] = self.hotel_id
        dict["customer_id"] = self.customer_id
        dict["room_id"] = self.room_id
        dict["room_type"] = self.room_type
        dict["num_of_people"] = self.num_of_people
        dict["check_in_date"] = self.check_in_date
        dict["check_out_date"] = self.check_out_date
        dict["timestamp"] = self.timestamp
        # dict["date_created"] = self.date_created
        dict["trace_id"] = self.trace_id

        return dict 