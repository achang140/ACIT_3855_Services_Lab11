from sqlalchemy import Column, Integer, String, DateTime
from base import Base
import datetime

class HotelActivity(Base):
    """ Hotel Activity """

    __tablename__ = "hotel_activity"

    id = Column(Integer, primary_key=True)
    hotel_id = Column(String(250), nullable=False)
    customer_id = Column(String(250), nullable=False)
    activity_id = Column(String(250), nullable=False)
    activity_name = Column(String(250), nullable=False)
    num_of_people = Column(Integer, nullable=False)
    reservation_date = Column(String(100), nullable=False)
    timestamp = Column(String(100), nullable=False)
    date_created = Column(DateTime, nullable=False)
    trace_id = Column(String(250), nullable=False)

    def __init__(self, hotel_id, customer_id, activity_id, activity_name, num_of_people, reservation_date, timestamp, trace_id):
        """ Initializes a hotel activity booking """
        self.hotel_id = hotel_id
        self.customer_id = customer_id
        self.activity_id = activity_id
        self.activity_name = activity_name
        self.num_of_people = num_of_people
        self.reservation_date = reservation_date
        self.timestamp = timestamp
        self.date_created = datetime.datetime.now()
        self.trace_id = trace_id

    def to_dict(self):
        """ Dictionary Representation of a hotel activity booking """
        dict = {} 
        # dict["id"] = self.id 
        dict["hotel_id"] = self.hotel_id
        dict["customer_id"] = self.customer_id
        dict["activity_id"] = self.activity_id
        dict["activity_name"] = self.activity_name
        dict["num_of_people"] = self.num_of_people
        dict["reservation_date"] = self.reservation_date
        dict["timestamp"] = self.timestamp
        # dict["date_created"] = self.date_created
        dict["trace_id"] = self.trace_id

        return dict 

