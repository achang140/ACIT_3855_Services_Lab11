from sqlalchemy import Column, Integer, String, DateTime
from base import Base
import datetime

class Stats(Base):
    """ Processing Statistics """
    
    __tablename__ = "stats"

    id = Column(Integer, primary_key=True)
    num_hotel_room_reservations = Column(Integer, nullable=False)
    max_hotel_room_ppl = Column(Integer, nullable=False)
    num_hotel_activity_reservations = Column(Integer, nullable=False)
    max_hotel_activity_ppl = Column(Integer, nullable=False)
    last_updated = Column(DateTime, nullable=False)

    def __init__(self, num_hotel_room_reservations, max_hotel_room_ppl, num_hotel_activity_reservations, max_hotel_activity_ppl, last_updated):
        """ Initializes a processing statistics object """

        self.num_hotel_room_reservations = num_hotel_room_reservations
        self.max_hotel_room_ppl = max_hotel_room_ppl
        self.num_hotel_activity_reservations = num_hotel_activity_reservations
        self.max_hotel_activity_ppl = max_hotel_activity_ppl
        if last_updated is None:
            self.last_updated = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        else:
            self.last_updated = last_updated

    def to_dict(self):
        """ Dictionary Representation of a statistics """
        
        dict = {}
        
        dict["num_hotel_room_reservations"] = self.num_hotel_room_reservations
        dict["max_hotel_room_ppl"] = self.max_hotel_room_ppl
        dict["num_hotel_activity_reservations"] = self.num_hotel_activity_reservations
        dict["max_hotel_activity_ppl"] = self.max_hotel_activity_ppl
        dict["last_updated"] = self.last_updated

        return dict 
