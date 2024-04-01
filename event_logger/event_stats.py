from sqlalchemy import Column, Integer, String, DateTime
from base import Base
import datetime

class EventStats(Base):
    """ Initializes an EventStats object with the provided message information, message code, and last updated timestamp."""
    
    __tablename__ = "event_stats"

    id = Column(Integer, primary_key=True)
    message_info = Column(String, nullable=False)
    message_code = Column(String, nullable=False)
    last_updated = Column(DateTime, nullable=False)

    def __init__(self, message_info, message_code, last_updated):
        """ Initializes the EventStats object """

        self.message_info = message_info
        self.message_code = message_code
        if last_updated is None:
            self.last_updated = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        else:
            self.last_updated = last_updated

    def to_dict(self):
        """ Dictionary Representation of a statistics """
        
        dict = {}
        
        dict["message_info"] = self.message_info
        dict["message_code"] = self.message_code
        dict["last_updated"] = self.last_updated

        return dict 