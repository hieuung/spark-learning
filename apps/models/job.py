from sqlalchemy import Column, Integer, Text, DateTime, VARCHAR, Enum
from sqlalchemy.types import JSON
from models import Base


class Job(Base):
    __tablename__ = "job"
    __table_args__ = {'schema': 'lake'}
    
    id = Column(
        'id',
        Integer,
        primary_key=True,
    )
    db_name = Column(
        'db_name',
        VARCHAR,
        nullable=False
    )
    table_name = Column(
        'table_name',
        VARCHAR,
        nullable=False
    )
    action = Column(
        'action',
        VARCHAR,
        nullable=False
    )
    last_row_number = Column(
        'last_row_number',
        Integer,
        nullable=False
    )
    status = Column(
        'status',
        VARCHAR,
        nullable=False
    )
    details = Column(
        'details',
        JSON,
        nullable=False
    )
    job_date = Column(
        'job_date',
        VARCHAR,
        nullable=False
    )
    job_start = Column(
        'job_start',
        DateTime,
        nullable=False
    )
    zone = Column(
        'zone',
        VARCHAR,
        nullable=False
    )
    job_end = Column(
        'job_end',
        DateTime,
        nullable=False
    )

    def __repr__(self):
        return '<Job %r>' % (self.id, )
