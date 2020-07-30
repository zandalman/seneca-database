#!/usr/bin/env python
# -*- coding:utf-8 -*-

from distutils.version import StrictVersion
from contextlib import contextmanager
from tabulate import tabulate
import json
import datetime
import os
import sqlalchemy
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy import create_engine, ForeignKey, Column, Integer, String, Float, JSON, DateTime, ARRAY
from sqlalchemy.sql import func
from dictalchemy import DictableModel


class Error(Exception):
    """
    Generic error class.

    Args:
        message (str): A human readable string describing the error

    Attributes:
        message (str): A human readable string describing the error
    """
    def __init__(self, message):
        self.message = message


Base = declarative_base(cls=DictableModel)


class Mixin(object):
    """
    Mixin class for shared functionality between classes.

    Args:
        id (int): The primary key used to identify objects in the database.
            Each object of a particular class has a unique id.
        name (str): The name of the object.
        time: A Python DateTime object representing the time that the object was created or last updated
        version (int): An integer which increments each time an object is updated.

    Attributes:
        id (int): The primary key used to identify objects in the database.
            Each object of a particular class has a unique id.
        name (str): The name of the object.
        time: A Python DateTime object representing the time that the object was created or last updated
        version (int): An integer which increments each time an object is updated.
    """
    id = Column(Integer, primary_key=True)
    name = Column(String)
    time = Column(DateTime)
    version = Column(Integer)

    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()
    __table_args__ = {'mysql_engine': 'SQLite', 'extend_existing': True}


class Gateware(Mixin, Base):
    """
    Gateware class.

    Related to the class DeviceDB by a one-to-one relationship.

    Args:
        path (str): The path to the gateware file.
        filename (str) - The name of the gateware file.
        EEM_connections (list): A list of the EEM cable connections associated with the gateware.
        devicedb: The device database associated with the gateware.

    Attributes:
        path (str): The path to the gateware file.
        filename (str) - The name of the gateware file.
        EEM_connections (list): A list of the EEM cable connections associated with the gateware.
        devicedb: The DeviceDB object associated with the gateware.
    """
    path = Column(String)
    filename = Column(String)
    EEM_connections = ARRAY(String)
    devicedb = relationship("DeviceDB", back_populates="gateware", uselist=False, cascade="all, delete, delete-orphan")

    def __repr__(self):
        return "<Gateware(name='%s')>" % self.name


class DeviceDB(Mixin, Base):
    """
    Device database class.

    Related to the class Gateware by a one-to-one relationship.
    Related to the class Project by a one-to-many relationship.

    Args:
        path (str): The path to the device database file.
        filename (str): The name of the device database file.
        gateware_id (int): The id of the gateware associated with the device database.
        gateware: The Gateware object associated with the device database.
        projects: A list of Project objects associated with the device database.

    Attributes:
        path (str): The path to the device database file.
        filename (str): The name of the device database file.
        gateware_id (int): The id of the gateware associated with the device database.
        gateware: The Gateware object associated with the device database.
        projects: A list of Project objects associated with the device database.
    """
    path = Column(String)
    filename = Column(String)
    gateware_id = Column(Integer, ForeignKey('gateware.id'))
    gateware = relationship("Gateware", back_populates="devicedb")
    projects = relationship("Project", back_populates="devicedb", cascade="all, delete, delete-orphan")

    def __repr__(self):
        try:
            numprojects = len(self.projects)
        except AttributeError:
            numprojects = 0
        return "<DeviceDB(name='%s', numprojects=%d)>" % (self.name, numprojects)


class Project(Mixin, Base):
    """
    Project class.

    Related to the class DeviceDB by a many-to-one relationship.
    Related to the class Pipeline by a one-to-many relationship.

    Args:
        description (str): A description of the project.
        devicedb_id (int): The id of the DeviceDB associated with the project.
        devicedb: The DeviceDB object associated with the project.
        pipelines: The Pipeline objects associated with the project.

    Attributes:
        description (str): A description of the project.
        devicedb_id (int): The id of the DeviceDB associated with the project.
        devicedb: The DeviceDB object associated with the project.
        pipelines: The Pipeline objects associated with the project.
    """
    description = Column(String)
    devicedb_id = Column(Integer, ForeignKey('devicedb.id'))
    devicedb = relationship("DeviceDB", back_populates="projects")
    pipelines = relationship("Pipeline", back_populates="project", cascade="all, delete, delete-orphan")

    def __repr__(self):
        try:
            numsequences = len(self.sequences)
        except AttributeError:
            numsequences = 0
        return "<Project(name='%s', devicedb='%s', numpipelines='%d')>" % (self.name, self.devicedb.name, len(self.pipelines))



class Pipeline(Mixin, Base):
    """
    Pipeline class.

    Related to the class Project by a many-to-one relationship.
    Related to the class Sequence by a one-to-many relationship.

    Args:
        description (str): A description of the pipeline.
        ordering (dict): The parallel/series sequence ordering in JSON format.
        project_id (int): The id of the Project associated with the pipeline.
        project: The Project object associated with the pipeline.
        sequences: The Sequence objects associated with the pipeline.

    Attributes:
        description (str): A description of the pipeline.
        ordering (dict): The parallel/series sequence ordering in JSON format.
        project_id (int): The id of the Project associated with the pipeline.
        project: The Project object associated with the pipeline.
        sequences: The Sequence objects associated with the pipeline.
    """
    description = Column(String)
    ordering = Column(JSON)
    project_id = Column(Integer, ForeignKey('project.id'))
    project = relationship("Project", back_populates="pipelines")
    sequences = relationship("Sequence", back_populates="pipeline", cascade="all, delete, delete-orphan")

    def __repr__(self):
        try:
            numsequences = len(self.sequences)
        except AttributeError:
            numsequences = 0
        return "<Pipeline(name='%s', project='%s', numsequences='%d')>" % (self.name, self.project.name, len(self.sequences))


class Sequence(Mixin, Base):
    """
    Experimental sequence class.

    Related to the class Pipeline by a many-to-one relationship.
    Related to the class Measurement by a one-to-many relationship.

    Args:
        path (str): The path to the sequence file.
        filename (str): The name of the sequence file.
        description (str): A description of the experimental sequence.
        params (dict): The sequence parameters in JSON format.
        pipeline_id (int): The id of the Pipeline object associated with the sequence.
        pipeline: The Pipeline object associated with the sequence.
        measurements: A list of Measurement objects associated with the sequence.

    Attributes:
        path (str): The path to the sequence file.
        filename (str): The name of the sequence file.
        description (str): A description of the experimental sequence.
        params (dict): The sequence parameters in JSON format.
        project_id (int): The id of the Pipeline object associated with the sequence.
        project: The Pipeline object associated with the sequence.
        measurements: A list of Measurement objects associated with the sequence.
    """
    path = Column(String)
    filename = Column(String)
    description = Column(String)
    params = Column(JSON)
    pipeline_id = Column(Integer, ForeignKey("pipeline.id"))
    pipeline = relationship("Pipeline", back_populates="sequences")
    measurements = relationship("Measurement", back_populates="sequence", cascade="all, delete, delete-orphan")

    def __repr__(self):
        try:
            nummeasurements = len(self.measurements)
        except AttributeError:
            nummeasurements = 0
        return "<Sequence(name='%s', pipeline='%s', nummeasurements=%d)>" % (self.name, self.pipeline.name, len(self.measurements))


class Measurement(Mixin, Base):
    """
    Measurement class.

    Related to the class Sequence by a many-to-one relationship.

    Args:
        path_csv (str): The path to a csv file associated with the measurement.
        filename_csv (str): The name of a csv file associated with the measurement.
        path_jpg (str): The path to a jpg file associated with the measurement.
        filename_jpg (str): The name of a jpg file associated with the measurement.
        path_HDF5 (str): The path to a HDF5 file associated with the measurement.
        filename_HDF5 (str): The name of a HDF5 file associated with the measurement.
        sequence_id (int): The id of the Sequence object associated with the measurement.
        sequence: The sequence object associated with the measurement.

    Attributes:
        path_csv (str): The path to a csv file associated with the measurement.
        filename_csv (str): The name of a csv file associated with the measurement.
        path_jpg (str): The path to a jpg file associated with the measurement.
        filename_jpg (str): The name of a jpg file associated with the measurement.
        path_HDF5 (str): The path to a HDF5 file associated with the measurement.
        filename_HDF5 (str): The name of a HDF5 file associated with the measurement.
        sequence_id (int): The id of the Sequence object associated with the measurement.
        sequence: The sequence object associated with the measurement.
    """
    path_csv = Column(String)
    filename_csv = Column(String)
    path_jpg = Column(String)
    filename_jpg = Column(String)
    path_HDF5 = Column(String)
    filename_HDF5 = Column(String)
    sequence_id = Column(Integer, ForeignKey("sequence.id"))
    sequence = relationship("Sequence", back_populates="measurements")

    def __repr__(self):
        return "<Measurement(sequence='%s', project='%s')>" % (self.sequence.name, self.sequence.project.name)
