#!/usr/bin/env python
# -*- coding:utf-8 -*-

# import classess
from classes import *


@contextmanager
def session_scope(Session):
    """
    Automatically manage sessions.

    Args:
        Session: SQLAlchemy sessionmaker object.

    Yields:
        SQLAlchemy session object.

    Examples:
        >>> with session_scope(Session) as session:
        ...     session.add(Gateware())
    """
    session = Session()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


def clear_database(engine):
    """
    Clear all objects from the database

    Args:
        engine: SQLAlchemy engine object.
    """
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)


obj_types = {"gateware": Gateware, "devicedb": DeviceDB, "project": Project, "pipeline": Pipeline, "sequence": Sequence, "measurement": Measurement}


def search_objects(session, obj_type, **kwargs):
    """
    Search for objects in the database by attributes

    Args:
        session: SQLAlchemy session object.
        obj_type (str): Class of objects included in the search.
            Options are "gateware", "devicedb", "project", "sequence", "measurement".
        **kwargs: Search filters.

    Returns:
        List of objects in the database that meet the search criteria.
        The list will be empty if no objects meet the search criteria.

    Examples:
        >>> search_objects(session, "gateware", name="random gateware")
        []
    """
    query = session.query(obj_types[obj_type]).order_by(obj_types[obj_type].id)
    for attr in kwargs.keys():
        query = query.filter(getattr(obj_types[obj_type], attr) == kwargs[attr])
    objs = query.all()
    return objs


def get_object_by_id(session, obj_type, id):
    """
    Return an object in the database by id.

    Args:
        session: SQLAlchemy session object.
        obj_type (str): Class of the object to return.
            Options are "gateware", "devicedb", "project", "sequence", "measurement".
        id (int): Id of the object to return.

    Returns:
        Object in the database with the given id.

    Raises:
        MultipleResultsFound: More than one object of given class in database has the given id.
        NoResultsFound: No object of given class in database has the given id.
    """
    obj = session.query(obj_types[obj_type]).filter(obj_types[obj_type].id == id).one()
    return obj


def add_object(session, obj_type, duplicates=False, **kwargs):
    """
    Add an object to database.

    Returns the id of the added object, which can be used to retrieve the object from the database.
    It is recommended to add objects into the database from the top down in class hierarchy
    (i.e. add gateware, device databases, projects, sequences, and measurements in that order).
    Objects can be related to other objects by specifying "gateware_id", "devicedb_id", "project_id", or "sequence_id" in **kwargs.

    Args:
        session: SQLAlchemy session object.
        obj_type (str): Class of the object to add.
            Options are "gateware", "devicedb", "project", "sequence", "measurement".
        duplicates (bool): Allow duplicate objects to be added. Defaults to False.
            This is useful for adding objects whose attributes will be specified later.
        **kwargs: Attributes of the object to add.

    Returns:
        int: Id of the added object. Returns 0 if unsuccessful.
            Will be unsuccessful if duplicates=False and a duplicate object is detected
            or if the object is related to another object by an invalid id

    Examples:
        >>> add_object(session, "devicedb", name="example device database")
        1
    """
    for obj_type_id in obj_types.keys():
        if (obj_type_id + "_id") in kwargs.keys():
            if len(search_objects(session, obj_type_id, id=kwargs[obj_type_id + "_id"])) == 0:
                print("Warning: %s linked to undefined %s with id %s" % (obj_type, obj_type_id, kwargs[obj_type_id + "_id"]))
                return 0
    if not duplicates:
        if len(search_objects(session, obj_type, version=1, **kwargs)) > 0:
            print("Warning: duplicate %s detected; object not added" % obj_type)
            return 0
    time = datetime.datetime.utcnow()
    new_obj = obj_types[obj_type](version=1, time=time, **kwargs)
    session.add(new_obj)
    obj_id = search_objects(session, obj_type, version=1, time=time, **kwargs)[0].id
    return obj_id


def delete_object(session, obj_type, id):
    """
    Delete an object from the database.

    Args:
        session: SQLAlchemy session object.
        obj_type (string): Class of the object to delete.
            Options are "gateware", "devicedb", "project", "sequence", "measurement".
        id (int): Id of the object to delete.

    Raises:
        MultipleResultsFound: More than one object of given class in database has the given id.
        NoResultsFound: No object of given class in database has the given id.
    """
    obj = get_object_by_id(session, obj_type, id)
    session.delete(obj)


def update_object(session, obj_type, id, update_time=True, **kwargs):
    """
    Update an object's attributes in the database.

    Only attributes of the object in **kwargs will be updated.
    Do not use this function to update the id, version, or time attributes.
    Object relationships can be updated by specifying "gateware_id", "devicedb_id", "project_id", or "sequence_id" in **kwargs.

    Args:
        session: SQLAlchemy session object.
        obj_type (string): Class of the object to update.
            Options are "gateware", "devicedb", "project", "sequence", "measurement".
        id (int): Id of the object to update.
        update_time (bool): Update the object's time attribute with the current time. Defaults to True.
        **kwargs: Attributes of the object to update.

    Raises:
        MultipleResultsFound: More than one object of given class in database has the given id.
        NoResultsFound: No object of given class in database has the given id.
    """
    obj = get_object_by_id(session, obj_type, id)
    for attr in kwargs.keys():
        setattr(obj, attr, kwargs[attr])
    obj.version += 1
    if update_time:
        obj.time = datetime.datetime.utcnow()


def write_data(data, path, outfile):
    """
    Write JSON data to a file.

    Args:
        data (dict): JSON data to write.
        path (str): Path of the file to write.
        outfile (str): File name. Usually a text file.
    """
    outfile_path = os.path.join(path, outfile)
    with open(outfile_path, 'w') as outfile:
        json.dump(data, outfile, default=str)


def write_object_info(session, obj_type, id, outfile, path=os.getcwd()):
    """
    Write all data for an object to a file as JSON.

    Args:
        session: SQLAlchemy session object.
        obj_type (string): Class of the object to write.
            Options are "gateware", "devicedb", "project", "sequence", "measurement".
        id (int): Id of the object to write.
        outfile (str): File name. Usually a text file.
        path (str): Path of the file to write. Defaults to the current directory.

    Raises:
        MultipleResultsFound: More than one object of given class in database has the given id.
        NoResultsFound: No object of given class in database has the given id.
    """
    obj = get_object_by_id(session, obj_type, id)
    write_data(obj.asdict(), path, outfile)


def write_database(session, outfile, path=os.getcwd()):
    """
    Write all data in the database to a file as JSON.

    Args:
        session: SQLAlchemy session object.
        outfile (str): File name. Usually a text file.
        path (str): Path of the file to write. Defaults to the current directory.
    """
    data = {"gateware": []}
    for gateware in search_objects(session, "gateware"):
        data["gateware"].append(gateware.asdict())
        devicedb = search_objects(session, "devicedb", gateware_id=gateware.id)
        data["gateware"][-1]["devicedb"] = devicedb.asdict()
        data["gateware"][-1]["devicedb"]["projects"] = []
        for project in search_objects(session, "project", devicedb_id=devicedb.id):
            data["gateware"][-1]["devicedb"]["projects"].append(project.asdict())
            data["gateware"][-1]["devicedb"]["projects"][-1]["pipelines"] = []
            for pipeline in search_objects(session, "pipeline", project_id=project.id):
                data["gateware"][-1]["devicedb"]["projects"][-1]["pipelines"].append(pipeline.asdict())
                data["gateware"][-1]["devicedb"]["projects"][-1]["pipelines"][-1]["sequences"] = []
                for sequence in search_objects(session, "sequence", pipeline_id=pipeline.id):
                    data["gateware"][-1]["devicedb"]["projects"][-1]["pipelines"][-1]["sequences"].append(sequence.asdict())
                    data["gateware"][-1]["devicedb"]["projects"][-1]["pipelines"][-1]["sequences"][-1]["measurements"] = []
                    for measurement in search_objects(session, "measurement", sequence_id=sequence.id):
                        data["gateware"][-1]["devicedb"]["projects"][-1]["pipelines"][-1]["sequences"][-1]["measurements"].append(measurement.asdict())
    if data == {"gateware": []}:
        print("Warning: database is empty")
    write_data(data, path, outfile)


def print_info(session, detailed=False):
    """
    Prints the database information in a human readable way. Mostly for debugging purposes.

    Args:
        session: SQLAlchemy session object.
        detailed (bool): Print more detailed database information. Defaults to False.
    """
    if detailed:
        for proj in session.query(Project).order_by(Project.id):
            print("\nProject: %s\nDeviceDB: %s\nGateware: %s\nDescription: %s" % (proj.name, proj.devicedb.name, proj.devicedb.gateware.name, proj.description))
            table = []
            mes_cnt_query = session.query(Measurement.sequence_id, func.count('*').label('measurement_count')).group_by(Measurement.sequence_id).subquery()
            for seq, mes_cnt in session.query(Sequence, mes_cnt_query.c.measurement_count).outerjoin(mes_cnt_query, Sequence.id == mes_cnt_query.c.sequence_id).filter(Sequence.pipeline.project == proj).order_by(Sequence.id):
                table.append([seq.name, mes_cnt])
            print(tabulate(table, headers=['Sequence', 'Measurements']))
    else:
        table = []
        seq_cnt_query = session.query(Sequence.pipeline.project_id, func.count('*').label('sequence_count')).group_by(Sequence.pipeline.project_id).subquery()
        for proj, seq_cnt in session.query(Project, seq_cnt_query.c.sequence_count).outerjoin(seq_cnt_query, Project.id == seq_cnt_query.c.project_id).order_by(Project.id):
            table.append([proj.name, proj.devicedb.name, proj.devicedb.gateware.name, seq_cnt])
        print(tabulate(table, headers=['Project', 'Device Database', 'Gateware', 'Sequences']))


def init_db(name="test.db", memory=False, echo=False):
    """
    Initialize the database.

    Args:
        name (str): The filename of the database. Can also be a path. Ignored if memory == True. Defaults to "test.db."
        memory (bool): Create an in-memory only database. Defaults to False.
        echo (bool): Print all generated SQL. Defaults to False.
    """
    if memory:
        name = ":memory:"
    engine = create_engine('sqlite:///%s' % name, echo=echo)
    Session = sessionmaker(bind=engine)
    Base.metadata.create_all(engine)
    return Session

