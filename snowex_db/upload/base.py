from snowexsql.db import get_db
from snowexsql.tables import Instrument, Campaign, Observer, DOI, MeasurementType, Site


class BaseUpload:
    @staticmethod
    def _check_or_add_object(session, clz, check_kwargs, object_kwargs=None):
        """
        Check for existing object, add to the database if not found

        Args:
            session: database session
            clz: class to add to database
            check_kwargs: kwargs for checking if the class exists
            object_kwargs: kwargs for instantiating the object
        """

        # Check if the object exists
        obj = session.query(clz).filter_by(**check_kwargs).first()
        if not obj:
            # Use check kwargs if not object_kwargs given
            object_kwargs = object_kwargs or check_kwargs
            # If the object does not exist, create it
            obj = clz(**object_kwargs)
            session.add(obj)
            session.commit()
        return obj

    @classmethod
    def _add_entry(cls, **kwargs):
        raise NotImplemented("You need this")
