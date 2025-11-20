from dataclasses import dataclass
from snowexsql.tables import Site


@dataclass
class CacheObject:
    key: str
    id: int


class BaseUpload:
    def __init__(self):
        # Lookup cache for inserting
        self._lookup_cache = {}

    def _check_or_add_object(
        self, session, clz, check_kwargs, object_kwargs=None, update=False
    ):
        """
        Check for an existing object, add to the database if not found

        Args:
            session: database session
            clz: class to add to database
            check_kwargs: kwargs for checking if the class exists
            object_kwargs: kwargs for instantiating the object
            update: Update existing record with given attributes
        """

        # Check in lookup cache
        obj = self._lookup_cache.get(str(check_kwargs), None)
        if obj:
            return obj

        # Check in the database
        obj = session.query(clz).filter_by(**check_kwargs).first()

        # Create the object or put it in the cache
        if not obj:
            # Use check kwargs if not object_kwargs given
            object_kwargs = object_kwargs or check_kwargs
            obj = clz(**object_kwargs)
            session.add(obj)
            session.commit()
            self._lookup_cache[str(check_kwargs)] = CacheObject(
                key=str(check_kwargs), id=obj.id
            )
        elif obj and isinstance(obj, Site) and update:
            # Special case for sites when layer data are uploaded through separate
            # CSV files, where one "Summary" file contains most of the details.
            update_args = {
                key: value for key, value in object_kwargs.items() if value is not None
            }
            del update_args["doi"]
            del update_args["campaign"]
            del update_args["observers"]
            session.query(clz).filter_by(**check_kwargs).update(update_args)
            session.commit()
        return obj

    @classmethod
    def _add_entry(cls, **kwargs):
        raise NotImplementedError("You need this")
