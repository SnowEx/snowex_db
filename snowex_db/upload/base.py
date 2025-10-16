from dataclasses import dataclass

@dataclass
class CacheObject:
    key: str
    id: int


class BaseUpload:
    def __init__(self):
        # Lookup cache for inserting
        self._lookup_cache = {}

    def _check_or_add_object(self, session, clz, check_kwargs, object_kwargs=None):
        """
        Check for an existing object, add to the database if not found

        Args:
            session: database session
            clz: class to add to database
            check_kwargs: kwargs for checking if the class exists
            object_kwargs: kwargs for instantiating the object
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
        return obj

    @classmethod
    def _add_entry(cls, **kwargs):
        raise NotImplementedError("You need this")
