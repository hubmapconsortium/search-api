import abc


class TranslatorInterface(metaclass=abc.ABCMeta):
    @classmethod
    def __subclasshook__(cls, subclass):
        return (hasattr(subclass, 'translate_all') and
                callable(subclass.translate_all) and
                hasattr(subclass, 'translate') and
                callable(subclass.translate) and
                hasattr(subclass, 'delete') and
                callable(subclass.delete) and
                hasattr(subclass, 'index_public_collection') and
                callable(subclass.index_public_collection) and
                hasattr(subclass, 'index_upload') and
                callable(subclass.index_upload) and
                hasattr(subclass, 'index_tree') and
                callable(subclass.index_tree))

    @abc.abstractmethod
    def translate_all(self):
        """Live reindex of all docs"""
        raise NotImplementedError

    @abc.abstractmethod
    def translate(self, entity_id):
        """Reindex a single document"""
        raise NotImplementedError

    @abc.abstractmethod
    def delete(self, entity_id):
        """Used by app.py reindex_all_uuids() for Live reindex all """
        raise NotImplementedError

    @abc.abstractmethod
    def translate_public_collection(self, entity_id, reindex):
        """Used by app.py reindex_all_uuids() for Live reindex all """
        raise NotImplementedError


    @abc.abstractmethod
    def translate_upload(self, entity_id, reindex):
        """Used by app.py reindex_all_uuids() for Live reindex all """
        raise NotImplementedError


    @abc.abstractmethod
    def translate_tree(self, entity_id):
        """Used by app.py reindex_all_uuids() for Live reindex all """
        raise NotImplementedError
