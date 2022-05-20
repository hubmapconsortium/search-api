import abc


class TranslatorInterface(metaclass=abc.ABCMeta):
    @classmethod
    def __subclasshook__(cls, subclass):
        return (hasattr(subclass, 'translate_all') and
                callable(subclass.translate_all) and
                hasattr(subclass, 'translate') and
                callable(subclass.translate) and
                hasattr(subclass, 'update') and
                callable(subclass.update) and
                hasattr(subclass, 'add') and
                callable(subclass.add) and
                hasattr(subclass, 'is_public') and
                callable(subclass.is_public))

    @abc.abstractmethod
    def translate_all(self):
        """Live reindex of all docs"""
        raise NotImplementedError

    @abc.abstractmethod
    def translate(self, entity_id):
        """Reindex a single document"""
        raise NotImplementedError

    @abc.abstractmethod
    def update(self, entity_id, document):
        """PUT method to update a document with the passed ID"""
        raise NotImplementedError

    @abc.abstractmethod
    def add(self, entity_id, document):
        """POST method to add a document with the passed ID"""
        raise NotImplementedError

    @abc.abstractmethod
    def is_public(self, document):
        """Returns TRUE or FALSE if a document is public and should be indexed as such"""
        raise NotImplementedError
