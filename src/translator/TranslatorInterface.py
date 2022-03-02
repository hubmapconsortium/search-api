import abc


class TranslatorInterface(metaclass=abc.ABCMeta):
    @classmethod
    def __subclasshook__(cls, subclass):
        return (hasattr(subclass, 'translate_all') and
                callable(subclass.translate_all) and
                hasattr(subclass, 'translate') and
                callable(subclass.translate))

    @abc.abstractmethod
    def translate_all(self):
        """Live reindex of all docs"""
        raise NotImplementedError

    @abc.abstractmethod
    def translate(self, entity_id):
        """Reindex a single document"""
        raise NotImplementedError
