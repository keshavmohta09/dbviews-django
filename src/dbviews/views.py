from typing import Any

from django.db import connection, models

from dbviews.metaclasses import ViewModelMeta


class ViewManager(models.Manager):
    """
    This class is used as a manager for views and materialized views.
    """

    def get_queryset(self):
        queryset = super().get_queryset()
        return queryset.defer("view_query")

    def bulk_create(self, *args, **kwargs):
        raise NotImplementedError

    def create(self, *args, **kwargs):
        raise NotImplementedError

    def get_or_create(self, *args, **kwargs):
        raise NotImplementedError

    def delete(self, *args, **kwargs):
        raise NotImplementedError

    def update(self, *args, **kwargs):
        raise NotImplementedError


class DbView(models.Model, metaclass=ViewModelMeta):
    """
    This class is utilized for creating views in database through inheritance.
    """

    _skip_meta_validations = True

    objects = ViewManager()

    class Meta:
        abstract = True

    def delete(self, *args, **kwargs):
        raise NotImplementedError

    def save(self, *args, **kwargs):
        raise NotImplementedError

    @classmethod
    def get_all_subclasses(cls):
        """
        This method is used to return all the subclasses including nested inherited classes
        """

        def get_subclasses(cls, subclasses):
            new_subclasses = cls.__subclasses__()
            if new_subclasses:
                subclasses.update(new_subclasses)
                for subclass in new_subclasses:
                    get_subclasses(cls=subclass, subclasses=subclasses)

        subclasses = set()
        get_subclasses(cls, subclasses)
        return subclasses

    def __getattribute__(self, name: str) -> Any:
        if name == "view_query":
            raise AttributeError(
                f"'{self.__class__.__name__}' object has no attribute '{name}'"
            )
        return super().__getattribute__(name)

    def __setattr__(self, name: str, value: Any) -> None:
        if name == "view_query":
            return
        return super().__setattr__(name, value)


class DbMaterializedView(models.Model, metaclass=ViewModelMeta):
    """
    This class is utilized for creating materialized views in database through inheritance.
    """

    _skip_meta_validations = True

    objects = ViewManager()

    class Meta:
        abstract = True

    def delete(self, *args, **kwargs):
        raise NotImplementedError

    def save(self, *args, **kwargs):
        raise NotImplementedError

    @classmethod
    def refresh(cls):
        """
        This method is used to refresh the view
        """
        with connection.cursor() as cursor:
            cursor.execute(f"REFRESH MATERIALIZED VIEW {cls._meta.db_table}")

    @classmethod
    def get_all_subclasses(cls):
        """
        This method is used to return all the subclasses including nested inherited classes
        """

        def get_subclasses(cls, subclasses):
            new_subclasses = cls.__subclasses__()
            if new_subclasses:
                subclasses.update(new_subclasses)
                for subclass in new_subclasses:
                    get_subclasses(cls=subclass, subclasses=subclasses)

        subclasses = set()
        get_subclasses(cls, subclasses)
        return subclasses

    def __getattribute__(self, name: str) -> Any:
        if name == "view_query":
            raise AttributeError(
                f"'{self.__class__.__name__}' object has no attribute '{name}'"
            )
        return super().__getattribute__(name)

    def __setattr__(self, name: str, value: Any) -> None:
        if name == "view_query":
            return
        return super().__setattr__(name, value)
