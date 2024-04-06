from typing import TYPE_CHECKING, Union

from django.db.models import Field

if TYPE_CHECKING:
    from dbviews import views


class QueryField(Field):
    """
    This field is used to store query of a particular database view or materialized view
    """

    def __init__(self, query):
        self.query = query
        super().__init__()

    def deconstruct(self):
        name, path, args, kwargs = super().deconstruct()
        kwargs["query"] = self.query
        return name, path, args, kwargs

    def to_python(self, value):
        return self.query

    def get_prep_value(self, value):
        raise NotImplementedError("This field should not be used for data operations.")

    def from_db_value(self, value, expression, connection):
        raise NotImplementedError("This field should not be used for data operations.")

    def contribute_to_class(
        self,
        cls: Union["views.DbView", "views.DbMaterializedView"],
        name: str,
        **kwargs,
    ) -> None:

        if name != "view_query":
            raise FileNotFoundError(
                f"Name of field should be `view_query` instead of `{name}`"
            )

        return super().contribute_to_class(cls, name, **kwargs)
