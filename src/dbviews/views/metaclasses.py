from copy import copy, deepcopy

from django.db import models

from dbviews.views.fields import QueryField


class ViewModelMeta(models.base.ModelBase):
    """
    This metaclass is used to validate whether the view and
    materialized has `view_query` field
    """

    def __new__(cls, name, bases, attrs, **kwargs):
        new_class = super().__new__(cls, name, bases, attrs, **kwargs)

        if attrs.pop("_skip_meta_validations", False):
            return new_class
        if "view_query" not in attrs:
            raise FileNotFoundError(
                "`view_query` field value is required to create a view and materialized view"
            )

        if not isinstance(attrs["view_query"], QueryField):
            raise TypeError("`view_query` must be instance of `QueryField`")

        if not isinstance(attrs["view_query"].query, str):
            raise TypeError("Provide sql query as query value in `view_query`")

        return new_class
