from django.db import migrations

from dbviews.views import DbMaterializedView, DbView


def create_view(view, schema_editor, view_query):
    """
    This function is used to create a view in database
    """
    view_sql = f"CREATE VIEW {view._meta.db_table} AS {view_query}"
    schema_editor.execute(view_sql)


def drop_view(view, schema_editor):
    """
    This function is used to delete a view from database
    """
    view_sql = f"DROP VIEW IF EXISTS {view._meta.db_table}"
    schema_editor.execute(view_sql)


def create_materialized_view(view, schema_editor, view_query):
    """
    This function is used to create a materialized view in database
    """
    view_sql = f"CREATE MATERIALIZED VIEW {view._meta.db_table} AS {view_query}"
    schema_editor.execute(view_sql)


def drop_materialized_view(view, schema_editor):
    """
    This function is used to delete a materialized view from database
    """
    view_sql = f"DROP MATERIALIZED VIEW IF EXISTS {view._meta.db_table}"
    schema_editor.execute(view_sql)


class CreateView(migrations.CreateModel):
    """
    This operation is used to create view in database
    """

    def __init__(self, name, fields, options=None, bases=None, managers=None) -> None:
        self.bases = (DbView,)
        super().__init__(name, fields, options, bases, managers)

    def deconstruct(self):
        output = super().deconstruct()
        output[-1]["bases"] = self.bases
        return output

    def database_forwards(self, app_label, schema_editor, from_state, to_state):
        view = to_state.apps.get_model(app_label, self.name)

        if self.allow_migrate_model(schema_editor.connection.alias, view):
            create_view(view, schema_editor, view._meta.get_field("view_query").query)

    def database_backwards(self, app_label, schema_editor, from_state, to_state):
        view = from_state.apps.get_model(app_label, self.name)

        if self.allow_migrate_model(schema_editor.connection.alias, view):
            drop_view(view, schema_editor)

    def describe(self) -> str:
        return f"Create view {self.name}"


class DeleteView(migrations.DeleteModel):
    """
    This operation is used to delete a view from database
    """

    def database_forwards(self, app_label, schema_editor, from_state, to_state):
        model = from_state.apps.get_model(app_label, self.name)

        if self.allow_migrate_model(schema_editor.connection.alias, model):
            drop_view(model, schema_editor)

    def database_backwards(self, app_label, schema_editor, from_state, to_state):
        view = to_state.apps.get_model(app_label, self.name)
        if self.allow_migrate_model(schema_editor.connection.alias, view):
            create_view(view, schema_editor, view._meta.get_field("view_query").query)

    def describe(self) -> str:
        return f"Delete view {self.name}"

    @property
    def migration_name_fragment(self):
        return f"delete_{self.name_lower}"


class AlterView(migrations.CreateModel):
    """
    This operation is used to update a view in database
    """

    def __init__(self, name, fields, options=None, bases=None, managers=None) -> None:
        self.bases = (DbView,)
        super().__init__(name, fields, options, bases, managers)

    def deconstruct(self):
        output = super().deconstruct()
        output[-1]["bases"] = self.bases
        return output

    def database_forwards(self, app_label, schema_editor, from_state, to_state):
        view = to_state.apps.get_model(app_label, self.name)

        if self.allow_migrate_model(schema_editor.connection.alias, view):
            drop_view(view, schema_editor)
            create_view(view, schema_editor, view._meta.get_field("view_query").query)

    def database_backwards(self, app_label, schema_editor, from_state, to_state):
        view = to_state.apps.get_model(app_label, self.name)

        if self.allow_migrate_model(schema_editor.connection.alias, view):
            drop_view(view, schema_editor)
            create_view(view, schema_editor, view._meta.get_field("view_query").query)

    def describe(self) -> str:
        return f"Alter view {self.name}"

    @property
    def migration_name_fragment(self):
        return f"alter_{self.name_lower}"


class CreateMaterializedView(migrations.CreateModel):
    """
    This operation is used to create a materialized view in database
    """

    def __init__(self, name, fields, options=None, bases=None, managers=None) -> None:
        self.bases = (DbMaterializedView,)
        super().__init__(name, fields, options, bases, managers)

    def deconstruct(self):
        output = super().deconstruct()
        output[-1]["bases"] = self.bases
        return output

    def database_forwards(self, app_label, schema_editor, from_state, to_state):
        view = to_state.apps.get_model(app_label, self.name)

        if self.allow_migrate_model(schema_editor.connection.alias, view):
            create_materialized_view(
                view, schema_editor, view._meta.get_field("view_query").query
            )

    def database_backwards(self, app_label, schema_editor, from_state, to_state):
        view = from_state.apps.get_model(app_label, self.name)

        if self.allow_migrate_model(schema_editor.connection.alias, view):
            drop_materialized_view(view, schema_editor)

    def describe(self) -> str:
        return f"Create materialized view {self.name}"


class DeleteMaterializedView(migrations.DeleteModel):
    """
    This operation is used to delete a materialized view from database
    """

    def database_forwards(self, app_label, schema_editor, from_state, to_state):
        model = from_state.apps.get_model(app_label, self.name)

        if self.allow_migrate_model(schema_editor.connection.alias, model):
            drop_materialized_view(model, schema_editor)

    def database_backwards(self, app_label, schema_editor, from_state, to_state):
        view = to_state.apps.get_model(app_label, self.name)
        if self.allow_migrate_model(schema_editor.connection.alias, view):
            create_materialized_view(
                view, schema_editor, view._meta.get_field("view_query").query
            )

    def describe(self) -> str:
        return f"Delete materialized view {self.name}"

    @property
    def migration_name_fragment(self):
        return f"delete_{self.name_lower}"


class AlterMaterializedView(migrations.CreateModel):
    """
    This operation is used to update a materialized view in database
    """

    def __init__(self, name, fields, options=None, bases=None, managers=None) -> None:
        self.bases = (DbMaterializedView,)
        super().__init__(name, fields, options, bases, managers)

    def deconstruct(self):
        output = super().deconstruct()
        output[-1]["bases"] = self.bases
        return output

    def database_forwards(self, app_label, schema_editor, from_state, to_state):
        view = to_state.apps.get_model(app_label, self.name)

        if self.allow_migrate_model(schema_editor.connection.alias, view):
            drop_materialized_view(view, schema_editor)
            create_materialized_view(
                view, schema_editor, view._meta.get_field("view_query").query
            )

    def database_backwards(self, app_label, schema_editor, from_state, to_state):
        view = to_state.apps.get_model(app_label, self.name)

        if self.allow_migrate_model(schema_editor.connection.alias, view):
            drop_materialized_view(view, schema_editor)
            create_materialized_view(
                view, schema_editor, view._meta.get_field("view_query").query
            )

    def describe(self) -> str:
        return f"Alter materialized view {self.name}"

    @property
    def migration_name_fragment(self):
        return f"alter_{self.name_lower}"
