from copy import copy

from dbviews.fields import QueryField
from dbviews.operations import (
    AlterMaterializedView,
    AlterView,
    CreateMaterializedView,
    CreateView,
    DeleteMaterializedView,
    DeleteView,
)
from dbviews.views import DbMaterializedView, DbView
from django.db.migrations.autodetector import (
    MigrationAutodetector as BaseMigrationAutodetector,
)
from django.db.migrations.utils import resolve_relation


class MigrationAutodetector(BaseMigrationAutodetector):
    def _detect_changes(self, convert_apps=None, graph=None):
        """
        NOTE: Rewrite this function to generate views operations

        Return a dict of migration plans which will achieve the
        change from from_state to to_state. The dict has app labels
        as keys and a list of migrations as values.

        The resulting migrations aren't specially named, but the names
        do matter for dependencies inside the set.

        convert_apps is the list of apps to convert to use migrations
        (i.e. to make initial migrations for, in the usual case)

        graph is an optional argument that, if provided, can help improve
        dependency generation and avoid potential circular dependencies.
        """
        # The first phase is generating all the operations for each app
        # and gathering them into a big per-app list.
        # Then go through that list, order it, and split into migrations to
        # resolve dependencies caused by M2Ms and FKs.
        self.generated_operations = {}
        self.altered_indexes = {}
        self.altered_constraints = {}
        self.renamed_fields = {}

        # Prepare some old/new state and model lists, separating
        # proxy models and ignoring unmigrated apps.
        self.old_model_keys = set()
        self.old_proxy_keys = set()
        self.old_unmanaged_keys = set()
        self.new_model_keys = set()
        self.new_proxy_keys = set()
        self.new_unmanaged_keys = set()

        # To store old and new view keys
        self.old_view_keys = set()
        self.new_view_keys = set()
        # Store all database view tables
        self.db_view_tables = [
            f"{view._meta.db_table}" for view in DbView.get_all_subclasses()
        ]

        # To store old and new materialized view keys
        self.old_materialized_view_keys = set()
        self.new_materialized_view_keys = set()
        # Store all database materialized view tables
        self.db_materialized_view_tables = [
            f"{view._meta.db_table}" for view in DbMaterializedView.get_all_subclasses()
        ]

        # Store from and to state model states of views and materialized views
        # and remove from model states
        self.from_state_view_states = {}
        self.to_state_view_states = {}

        for (app_label, model_name), model_state in copy(
            self.from_state.models
        ).items():
            db_table = (
                model_state.options.get("db_table") or f"{app_label}_{model_name}"
            )

            if not model_state.options.get("managed", True):
                self.old_unmanaged_keys.add((app_label, model_name))
            elif app_label not in self.from_state.real_apps:
                if model_state.options.get("proxy"):
                    self.old_proxy_keys.add((app_label, model_name))
                elif DbView in model_state.bases:
                    self.old_view_keys.add((app_label, model_name))
                    self.from_state_view_states[(app_label, model_name)] = (
                        self.from_state.models.pop((app_label, model_name))
                    )
                elif DbMaterializedView in model_state.bases:
                    self.old_materialized_view_keys.add((app_label, model_name))
                    self.from_state_view_states[(app_label, model_name)] = (
                        self.from_state.models.pop((app_label, model_name))
                    )
                else:
                    self.old_model_keys.add((app_label, model_name))

        for (app_label, model_name), model_state in copy(self.to_state.models).items():
            db_table = (
                model_state.options.get("db_table") or f"{app_label}_{model_name}"
            )

            if not model_state.options.get("managed", True):
                self.new_unmanaged_keys.add((app_label, model_name))
            elif app_label not in self.from_state.real_apps or (
                convert_apps and app_label in convert_apps
            ):
                if model_state.options.get("proxy"):
                    self.new_proxy_keys.add((app_label, model_name))
                elif db_table in self.db_view_tables:
                    self.new_view_keys.add((app_label, model_name))
                    self.to_state_view_states[(app_label, model_name)] = (
                        self.to_state.models.pop((app_label, model_name))
                    )
                elif db_table in self.db_materialized_view_tables:
                    self.new_materialized_view_keys.add((app_label, model_name))
                    self.to_state_view_states[(app_label, model_name)] = (
                        self.to_state.models.pop((app_label, model_name))
                    )
                else:
                    self.new_model_keys.add((app_label, model_name))

        self.from_state.resolve_fields_and_relations()
        self.to_state.resolve_fields_and_relations()

        # Renames have to come first
        self.generate_renamed_models()

        # Prepare lists of fields and generate through model map
        self._prepare_field_lists()
        self._generate_through_model_map()

        # Generate non-rename model operations
        self.generate_deleted_models()
        self.generate_created_models()
        self.generate_deleted_proxies()
        self.generate_created_proxies()
        self.generate_altered_options()
        self.generate_altered_managers()
        self.generate_altered_db_table_comment()

        # Generate views operations
        self.generate_deleted_views()
        self.generate_created_views()
        self.generate_altered_views()

        # Generate materialized views operations
        self.generate_deleted_materialized_views()
        self.generate_created_materialized_views()
        self.generate_altered_materialized_views()

        # Create the renamed fields and store them in self.renamed_fields.
        # They are used by create_altered_indexes(), generate_altered_fields(),
        # generate_removed_altered_index/unique_together(), and
        # generate_altered_index/unique_together().
        self.create_renamed_fields()
        # Create the altered indexes and store them in self.altered_indexes.
        # This avoids the same computation in generate_removed_indexes()
        # and generate_added_indexes().
        self.create_altered_indexes()
        self.create_altered_constraints()
        # Generate index removal operations before field is removed
        self.generate_removed_constraints()
        self.generate_removed_indexes()
        # Generate field renaming operations.
        self.generate_renamed_fields()
        self.generate_renamed_indexes()
        # Generate removal of foo together.
        self.generate_removed_altered_unique_together()
        self.generate_removed_altered_index_together()  # RemovedInDjango51Warning.
        # Generate field operations.
        self.generate_removed_fields()
        self.generate_added_fields()
        self.generate_altered_fields()
        self.generate_altered_order_with_respect_to()
        self.generate_altered_unique_together()
        self.generate_altered_index_together()  # RemovedInDjango51Warning.
        self.generate_added_indexes()
        self.generate_added_constraints()
        self.generate_altered_db_table()

        self._sort_migrations()
        self._build_migration_list(graph)
        self._optimize_migrations()

        return self.migrations

    def generate_created_views(self):
        """
        Find all new views and make create operations
        """
        added_views = self.new_view_keys - self.old_view_keys
        all_added_views = sorted(
            added_views, key=self.swappable_first_key, reverse=True
        )
        for app_label, view_name in all_added_views:
            view_state = self.to_state_view_states[app_label, view_name]
            # Gather related fields
            related_fields = {}
            primary_key_rel = None
            for field_name, field in view_state.fields.items():
                if field.remote_field:
                    if field.remote_field.model:
                        if field.primary_key:
                            primary_key_rel = field.remote_field.model
                        elif not field.remote_field.parent_link:
                            related_fields[field_name] = field
                    if getattr(field.remote_field, "through", None):
                        related_fields[field_name] = field

            # Depend on the deletion of any possible proxy version of us
            dependencies = [
                (app_label, view_name, None, False),
            ]
            # Depend on all bases
            for base in view_state.bases:
                if isinstance(base, str) and "." in base:
                    base_app_label, base_name = base.split(".", 1)
                    dependencies.append((base_app_label, base_name, None, True))
                    # Depend on the removal of base fields if the new model has
                    # a field with the same name.
                    old_base_view_state = self.from_state_view_states.get(
                        (base_app_label, base_name)
                    )
                    new_base_view_state = self.to_state_view_states.get(
                        (base_app_label, base_name)
                    )
                    if old_base_view_state and new_base_view_state:
                        removed_base_fields = (
                            set(old_base_view_state.fields)
                            .difference(
                                new_base_view_state.fields,
                            )
                            .intersection(view_state.fields)
                        )
                        for removed_base_field in removed_base_fields:
                            dependencies.append(
                                (base_app_label, base_name, removed_base_field, False)
                            )
            # Depend on the other end of the primary key if it's a relation
            if primary_key_rel:
                dependencies.append(
                    resolve_relation(
                        primary_key_rel,
                        app_label,
                        view_name,
                    )
                    + (None, True)
                )
            # Generate creation operation
            self.add_operation(
                app_label,
                CreateView(
                    name=view_state.name,
                    fields=[
                        d
                        for d in view_state.fields.items()
                        if isinstance(d[1], QueryField)
                    ],
                    options=view_state.options,
                    bases=(DbView,),
                    managers=view_state.managers,
                ),
                dependencies=dependencies,
            )

    def generate_deleted_views(self):
        """
        Find all deleted views make delete operations for them as well.
        """
        deleted_views = self.old_view_keys - self.new_view_keys
        all_deleted_views = sorted(deleted_views)
        for app_label, model_name in all_deleted_views:
            view_state = self.from_state_view_states[app_label, model_name]
            self.add_operation(
                app_label,
                DeleteView(name=view_state.name),
            )

    def generate_altered_views(self):
        """
        Find all views modified by changes in view_query.
        """
        common_views = self.new_view_keys & self.old_view_keys
        for app_label, view_name in common_views:
            old_view_state = self.from_state_view_states[app_label, view_name]
            new_view_state = self.to_state_view_states[app_label, view_name]

            # Return when there is no change in view query
            if (
                old_view_state.get_field("view_query").query
                == new_view_state.get_field("view_query").query
            ):
                return

            # Gather related fields
            related_fields = {}
            primary_key_rel = None
            for field_name, field in new_view_state.fields.items():
                if field.remote_field:
                    if field.remote_field.model:
                        if field.primary_key:
                            primary_key_rel = field.remote_field.model
                        elif not field.remote_field.parent_link:
                            related_fields[field_name] = field
                    if getattr(field.remote_field, "through", None):
                        related_fields[field_name] = field

            # Depend on the deletion of any possible proxy version of us
            dependencies = [
                (app_label, view_name, None, False),
            ]
            # Depend on all bases
            for base in new_view_state.bases:
                if isinstance(base, str) and "." in base:
                    base_app_label, base_name = base.split(".", 1)
                    dependencies.append((base_app_label, base_name, None, True))
                    # Depend on the removal of base fields if the new model has
                    # a field with the same name.
                    old_base_view_state = self.from_state_view_states.get(
                        (base_app_label, base_name)
                    )
                    new_base_view_state = self.to_state_view_states.get(
                        (base_app_label, base_name)
                    )
                    if old_base_view_state and new_base_view_state:
                        removed_base_fields = (
                            set(old_base_view_state.fields)
                            .difference(
                                new_base_view_state.fields,
                            )
                            .intersection(new_view_state.fields)
                        )
                        for removed_base_field in removed_base_fields:
                            dependencies.append(
                                (base_app_label, base_name, removed_base_field, False)
                            )
            # Depend on the other end of the primary key if it's a relation
            if primary_key_rel:
                dependencies.append(
                    resolve_relation(
                        primary_key_rel,
                        app_label,
                        view_name,
                    )
                    + (None, True)
                )
            # Generate creation operation
            self.add_operation(
                app_label,
                AlterView(
                    name=new_view_state.name,
                    fields=[
                        d
                        for d in new_view_state.fields.items()
                        if isinstance(d[1], QueryField)
                    ],
                    options=new_view_state.options,
                    bases=(DbView,),
                    managers=new_view_state.managers,
                ),
                dependencies=dependencies,
            )

    def generate_created_materialized_views(self):
        """
        Find all new materialized views and make create operations
        """
        added_views = self.new_materialized_view_keys - self.old_materialized_view_keys
        all_added_views = sorted(
            added_views, key=self.swappable_first_key, reverse=True
        )
        for app_label, view_name in all_added_views:
            view_state = self.to_state_view_states[app_label, view_name]
            # Gather related fields
            related_fields = {}
            primary_key_rel = None
            for field_name, field in view_state.fields.items():
                if field.remote_field:
                    if field.remote_field.model:
                        if field.primary_key:
                            primary_key_rel = field.remote_field.model
                        elif not field.remote_field.parent_link:
                            related_fields[field_name] = field
                    if getattr(field.remote_field, "through", None):
                        related_fields[field_name] = field

            # Depend on the deletion of any possible proxy version of us
            dependencies = [
                (app_label, view_name, None, False),
            ]
            # Depend on all bases
            for base in view_state.bases:
                if isinstance(base, str) and "." in base:
                    base_app_label, base_name = base.split(".", 1)
                    dependencies.append((base_app_label, base_name, None, True))
                    # Depend on the removal of base fields if the new model has
                    # a field with the same name.
                    old_base_view_state = self.from_state_view_states.get(
                        (base_app_label, base_name)
                    )
                    new_base_view_state = self.to_state_view_states.get(
                        (base_app_label, base_name)
                    )
                    if old_base_view_state and new_base_view_state:
                        removed_base_fields = (
                            set(old_base_view_state.fields)
                            .difference(
                                new_base_view_state.fields,
                            )
                            .intersection(view_state.fields)
                        )
                        for removed_base_field in removed_base_fields:
                            dependencies.append(
                                (base_app_label, base_name, removed_base_field, False)
                            )
            # Depend on the other end of the primary key if it's a relation
            if primary_key_rel:
                dependencies.append(
                    resolve_relation(
                        primary_key_rel,
                        app_label,
                        view_name,
                    )
                    + (None, True)
                )
            # Generate creation operation
            self.add_operation(
                app_label,
                CreateMaterializedView(
                    name=view_state.name,
                    fields=[
                        d
                        for d in view_state.fields.items()
                        if isinstance(d[1], QueryField)
                    ],
                    options=view_state.options,
                    bases=(DbMaterializedView,),
                    managers=view_state.managers,
                ),
                dependencies=dependencies,
            )

    def generate_deleted_materialized_views(self):
        """
        Find all deleted materialized views make delete operations for them as well.
        """
        deleted_views = (
            self.old_materialized_view_keys - self.new_materialized_view_keys
        )
        all_deleted_views = sorted(deleted_views)
        for app_label, model_name in all_deleted_views:
            view_state = self.from_state_view_states[app_label, model_name]
            self.add_operation(
                app_label,
                DeleteMaterializedView(name=view_state.name),
            )

    def generate_altered_materialized_views(self):
        """
        Find all materialized views modified by changes in view_query.
        """
        common_views = self.new_materialized_view_keys & self.old_materialized_view_keys
        for app_label, view_name in common_views:
            old_view_state = self.from_state_view_states[app_label, view_name]
            new_view_state = self.to_state_view_states[app_label, view_name]

            # Return when there is no change in view query
            if (
                old_view_state.get_field("view_query").query
                == new_view_state.get_field("view_query").query
            ):
                return

            # Gather related fields
            related_fields = {}
            primary_key_rel = None
            for field_name, field in new_view_state.fields.items():
                if field.remote_field:
                    if field.remote_field.model:
                        if field.primary_key:
                            primary_key_rel = field.remote_field.model
                        elif not field.remote_field.parent_link:
                            related_fields[field_name] = field
                    if getattr(field.remote_field, "through", None):
                        related_fields[field_name] = field

            # Depend on the deletion of any possible proxy version of us
            dependencies = [
                (app_label, view_name, None, False),
            ]
            # Depend on all bases
            for base in new_view_state.bases:
                if isinstance(base, str) and "." in base:
                    base_app_label, base_name = base.split(".", 1)
                    dependencies.append((base_app_label, base_name, None, True))
                    # Depend on the removal of base fields if the new model has
                    # a field with the same name.
                    old_base_view_state = self.from_state_view_states.get(
                        (base_app_label, base_name)
                    )
                    new_base_view_state = self.to_state_view_states.get(
                        (base_app_label, base_name)
                    )
                    if old_base_view_state and new_base_view_state:
                        removed_base_fields = (
                            set(old_base_view_state.fields)
                            .difference(
                                new_base_view_state.fields,
                            )
                            .intersection(new_view_state.fields)
                        )
                        for removed_base_field in removed_base_fields:
                            dependencies.append(
                                (base_app_label, base_name, removed_base_field, False)
                            )
            # Depend on the other end of the primary key if it's a relation
            if primary_key_rel:
                dependencies.append(
                    resolve_relation(
                        primary_key_rel,
                        app_label,
                        view_name,
                    )
                    + (None, True)
                )
            # Generate creation operation
            self.add_operation(
                app_label,
                AlterMaterializedView(
                    name=new_view_state.name,
                    fields=[
                        d
                        for d in new_view_state.fields.items()
                        if isinstance(d[1], QueryField)
                    ],
                    options=new_view_state.options,
                    bases=(DbMaterializedView,),
                    managers=new_view_state.managers,
                ),
                dependencies=dependencies,
            )
