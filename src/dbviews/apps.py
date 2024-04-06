from django.apps import AppConfig


class DbviewsConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "dbviews"

    def ready(self) -> None:
        """
        Setting custom migration auto detector as makemigrations autodetector
        to get views and materialized views code in migrations
        """
        from django.core.management.commands import makemigrations

        from dbviews.autodetector import MigrationAutodetector

        makemigrations.MigrationAutodetector = MigrationAutodetector
        return super().ready()
