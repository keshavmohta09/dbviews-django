## Usage

Discover how to define and utilize database views and materialized views seamlessly within your Django project.

## Installation

Ensure you have Python 3.9 or higher and Django 4.x installed before proceeding with the installation.

You can install the views package via pip:

```bash
pip install dbviews-django
```

### Defining Views

To define views in your Django project, you will first need to ensure that the `dbviews` app is included in your project settings. You can do this by adding `'dbviews'` to the `INSTALLED_APPS` list in your `settings.py` file:

```python
# settings.py

INSTALLED_APPS = [
    ...
    'dbviews',
    ...
]
```

Once dbviews is included, you can define your views using the provided classes. 

You can define your views in either `models.py` or in a separate `views.py` file located within a `models` folder. 

Here's how you can structure your Django app:

    - yourapp/
        models/
            __init__.py
            models.py
            views.py
   

### Here's an example:
```python
# models/views.py

from dbviews import views

class MyView(views.DbView):
    """
    Define your view using DbView class.
    """
    view_query = views.QueryField(query="SELECT * FROM my_table WHERE condition = true")
    # Other fields....

class MyMaterializedView(views.DbMaterializedView):
    """
    Define your materialized view using DbMaterializedView class.
    """
    view_query = views.QueryField(query="SELECT * FROM my_table WHERE condition = true")
    # Other fields....
```

In the above example, MyView and MyMaterializedView are defined as subclasses of DbView and DbMaterializedView respectively. The view_query attribute specifies the SQL query that defines the view's logic.

To make your models and views accessible from the root of the models folder, you need to import them in the __init__.py file:

```python
# __init__.py  

from .models import MyModel  # Import your models  
from .views import MyView     # Import your views  
```

To refresh the materialized views you can refresh method.
```python
    MyMaterializedView.refresh()
```


### Applying Migrations
After defining your views, you'll need to generate migrations to apply these changes to your database schema. Use Django's `makemigrations` and `migrate` command to generate migration files:

```shell
python manage.py makemigrations # This will all the required migrations
python manage.py migrate # This will create all the views and models based on the migration
```

### Contributing
Contributions are welcome! 

If you encounter any issues or have feature requests, please don't hesitate to submit them on [GitHub](https://github.com/keshavmohta09/dbviews-django/).
