Entity, Field and named_field usage
===================================

Entity usage
------------

This class brings the build it in methods:

- `to_dict`: Will take all properties of the created class and will convert it into a dict instance.
- `from_dict`: This will take a dict instance and will set the values of every key into a model property with
  the same name.
- `from_record`: It takes an ordered Iterable object with the name of the fields that will be loaded into the model,
  and a tuple with the corresponding values

Entity models will be used with simple class properties or can be used with the `Field` descriptor of the package

Simple properties
*****************

.. code-block:: python

   from pydbrepo import Entity

   class Model(Entity):
       id = None
       name = None

   model = Model.from_dict({"id": 1, "name": "some"})
   # Model({"id": 1, "name": "some"})

   print(model.id) # => 1
   print(model.name) # => some

Property decorators
*******************

.. code-block:: python

   from pydbrepo import Entity

   class Model(Entity):
       def __init__(self):
           super().__init__()

           self.id = None
           self.name = None

       @property
       def id(self):
           return self._id

       @id.setter
       def id(self, value):
           self._id = value

       @property
       def name(self):
           return self._name

       @name.setter
       def name(self, value):
           self._name = value

   model = Model.from_dict({"id": 1, "name": "some"})
   # Model({"id": 1, "name": "some"})

   print(model.id) # => 1
   print(model.name) # => some

Field descriptor
****************

.. code-block:: python

   from pydbrepo import Entity, Field, named_fields

   @named_fields
   class Model(Entity):
       id = Field(type_=int)
       name = Field(type_=str)

   model = Model.from_dict({"id": 1, "name": "some"})
   # Model({"id": 1, "name": "some"})

   print(model.id) # => 1
   print(model.name) # => some

Casting values with Field descriptor
************************************

.. code-block:: python

   from uuid import UUID
   from pydbrepo import Entity, Field, named_fields

   @named_fields
   class Model(Entity):
       id = Field(type_=(UUID, str), cast_to=UUID, cast_if=str)
       name = Field(type_=str)

   model = Model.from_dict({"id": '10620c02-d80e-4950-b0a2-34a5f2d34ae5', "name": "some"})
   # Model({"id": UUID('10620c02-d80e-4950-b0a2-34a5f2d34ae5'), "name": "some"})

   print(model.id) # => 10620c02-d80e-4950-b0a2-34a5f2d34ae5
   print(model.name) # => some

Casting from a callback function
********************************

.. code-block:: python

   from datetime import date, datetime
   from pydbrepo import Entity, Field, named_fields

   def cast_epoch(value):
       if isinstance(value, date):
           return int(value.strftime("%s"))

       if isinstance(value, datetime):
           return int(value.timestamp())

   @named_fields
   class Model(Entity):
       name = Field(type_=str)
       epoch = Field(type_=(int, date, datetime), cast_to=cast_epoch, cast_if=(date, datetime))

   model = Model.from_dict({"name": "some", "epoch": datetime.now()})
   # Model({"name": "some", "epoch": 1231231231})

   print(model.name) # => some
   print(model.epoch) # => 1231231231

Iterable fields and casting with Field descriptor
*************************************************

.. code-block:: python

   from pydbrepo import Entity, Field, named_fields

   @named_fields
   class Item(Entity):
       name = Field(type_=str)
       price = Field(type_=float)

   @named_fields
   class Model(Entity):
       id = Field(type_=int)
       name = Field(type_=str)
       items = Field(type_=list, cast_items_to=Item)

   model = Model.from_dict({
       "id": 1,
       "name": "some",
       "items": [
           {"name": "some", "price": 5.99},
           {"name": "nothing", "price": 6.99},
       ]
   })
   # Model({"id": 1, "name": "some", "items": [Item({"name": "some", "price": 5.99}), Item({"name": "nothing", "price": 6.99})]})

   print(model.id) # => 1
   print(model.name) # => some
   print(model.items) # => [Item({"name": "some", "price": 5.99}), Item({"name": "nothing", "price": 6.99})]
   print(model.items[0].price) # => 5.99
