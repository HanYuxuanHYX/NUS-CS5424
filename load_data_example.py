import uuid
from cassandra.cqlengine import columns
from cassandra.cqlengine import connection
from datetime import datetime
from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine.models import Model


# first, define a model
class ExampleModel(Model):
    example_id = columns.UUID(primary_key=True, default=uuid.uuid4)
    example_type = columns.Integer(index=True)
    created_at = columns.DateTime()
    description = columns.Text(required=False)


# next, setup the connection to your cassandra server(s)...
# see http://datastax.github.io/python-driver/api/cassandra/cluster.html for options
# the list of hosts will be passed to create a Cluster() instance
if __name__ == '__main__':
    connection.setup(['127.0.0.1'], "cqlengine", protocol_version=3)
    sync_table(ExampleModel)
    em1 = ExampleModel.create(example_type=0, description="example1", created_at=datetime.now())
    em2 = ExampleModel.create(example_type=0, description="example2", created_at=datetime.now())
    em3 = ExampleModel.create(example_type=0, description="example3", created_at=datetime.now())
    em4 = ExampleModel.create(example_type=0, description="example4", created_at=datetime.now())
    em5 = ExampleModel.create(example_type=1, description="example5", created_at=datetime.now())
    em6 = ExampleModel.create(example_type=1, description="example6", created_at=datetime.now())
    em7 = ExampleModel.create(example_type=1, description="example7", created_at=datetime.now())
    em8 = ExampleModel.create(example_type=1, description="example8", created_at=datetime.now())

    q = ExampleModel.objects(example_type=1)
    q.count()
    for instance in q:
        print instance.description
    q2 = q.filter(example_id=em5.example_id)
    print q2.count()
