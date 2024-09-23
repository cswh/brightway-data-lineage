from openlineage.client import OpenLineageClient
from openlineage.client.transport.file import FileConfig, FileTransport

# from openlineage.client.run import RunEvent, RunState, Run, Job, Dataset
from openlineage.client.event_v2 import RunEvent, RunState, Run, Job, Dataset, DatasetEvent, JobEvent

from openlineage.client.uuid import generate_new_uuid
from datetime import datetime

file_config = FileConfig(
  log_file_path="test-lineage-file",
  append=True,
)

client = OpenLineageClient(transport=FileTransport(file_config))

producer = "test-brightway-user"

inventory = Dataset(namespace="food_delivery", name="public.inventory")
menus = Dataset(namespace="food_delivery", name="public.menus_1")
orders = Dataset(namespace="food_delivery", name="public.orders_1")

client.emit(DatasetEvent(eventTime=datetime.now().isoformat(), dataset=inventory))
client.emit(DatasetEvent(eventTime=datetime.now().isoformat(), dataset=menus))

job = Job(namespace="food_delivery", name="example.order_data")

client.emit(JobEvent(eventTime=datetime.now().isoformat(), job=job))

run = Run(str(generate_new_uuid()))

# client.emit(
#     RunEvent(
#         RunState.START,
#         datetime.now().isoformat(),
#         run, job, producer
#     )
# )

client.emit(
    RunEvent(
        eventTime = datetime.now().isoformat(),
        run = run,
        job = job,
        eventType = RunState.START,
        producer=producer
    )
)

# client.emit(
#     RunEvent(
#         RunState.COMPLETE,
#         datetime.now().isoformat(),
#         run, job, producer,
#         inputs=[inventory],
#         outputs=[menus, orders],
#     )
# )

client.emit(
    RunEvent(
        eventTime = datetime.now().isoformat(),
        run = run,
        job = job,
        eventType = RunState.COMPLETE,
        producer=producer,
        inputs=[inventory],
        outputs=[menus, orders],
    )
)