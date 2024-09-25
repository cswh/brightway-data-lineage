from datetime import datetime
from openlineage.client import OpenLineageClient
from openlineage.client.transport.http import ApiKeyTokenProvider, HttpConfig, HttpCompression, HttpTransport
from openlineage.client.transport.file import FileConfig, FileTransport
from openlineage.client.transport.console import ConsoleConfig, ConsoleTransport
from openlineage.client.event_v2 import RunEvent, RunState, Run, Job, Dataset, DatasetEvent, JobEvent
from openlineage.client.uuid import generate_new_uuid


transport_type = "http"

if transport_type == "file":
    file_config = FileConfig(
        log_file_path="test-lineage-file",
        append=True,
    )
    client = OpenLineageClient(transport=FileTransport(file_config))
elif transport_type == "http":
    http_config = HttpConfig(
        url="http://localhost:5000",
        endpoint="api/v1/lineage",
        timeout=5,
        verify=False,
        # auth=ApiKeyTokenProvider({"apiKey": "f048521b-dfe8-47cd-9c65-0cb07d57591e"}),
        compression=HttpCompression.GZIP,
    )
    client = OpenLineageClient(transport=HttpTransport(http_config))
elif transport_type == "console":
    # See https://openlineage.io/docs/client/python#console
    console_config = ConsoleConfig()
    client = OpenLineageClient(transport=ConsoleTransport(console_config))


producer = "test-brightway-user"

inventory = Dataset(namespace="food_delivery", name="public.inventory")
menus = Dataset(namespace="food_delivery", name="public.menus_1")
orders = Dataset(namespace="food_delivery", name="public.orders_1")

client.emit(DatasetEvent(eventTime=datetime.now().isoformat(), dataset=inventory))
client.emit(DatasetEvent(eventTime=datetime.now().isoformat(), dataset=menus))

job = Job(namespace="food_delivery", name="example.order_data")

client.emit(JobEvent(eventTime=datetime.now().isoformat(), job=job))

run = Run(str(generate_new_uuid()))

client.emit(
    RunEvent(
        eventTime = datetime.now().isoformat(),
        run = run,
        job = job,
        eventType = RunState.START,
        producer=producer
    )
)


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