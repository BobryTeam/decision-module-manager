import os
from queue import Queue

from decision_module_manager import DecisionModuleManager

from events import *

from kafka import KafkaConsumer, KafkaProducer


# put the values in a k8s manifest
kafka_bootstrap_server = os.environ.get('KAFKA_BOOTSTRAP_SERVER')
target_deployment = os.environ.get('TARGET_DEPLOYMENT')
target_namespace = os.environ.get('TARGET_NAMESPACE')

if kafka_bootstrap_server is None:
    print(f'Environment variable KAFKA_BOOTSTRAP_SERVER is not set')
    exit(1)

if target_deployment is None:
    print(f'Environment variable TARGET_DEPLOYMENT is not set')
    exit(1)

if target_namespace is None:
    print(f'Environment variable TARGET_NAMESPACE is not set')
    exit(1)


event_queue = Queue()

# share the queue with a reader
reader = KafkaEventReader(
    KafkaConsumer(
        'dmm',
        bootstrap_servers=kafka_bootstrap_server,
        auto_offset_reset='latest',
    ),
    event_queue
)

# writers to other microservices
writers = {
    'dmai': KafkaEventWriter(
        KafkaProducer(
            bootstrap_servers=kafka_bootstrap_server
        ),
        'dmai'
    ),
}

# init the microservice
decision_module_manager = DecisionModuleManager(
    event_queue, writers, target_deployment, target_namespace
)

decision_module_manager.running_thread.join()