import threading
from threading import Thread

import kubernetes

from events.event import *
from events.kafka_event import *

from trend_data.trend_data import TrendData
from scale_data.scale_data import ScaleData



class DecisionModuleManager:
    '''
    Класс отвечающий за представление DM Manager
    Его задача -- отправлять запрос на анализ тренда в DM AI и отправлять запрос на скалирование в k8s
    '''

    def __init__(self, target_deployment: str, target_namespace: str, dm_ai_event_writer: KafkaEventWriter, event_queue: Queue):
        '''
        Инициализация класса
        '''

        self.event_queue = event_queue

        kubernetes.config.load_incluster_config()
        self.target_deployment = target_deployment
        self.target_namespace = target_namespace
        self.k8s_api = kubernetes.client.AppsV1Api()

        self.dm_ai_event_writer = dm_ai_event_writer

        self.running = threading.Event()
        self.running.set()

        self.running_thread = Thread(target=self.run)
        self.running_thread.start()

    def run(self):
        '''
        Принятие событий из очереди 
        '''
        while self.running.is_set() or not self.event_queue.empty():
            if self.event_queue.empty(): continue
            event_thread = Thread(target=self.handle_event, args=((self.event_queue.get()),))
            event_thread.start()

    def handle_event(self, event: Event):
        '''
        Обработка ивентов, здесь находится основная логика микросервиса
        '''
        match event.type:
            case EventType.TrendData:
                self.handle_event_trend_data(event.data)
            case EventType.TrendAnalyseResult:
                self.handle_event_trend_analyse_result(event.data)
            case _:
                pass

    def handle_event_trend_data(self, trend_data: TrendData):
        # send trend data to DM AI
        self.dm_ai_event_writer.send_event(Event(EventType.TrendData, trend_data))

    def handle_event_trend_analyse_result(self, scale_data: ScaleData):
        # set new replica count to the target deployment
        deployment = self.k8s_api.read_namespaced_deployment(self.target_deployment, self.target_namespace)
        deployment.spec.replicas = scale_data.replica_count
        _api_response = self.k8s_api.patch_namespaced_deployment(
            name=self.target_deployment,
            namespace=self.target_namespace,
            body=deployment
        )

    def stop(self):
        self.running.clear()
