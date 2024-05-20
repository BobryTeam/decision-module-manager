from typing import Dict

from threading import Thread

import kubernetes

from events.event import *
from events.kafka_event import *

from trend_data.trend_data import TrendData
from scale_data.scale_data import ScaleData

from microservice.microservice import Microservice


class DecisionModuleManager(Microservice):
    '''
    Класс отвечающий за представление DM Manager
    Его задача -- отправлять запрос на анализ тренда в DM AI и отправлять запрос на скалирование в k8s
    '''

    def __init__(self, event_queue: Queue, writers: Dict[str, KafkaEventWriter], target_deployment: str, target_namespace: str):
        '''
        Инициализация класса:
        - `target_deployment` - название пода, который мы скалируем
        - `target_namespace` - неймспейс пода, который мы скалируем
        Поля класса:
        - `self.target_deployment` - название пода, который мы скалируем
        - `self.target_namespace` - неймспейс пода, который мы скалируем
        - `self.k8s_api` - доступ в апи кубов
        '''

        kubernetes.config.load_incluster_config()
        self.target_deployment = target_deployment
        self.target_namespace = target_namespace
        self.k8s_api = kubernetes.client.AppsV1Api()

        super().__init__(event_queue, writers)

    def handle_event(self, event: Event):
        '''
        Обработка ивентов
        '''
        target_function = None

        match event.type:
            case EventType.TrendData:
                target_function = self.handle_event_trend_data
            case EventType.TrendAnalyseResult:
                target_function = self.handle_event_trend_analyse_result
            case _:
                pass

        if target_function is not None:
            Thread(target=target_function, args=(event.data,)).start()

    def handle_event_trend_data(self, trend_data: TrendData):
        # send trend data to DM AI
        self.writers['dmai'].send_event(Event(EventType.TrendData, trend_data))

    def handle_event_trend_analyse_result(self, scale_data: ScaleData):
        # set new replica count to the target deployment
        deployment = self.k8s_api.read_namespaced_deployment(self.target_deployment, self.target_namespace)
        deployment.spec.replicas = scale_data.replica_count
        _api_response = self.k8s_api.patch_namespaced_deployment(
            name=self.target_deployment,
            namespace=self.target_namespace,
            body=deployment
        )
