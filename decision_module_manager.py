import threading
from threading import Thread

from events.event import *
from events.kafka_event import *

from trend_data.trend_data import TrendData
from scale_data.scale_data import ScaleData


import time


class DecisionModuleManager:
    '''
    Класс отвечающий за представление DM Manager
    Его задача -- отправлять запрос на анализ тренда в DM AI и отправлять запрос на скалирование в k8s
    '''

    def __init__(self, event_queue: Queue):
        '''
        Инициализация класса
        '''
        self.running = threading.Event()
        self.running.set()

        self.event_queue = event_queue

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
                self.handle_event_scalek8s(event.data)
            case _:
                pass

    def handle_event_trend_data(self, trend_data: TrendData):
        print(f'Got trend data: {trend_data}')

    def handle_event_scalek8s(self, scale_data: ScaleData):
        print(f'Got scale data: {scale_data}')

    def stop(self):
        self.running.clear()


if __name__ == '__main__':
    # shared event queue
    event_queue = Queue()

    kafka_event_reader = KafkaEventReader(
        KafkaConsumer(
            'dmm',
            bootstrap_servers='localhost:9092',
            auto_offset_reset='latest',
            enable_auto_commit=True,
        ),
        event_queue
    )

    dmm = DecisionModuleManager(event_queue)

    kafka_event_writer = KafkaEventWriter(
        KafkaProducer(
            bootstrap_servers='localhost:9092'
        ),
        'dmm'
    )

    for i in range(5):
        kafka_event_writer.send_event(Event(EventType.TrendData, f'some bullshit {i}'))
        time.sleep(2)

    kafka_event_writer.release()
