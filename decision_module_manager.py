from threading import Thread

from event import *


class DecisionModuleManager:
    '''
    Класс отвечающий за представление DM Manager
    Его задача -- отправлять запрос на анализ тренда в DM AI и отправлять запрос на скалирование в k8s
    '''

    def __init__(self, kafka_event_reader: KafkaEventReader):
        '''
        Инициализация класса
        '''
        self.kafka_event_reader = kafka_event_reader
        self.running = True
        Thread(target=self.kafka_event_reader.read_events)

    async def run(self):
        '''
        Принятие ивентов с KafkaEventReader
        '''
        while self.running:
            if self.kafka_event_reader.events.empty(): continue
            event = self.kafka_event_reader.events.get()
            self.handle_event(event)

    # можно сделать асинк, чтобы хендлить каждый ивент в отдельном потоке    
    def handle_event(self, event: Event):
        '''
        Обработка ивентов, здесь находится основная логика микросервиса
        '''
        match event.type:
            case EventType.TrendData:
                self.handle_event_trend_data(event.trend_data)
            case EventType.Scalek8s:
                self.handle_event_scalek8s(event.scale_data)
            case _:
                pass

    def handle_event_trend_data(trend_data: TrendData):
        pass

    def handle_event_scalek8s(scale_data: ScaleData):
        pass