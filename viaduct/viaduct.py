import logging
import multiprocessing as mp
import time
from abc import ABC, abstractmethod
from collections import defaultdict, deque
from dataclasses import dataclass
from functools import reduce
from multiprocessing import Process, Queue
from multiprocessing.managers import DictProxy
from pathlib import Path
from queue import Empty
from threading import Thread
from typing import Any, Deque, Dict, List, Set

from hydra.utils import instantiate
from omegaconf import DictConfig, ListConfig, OmegaConf


class Handler(ABC):
    """Интерфейс для компонента аналитики."""
    def on_start(self, *args, **kwargs) -> Any:
        pass

    @abstractmethod
    def handle(self, *args, **kwargs) -> Any:
        raise NotImplementedError

    def on_exit(self) -> None:
        pass


@dataclass
class Task:
    """Контейнер для передаваемых данных во flow аналитики."""
    data: Any
    stream_id: Any


class StopTask(Task):
    """Задача для изящной остановки аналитики."""


class SkipTask(Task):
    """Задача для пропуска итерации обработки."""


Tasks = List[Task]


class TaskHandler(ABC):
    """Обработчик задач.

    Своего рода интерфейс адаптера компонента аналитики (Handler) при его интегрировании во flow Viaduct.
    """
    def on_start(self) -> None:
        pass

    @abstractmethod
    def handle(self, tasks: Tasks) -> Tasks:
        """Метод для обработки задач в вызывающем runtime.

        Args:
            tasks: список задач для обработки
        """
        raise NotImplementedError

    def loop(self, input_queue: mp.Queue, output_queue: mp.Queue, step_number: int) -> None:
        """Метод для запуска обработчика в отдельном процессе.

        Args:
            input_queue: очередь для получения задач
            output_queue: очередь для отправки результатов обработки задач
            step_number: порядковый номер шага во flow аналитики
        """
        self.on_start()

        tasks_buffer: Deque[Tasks] = deque(maxlen=24*24*60)
        tasks_buffer_lock: mp.Lock = mp.Lock()
        stop_task_received: mp.Event = mp.Event()

        def _queue_inspector() -> None:
            last_log_time: float = time.time()

            while True:
                try:
                    tasks_ = input_queue.get(timeout=0.0001)
                except Empty:
                    continue

                if isinstance(tasks_, StopTask):
                    stop_task_received.set()
                    break

                with tasks_buffer_lock:
                    tasks_buffer.append(tasks_)

                if time.time() - last_log_time > 1.0:
                    logging.getLogger('viaduct').info(f'buffer{step_number},{len(tasks_buffer)}')
                    last_log_time = time.time()

        queue_inspector_thread: Thread = Thread(target=_queue_inspector, daemon=True)
        queue_inspector_thread.start()

        while not (stop_task_received.is_set() and len(tasks_buffer) == 0):

            try:
                with tasks_buffer_lock:
                    tasks = tasks_buffer.popleft()
            except IndexError:
                time.sleep(0.0001)
                continue

            output_queue.put(self.handle(tasks))

        output_queue.put(StopTask(data=None, stream_id=None))

        queue_inspector_thread.join()

        self.on_exit()

    def on_exit(self) -> None:
        pass


class FlowStep(TaskHandler):
    """Обрабатывает задачи последовательностью обработчиков."""
    def __init__(self, task_handlers: ListConfig, step_number: int, config_filepath: Path) -> None:
        """
        Args:
            task_handlers: конфигурация обработчиков задач
            step_number: порядковый номер шага во flow аналитики
            config_filepath: путь до конфигурационного файла всего flow аналитики

        Конфигурация обработчиков задач должна иметь тип ListConfig и следующую структуру:

          - _target_: <путь до класса адаптера обработчика задач>
            common_task_handler: <true/false>
            <остальные параметры обработчика задач>
          - ...

        Если common_task_handler == true, то обработчик:
        а) создаётся один раз в методе on_start()
        б) будет использоваться с одинаковыми настройками по умолчанию для всех потоков

        Если common_task_handler == false, то:
        а) будет создан отдельный экземпляр обработчика для каждого stream_id
        б) при создании конфигурация обработчика по умолчанию объединяется с конфигурацией под конкретный stream_id
        """
        self._task_handlers_cfg: ListConfig = task_handlers
        self._step_number: int = step_number
        self._config: Path = config_filepath

        self._common_task_handlers: Dict[int, TaskHandler] = {}
        self._stream_task_handlers: Dict[Any, List[TaskHandler]] = defaultdict(list)

    def on_start(self) -> None:
        """Создаёт обработчики задач, которые будут использоваться для всех потоков с одинаковыми настройками."""
        for i, task_handler_cfg in enumerate(self._task_handlers_cfg):
            if task_handler_cfg['common_task_handler']:
                self._common_task_handlers[i] = instantiate(task_handler_cfg)
                self._common_task_handlers[i].on_start()

    def _create_stream_task_handler(self, handler_number: int, default_cfg: DictConfig, stream_id: Any) -> TaskHandler:
        """Создает экземпляр обработчика задач для конкретного потока (stream_id).

        При создании обработчика настройки по умолчанию объединяются с настройками под конкретный поток.

        Args:
            handler_number: порядковый номер обработчика задач
            default_cfg: конфигурация по умолчанию
            stream_id: идентификатор потока

        Returns:
            экземпляр обработчика задач с настройками под конкретный поток
        """
        cfg: DictConfig = OmegaConf.load(self._config)
        key: str = f'stream_configs.{stream_id}.{self._step_number}.task_handlers.{handler_number}'
        stream_cfg = OmegaConf.select(cfg=cfg, key=key, default=OmegaConf.create({}))
        return instantiate(OmegaConf.merge(default_cfg, stream_cfg))

    def _get_stream_task_handlers(self, stream_id: Any) -> List[TaskHandler]:
        """Возвращает список обработчиков задач для конкретного потока.

        Args:
            stream_id: идентификатор потока

        Returns:
            список обработчиков задач потока
        """
        if stream_id not in self._stream_task_handlers:
            for i, task_handler_cfg in enumerate(self._task_handlers_cfg):
                if task_handler_cfg['common_task_handler']:
                    task_handler = self._common_task_handlers[i]
                else:
                    task_handler = self._create_stream_task_handler(i, task_handler_cfg, stream_id)
                    task_handler.on_start()

                self._stream_task_handlers[stream_id].append(task_handler)

        return self._stream_task_handlers[stream_id]

    def handle(self, tasks: Tasks) -> Tasks:
        # todo: 20.10.2023 d.zittser@ivideon.com - убрать блок проверки одинаковых stream_id
        stream_id: Any = tasks[0].stream_id
        stream_ids = [task.stream_id for task in tasks]
        assert stream_ids.count(stream_id) == len(stream_ids), 'Tasks must be from one stream'

        return reduce(lambda x, y: y.handle(x), self._get_stream_task_handlers(stream_id), tasks)

    def on_exit(self) -> None:
        exited: Set[int] = set()

        for task_handler in self._common_task_handlers.values():
            task_handler.on_exit()
            exited.add(id(task_handler))

        for task_handlers in self._stream_task_handlers.values():
            for task_handler in task_handlers:
                if id(task_handler) not in exited:
                    task_handler.on_exit()


class AnalyticsFacade(ABC):
    """Фасад аналитики."""
    def __init__(self, config: Path) -> None:
        """
        Конфигурационный yaml-файл должен иметь следующую структуру:

            flow:
            - _target_: <путь до viaduct.FlowStep>
              task_handlers:
                - _target_: <путь до класса адаптера обработчика задач>
                common_task_handler: <true/false>
                <остальные параметры обработчика задач>
            - _target_: <путь до viaduct.FlowStep>
              ...
            ...

        Важно отметить, что:
          а) первый обработчик будет отрабатывать в runtime, вызывающем метод process_frame()
          б) последний обработчик будет отрабатывать в runtime, вызывающем метод get_events()

        Задумано, что все обработчики между первым и последним будут запущены в отдельных процессах.

        Args:
            config: путь до конфигурационного yaml-файла всего flow аналитики
        """
        if not config.exists():
            raise FileNotFoundError(f'Config file {config} does not exist')

        self._lock_config: mp.Lock = mp.Lock()
        self._config: Path = config
        self._cfg: DictConfig = OmegaConf.load(config)

        self._mp_manager: mp.Manager = mp.Manager()
        self._analytics_events: DictProxy = self._mp_manager.dict()

        self._queues: List[Queue] = [Queue() for _ in range(len(self._cfg['flow']) - 1)]
        self._flow_steps: List[FlowStep] = []
        self._workers: List[Process] = []

        for i, step_cfg in enumerate(self._cfg['flow']):
            self._flow_steps.append(instantiate(step_cfg, _recursive_=False, step_number=i, config_filepath=config))

            if 0 < i < (len(self._cfg['flow']) - 1):
                args = (self._queues[i-1], self._queues[i], i)
                self._workers.append(Process(target=self._flow_steps[-1].loop, args=args))

        self._analytics_events_thread = Thread(target=self._receive_analytics_events)
        self._analytics_events_thread.start()

        for worker in self._workers:
            worker.start()
            time.sleep(1.0)

        self._stopped: bool = False

        self._log_queues_sizes_thread = Thread(target=self._log_queues_sizes, daemon=True)
        self._log_queues_sizes_thread.start()

    def _log_queues_sizes(self) -> None:
        while not self._stopped:
            logging.getLogger('viaduct').info('\n'.join([f'queue{i},{q.qsize()}' for i, q in enumerate(self._queues)]))
            time.sleep(1)

    @abstractmethod
    def init_stream(self, *args, **kwargs) -> Any:
        """Метод инициализации потока.

        Количество параметров и их структура под каждую аналитику согласовывается с вызывающей стороной.
        """
        raise NotImplementedError

    def process_frame(self, stream_id: Any, frame: 'numpy.ndarray') -> None:  # noqa: F821
        """Метод для обработки кадра.

        Пакует аргументы в задачу и отправляет на flow обработки.

        Args:
            stream_id: идентификатор потока
            frame: кадр
        """
        tasks = self._flow_steps[0].handle(tasks=[Task(data=frame, stream_id=stream_id)])

        if not isinstance(tasks, SkipTask):
            self._queues[0].put(tasks)

    def _receive_analytics_events(self) -> None:
        """Метод для получения результатов аналитики, запускается в отдельном потоке.

        Результаты, полученные из очереди, сохраняются в словарь, где:
            - ключ - идентификатор потока
            - значение - список событий, полученных из аналитики
        """
        while True:
            try:
                tasks: Tasks = self._queues[-1].get(timeout=0.001)
            except Empty:
                continue

            if isinstance(tasks, StopTask):
                break

            for task in tasks:
                if task.stream_id not in self._analytics_events:
                    self._analytics_events[task.stream_id] = self._mp_manager.list()

                self._analytics_events[task.stream_id].append(task)

    def get_events(self, stream_id: Any) -> List[Any]:
        """Получение событий аналитики для конкретного потока (stream_id).

        Забирает результаты для конкретного потока (stream_id) из словаря и пропускает их через final handler.

        По задумке главная задача последнего обработчика - преобразование события из формата аналитики в формат
        вызывающей стороны.

        Args:
            stream_id: идентификатор потока

        Returns:
            список событий для конкретного потока
        """
        events = []

        if stream_id in self._analytics_events:
            events = self._flow_steps[-1].handle(tasks=self._analytics_events[stream_id])
            self._analytics_events[stream_id] = self._mp_manager.list()

        return [event.data for event in events if event.data]

    def stop(self):
        """Метод для остановки всего flow аналитики."""
        self._queues[0].put(StopTask(data=None, stream_id=None))

        self._stopped = True
        self._log_queues_sizes_thread.join()

        for worker in self._workers:
            worker.join()
