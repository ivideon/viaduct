from dataclasses import replace
from pathlib import Path
from typing import Any, List, Optional

import pytest
from omegaconf import DictConfig, OmegaConf

from ivideon.viaduct.viaduct import AnalyticsFacade, Handler, TaskHandler, Tasks


class DummyHandler(Handler):
    """Обработчик-пустышка, который просто добавляет сообщение в список сообщений."""
    def __init__(self, name: str, some_threshold: Optional[float] = None):
        self._name = name
        self._some_threshold: float = some_threshold or 0.5
        self._it_is_default_threshold: bool = some_threshold is None
        self._message: Optional[str] = None

    def on_start(self) -> None:
        """При старте обработчика формируем сообщение, которое он будет добавлять в список сообщений."""
        threshold_type: str = {True: 'default', False: 'stream'}[self._it_is_default_threshold]
        self._message = f'I am {self._name} with {threshold_type} threshold {self._some_threshold:.1f}'

    def handle(self, previous_messages: List[str]) -> List[str]:
        return previous_messages + [self._message]


class DummyAdapter(TaskHandler):
    """Адаптер обработчика-пустышки для его интеграции во flow Viaduct."""
    def __init__(self, common_task_handler: bool, some_handler: Handler):
        self.common_task_handler = common_task_handler
        self._some_handler = some_handler

    def on_start(self) -> None:
        self._some_handler.on_start()

    def handle(self, tasks: Tasks) -> Tasks:
        return [replace(t, data=self._some_handler.handle(previous_messages=t.data)) for t in tasks]


class EventConverterAdapter(TaskHandler):
    """Финальный адаптер, который преобразует список сообщений в одно сообщение, разделенное переносом строки."""
    def __init__(self, common_task_handler: bool):
        self._common_task_handler = common_task_handler

    def handle(self, tasks: Tasks) -> Tasks:
        header: str = 'Dummy analytics flow stream_id = {}'
        return [replace(t, data='\n'.join([header.format(t.stream_id)] + t.data)) for t in tasks]


class DummyAnalytics(AnalyticsFacade):
    """Аналитика-пустышка."""
    def init_stream(self, stream_id: Any, heuristic_threshold: float) -> None:
        """Инициализируем поток, задавая порог для обработчика-пустышки Heuristic."""
        heuristic_threshold_config_key = f'stream_configs.{stream_id}.2.task_handlers.1.some_handler.some_threshold'

        with self._lock_config:
            cfg: DictConfig = OmegaConf.load(self._config)
            OmegaConf.update(cfg=cfg, key=heuristic_threshold_config_key, value=heuristic_threshold)
            OmegaConf.save(config=cfg, f=self._config)
        # так как первый и последний обработчики будут отрабатывать на стороне воркера, то инициализируем их
        self._flow_steps[0].on_start()
        self._flow_steps[-1].on_start()


@pytest.fixture
def dummy_analytics_yaml_config(tmp_path: Path) -> Path:
    config_filepath: Path = tmp_path / 'dummy_analytics_config.yaml'

    config = {
        'flow': [
            {
                '_target_': 'ivideon.viaduct.viaduct.FlowStep',
                'task_handlers': [
                    {
                        '_target_': 'ivideon.viaduct.tests.conftest.DummyAdapter',
                        'common_task_handler': True,
                        'some_handler': {
                            '_target_': 'ivideon.viaduct.tests.conftest.DummyHandler',
                            'name': 'PreProcessor',
                        },
                    },
                ],
            },
            {
                '_target_': 'ivideon.viaduct.viaduct.FlowStep',
                'task_handlers': [
                    {
                        '_target_': 'ivideon.viaduct.tests.conftest.DummyAdapter',
                        'common_task_handler': True,
                        'some_handler': {
                            '_target_': 'ivideon.viaduct.tests.conftest.DummyHandler',
                            'name': 'Inference',
                        },
                    },
                ],
            },
            {
                '_target_': 'ivideon.viaduct.viaduct.FlowStep',
                'task_handlers': [
                    {
                        '_target_': 'ivideon.viaduct.tests.conftest.DummyAdapter',
                        'common_task_handler': True,
                        'some_handler': {
                            '_target_': 'ivideon.viaduct.tests.conftest.DummyHandler',
                            'name': 'PostProcessor',
                        },
                    },
                    {
                        '_target_': 'ivideon.viaduct.tests.conftest.DummyAdapter',
                        'common_task_handler': False,
                        'some_handler': {
                            '_target_': 'ivideon.viaduct.tests.conftest.DummyHandler',
                            'name': 'Heuristic',
                            'some_threshold': None,  # порог будет задан индивидуально для каждого потока
                        },
                    },
                ],
            },
            {
                '_target_': 'ivideon.viaduct.viaduct.FlowStep',
                'task_handlers': [
                    {
                        '_target_': 'ivideon.viaduct.tests.conftest.EventConverterAdapter',
                        'common_task_handler': True,
                    },
                ],
            },
        ],
    }

    OmegaConf.save(config, config_filepath)
    return config_filepath


@pytest.fixture(scope='function')
def dummy_analytics(dummy_analytics_yaml_config: Path) -> DummyAnalytics:
    return DummyAnalytics(config=dummy_analytics_yaml_config)
