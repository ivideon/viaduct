import logging.config

from .viaduct import AnalyticsFacade, FlowStep, Handler, SkipTask, StopTask, Task, TaskHandler, Tasks  # noqa: F401


LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'handlers': {
        'viaduct_filehandler': {
            'level': 'INFO',
            'class': 'logging.FileHandler',
            'filename': 'viaduct.log',
        },
    },
    'loggers': {
        'viaduct': {
            'handlers': ['viaduct_filehandler'],
            'level': 'INFO',
            'propagate': False,
        },
    },
}


logging.config.dictConfig(LOGGING_CONFIG)
