# 使用dictConfig风格的配置对象，动态指定RotatingFileHandler的参数
logConfig = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'generalFormatter': {
            'format': '%(asctime)s %(name)s %(levelname)s %(message)s',
            'datefmt': '%H:%M:%S',
        },
        'simpleFormatter': {
            'format': '%(asctime)s %(levelname)s %(message)s',
            'datefmt': '%H:%M:%S',
        }
    },
    'handlers': {
        'fileHandler': {
            'class': 'logging.handlers.RotatingFileHandler',
            'level': 'DEBUG',
            'formatter': 'generalFormatter',
            'filename': 'backtesting.log',
            'maxBytes': 1024 * 1024 * 5,
            'backupCount': 3,
            'encoding': 'utf-8'
        },
        'consoleHandler': {
            'class': 'logging.StreamHandler',
            'level': 'DEBUG',
            'formatter': 'generalFormatter',
            'stream': 'ext://sys.stdout'
        }
    },
    'loggers': {
        'root': {
            # 'handlers': ['fileHandler', 'consoleHandler', 'pyQtGraphHandler'],
            'handlers': ['fileHandler', 'consoleHandler'],
            'level': 'DEBUG',
            'propagate': True
        }
    }
}
