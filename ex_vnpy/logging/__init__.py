import logging

from PySide6.QtWidgets import QTextEdit


class PyQtGraphHandler(logging.Handler):
    def __init__(self, log_widget):
        super().__init__()
        self.log_widget = log_widget

    def emit(self, record):
        msg = self.format(record)
        self.log_widget.append(msg)

class LogWidget(QTextEdit):
    def __init__(self):
        super().__init__()
        self.setReadOnly(True)

