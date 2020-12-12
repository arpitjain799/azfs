import importlib
import sys


class CliFactory:

    def __init__(self, target_file_dir):
        self.target_file_dir = target_file_dir

    def load_export_decorator(self, target_file_name: str):
        if self.target_file_dir not in sys.path:
            sys.path.insert(0, self.target_file_dir)
        try:
            app = importlib.import_module(target_file_name)
            export_decorator = getattr(app, 'export_decorator')
        except SyntaxError as e:
            message = (
                          'Unable to import your app.py file:\n\n'
                          'File "%s", line %s\n'
                          '  %s\n'
                          'SyntaxError: %s'
                      ) % (getattr(e, 'filename'), e.lineno, e.text, e.msg)
            raise RuntimeError(message)
        return export_decorator
