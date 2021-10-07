from os.path import exists

from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class FileSensor(BaseSensorOperator):
    """
    Menunggu suatu file apakah sudah ada.
    """
    template_fields = ('path', )

    @apply_defaults
    def __init__(self, path, *args, **kwargs):
        self.path = path
        kwargs.setdefault('poke_interval', 10)
        super(FileSensor, self).__init__(*args, **kwargs)

    def poke(self, _context):
        print('poking path', self.path)
        return exists(self.path)
