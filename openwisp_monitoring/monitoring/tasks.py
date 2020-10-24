from datetime import datetime

import pytz
from celery import shared_task
from django.core.exceptions import ObjectDoesNotExist
from swapper import load_model

from ..db import timeseries_db
from ..db.exceptions import TimeseriesWriteException
from .settings import RETRY_OPTIONS


@shared_task(bind=True, autoretry_for=(TimeseriesWriteException,), **RETRY_OPTIONS)
def timeseries_write(self, name, values, metric_pk=None, threshold_dict=None, **kwargs):
    """
    write with exponential backoff on a failure
    """
    timeseries_db.write(name, values, **kwargs)
    if not metric_pk or not threshold_dict:
        return
    try:
        metric = load_model('monitoring', 'Metric').objects.get(pk=metric_pk)
        time = threshold_dict.pop('time')
        if isinstance(time, str):
            time = (
                datetime.strptime(time, '%Y-%m-%dT%H:%M:%S.%fZ')
                if 'Z' in time
                else datetime.strptime(time, '%Y-%m-%dT%H:%M:%S.%f')
            )
            time = pytz.utc.localize(time)
        metric.check_threshold(time=time, **threshold_dict)
    except ObjectDoesNotExist:
        # The metric can be deleted by the time threshold is being checked.
        # This can happen as the task is being run async.
        pass
