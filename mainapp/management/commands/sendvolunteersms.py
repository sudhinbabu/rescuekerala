import os

from django.core.management.base import BaseCommand
from django.core.cache import cache
import requests
from threading import Thread
from datetime import datetime

from django.conf import settings
import csv
from dateutil import parser
import time

import logging
import calendar


logger = logging.getLogger('send_volunteer_sms')


# python manage.py sendvolunteersms
# python manage.py sendvolunteersms /tmp/volunteer.csv
# python manage.py sendvolunteersms --clearcache=1
class Command(BaseCommand):
    # SMS_API_URL = "http://api.esms.kerala.gov.in/fastclient/SMSclient.php"
    SMS_API_URL = "http://127.0.0.1:8000/test_send_sms/"
    API_USERNAME = os.environ.get("SMS_USER")
    API_PASSWORD = os.environ.get("SMS_PASSWORD")

    DEFAULT_CSV = os.path.join(settings.BASE_DIR,
                               'mainapp/management/commands/smsvolunteer.csv')
    BATCH_MAX = 10

    msg_url_template = "http://keralarescue.in/c/{sendID}/{timestamp}"
    message_template = "Thank you for registering to volunteer. Please click here to confirm {url}"
    success_check_cache_key = "SendingFailed_{phone}"

    def add_arguments(self, parser):
        parser.add_argument('path', nargs='?', type=str)
        parser.add_argument('--clearcache', nargs='?', type=bool)

    @property
    def volunteers(self):
        with open(self.path, "r") as volunteers:
            for volunteer in csv.DictReader(volunteers):
                yield volunteer

    @staticmethod
    def clean_timestamp(timestamp):
        # not clear about this logic just copied from -> sms.py
        timestamp = parser.parse(timestamp)
        timestamp = calendar.timegm(timestamp.utctimetuple())
        return str(timestamp)[-4:]

    def send_sms(self, payload):
        res = requests.get(self.SMS_API_URL, params=payload)
        if res.status_code in (200, 201):
            cache.set(self.success_check_cache_key.format(
                      phone=payload["numbers"]), True)
        else:
            logger.info("failed {} {}".format())

    def process_batch(self, batch):
        tasks = []
        for payload in batch:
            self.total_count += 1
            t = Thread(target=self.send_sms,
                       args=(payload,))
            tasks.append(t)
            t.start()

        for task in tasks:
            t.join()

    def handle(self, *args, **options):
        if options["clearcache"]:
            logger.info("clearing cache for sendvolunteersms.")
            cache.delete_pattern(self.success_check_cache_key.format(phone="*"))
        else:
            t1 = time.time()
            self.path = options["path"] if options["path"] else self.DEFAULT_CSV
            batch = []
            batch_count = 0
            self.total_count = 0
            logger.info("STARTING sendvolunteersms.")

            for volunteer in self.volunteers:
                msg_url = self.msg_url_template.format(sendID=volunteer["ID"],
                                                       timestamp="{:%Y-%m-%d %H:%M}".format(datetime.now()))
                message = self.message_template.format(url=msg_url)
                payload = {'username': self.API_USERNAME,
                           'password': self.API_PASSWORD,
                           'message': message,
                           'numbers': volunteer["phone"]}
                if not cache.get(
                    self.success_check_cache_key.format(
                        phone=payload["numbers"])):
                    batch.append(payload)
                    batch_count += 1
                    if batch_count == self.BATCH_MAX:
                        self.process_batch(payload)
                        batch_count = 0
                        batch = []
            if batch:
                self.process_batch(batch)
            logger.info("{} COMPLETED IN {} Seconds sendvolunteersms.".format(self.total_count,
                                                                              time.time() - t1))
