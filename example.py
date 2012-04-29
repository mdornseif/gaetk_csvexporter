#!/usr/bin/env python
# encoding: utf-8
"""
bi_automation_legacy.py

Created by Maximillian Dornseif on 2011-11-01.
Copyright (c) 2011, 2012 Dr. Maximillian Dornseif All rights reserved.
"""

import config
config.imported = True

import collections
import cStringIO as StringIO
import csv
import datetime
import logging

import gaetk.handler
import gaetk.tools
import gaetk
from gaetk.infrastructure import query_iterator, taskqueue_add_multi
from gaetk import configuration
from google.appengine.api import files, mail, taskqueue
from huTools.aggregation import avg, median
from huTools.calendar.formats import convert_to_date, tertial
from huTools.calendar.tools import date_trunc

from modules.bi.bi_models import BiWertschoepfungArtikel
from modules.bi.bi_models import BiWertschoepfungArtikelMonat, BiWertschoepfungArtikelTertial
from modules.bi.bi_models import BiHistorischerArtikelbestand
from modules.ic import ic_models


class CronDatenExportieren(gaetk.handler.BasicHandler):
    """BiHistorischerArtikelbestandTertial exportieren."""

    def get(self):
        """Cron-Handler ohne Parameter"""
        # Die Daten werden in CSV-Dateien zu Google Cloud Storage exportiert.
        # Das ganze findet sich dann unter https://sandbox.google.com/storage/?project=1027405963272&pli=1
        taskqueue.add(queue_name='biq', url='/auto/bi/TaskWertschoepfungExportieren')
        taskqueue.add(queue_name='biq', url='/auto/bi/TaskLagerbestandExportieren')
        self.response.out.write('queued')


# Der Folgende Handler ist der Versuch eines generichen Dateiexport-Handlers.
class TaskLagerbestandExportieren(gaetk.handler.BasicHandler):
    """BiHistorischerArtikelbestandTertial exportieren."""
    filename = '/gs/hudora-bi/BiHistorischerArtikelbestand.csv'
    # Wie viele Datensätze auf einmal erzeugen
    batchsize = 250

    def get_query(self):
        """Biefert die Query, deren Daten exportiert werden sollen."""
        return BiHistorischerArtikelbestand.all().order('artnr')

    def create_header(self, output):
        """Erzeugt eine oder mehrere Headerzeilen in `output`"""
        output.writerow(['# artnr', 'tertial', 'datum',
                         'menge_avg', 'menge_median', 'menge_min', 'menge_max',
                         'menge_fakturiert'])
        output.writerow(['# stand:', datetime.date.today().strftime('%Y-%m-%d')])

    def create_row(self, output, data):
        """Erzeugt eine einzelne Zeile im Output."""
        bestand = data
        if not (bestand.menge_avg == 0 and bestand.menge_fakturiert == 0):
            try:
                # es gab ersthafte Bewegungen.
                output.writerow([bestand.artnr, tertial(bestand.datum), bestand.datum,
                                 bestand.menge_avg, bestand.menge_median,
                                 bestand.menge_min, bestand.menge_max,
                                 bestand.menge_fakturiert])
            except Exception, msg:
                logging.error("error %s", msg)
                return  # hier gibt es nichts mehr zu retten und eine Wiederholung des Tasks
                        # hilft in der Praxis nicht.

    def create_file(self):
        """Generiert die Ausgabedatei; zB in Google Cloud Storage."""
        return files.gs.create(self.filename, mime_type='application/octet-stream', acl='project-private')

    def create_writer(self, fd):
        """Generiert den Ausgabedatenstrom aus fd."""
        return csv.writer(fd, dialect='excel')

    def post(self):
        """Erzeugt eine Ausgabedatei und schreibt in den weiteren Aufruf `batchsize` Entities."""
        query = self.get_query()
        writable_file_name = self.request.get('writable_file_name')
        # First call - create File
        if not writable_file_name:
            writable_file_name = self.create_file()
            logging.info("creating file %s", self.filename)
        # Warnung: you HAVE TO use a `with` construct.
        with files.open(writable_file_name, 'a') as fd:
            writer = self.create_writer(fd)
            last_cursor = self.request.get('cursor')
            if not last_cursor:
                # New File, create Header
                self.create_header(writer)
            else:
                # Existing file, continue where loft of
                query.with_cursor(last_cursor)

            data = list(query.fetch(self.batchsize))
            for row in data:
                self.create_row(writer, row)

        if len(data) == self.batchsize:
            # Continue processing
            logging.info('requeued to %s, batchsize = %d', self.request.path, self.batchsize)
            taskqueue.add(queue_name='biq', url=self.request.path,
                          params=dict(cursor=query.cursor(), writable_file_name=writable_file_name))
        else:
            # Finished. Finalize the file. # `fd.close(finalize=True)` seems not to work
            logging.info("finalizing %s")
            try:
                files.finalize(writable_file_name)
            except Exception, msg:
                logging.error("error %s", msg)
                return  # hier gibt es nichts mehr zu retten und eine Wiederholung des Tasks
                # hilft in der Praxis nicht.
            logging.info('done with final segment of %d rows', len(data))


class TaskWertschoepfungExportieren(TaskLagerbestandExportieren):
    """BiWertschoepfungArtikelTertial exportieren."""
    filename = '/gs/hudora-bi/BiWertschoepfungArtikelTertial.csv'
    # Wie viele Datensätze auf einmal erzeugen
    batchsize = 250

    def get_query(self):
        """Biefert die Query, deren Daten exportiert werden sollen."""
        return BiWertschoepfungArtikelTertial.all().order('artnr')

    def create_header(self, output):
        #erzeugt eine oder mehrere Headerzeilen in `output`
        output.writerow(['# artnr', 'tertial', 'datum',
                         'rechnungen', 'menge_sum', 'rechnungsbetrag_sum', 'umsatzerloes_sum',
                         'materialeinsatz1_sum', 'materialeinsatz2_sum', 'materialeinsatz3_sum'])
        output.writerow(['# stand:', datetime.date.today().strftime('%Y-%m-%d')])

    def create_row(self, output, data):
        """Erzeugt eine einzelne Zeile im Output."""
        if data.rechnungen:
            # es gab ersthafte Bewegungen.
            output.writerow([data.artnr, tertial(data.datum), data.datum,
                             data.rechnungen, data.menge_sum, data.rechnungsbetrag_sum, data.umsatzerloes_sum,
                             data.materialeinsatz1_sum, data.materialeinsatz2_sum, data.materialeinsatz3_sum])
