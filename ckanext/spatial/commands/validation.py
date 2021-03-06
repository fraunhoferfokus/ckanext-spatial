import sys
import re
import os
from pprint import pprint
import logging

from lxml import etree

from ckan.lib.cli import CkanCommand

log = logging.getLogger(__name__)

class Validation(CkanCommand):
    '''Validation commands

    Usage:
        validation report [package-name]
            Performs validation on the harvested metadata, either for all
            packages or the one specified.

        validation report-csv <filename>.csv
            Performs validation on all the harvested metadata in the db and
            writes a report in CSV format to the given filepath.
      
        validation file <filename>.xml
            Performs validation on the given metadata file.
    '''
    summary = __doc__.split('\n')[0]
    usage = __doc__
    max_args = 3
    min_args = 0

    def command(self):
        if not self.args or self.args[0] in ['--help', '-h', 'help']:
            print self.usage
            sys.exit(1)

        self._load_config()

        cmd = self.args[0]
        if cmd == 'report':
            self.report()
        elif cmd == 'report-csv':
            self.report_csv()
        elif cmd == 'file':
            self.validate_file()
        else:
            print 'Command %s not recognized' % cmd

    def report(self):
        from ckan import model
        from ckanext.harvest.model import HarvestObject
        from ckanext.spatial.lib.reports import validation_report

        if len(self.args) >= 2:
            package_ref = unicode(self.args[1])
            pkg = model.Package.get(package_ref)
            if not pkg:
                print 'Package ref "%s" not recognised' % package_ref
                sys.exit(1)
        else:
            pkg = None

        report = validation_report(package_id=pkg.id)
        for row in report.get_rows_html_formatted():
            print
            for i, col_name in enumerate(report.column_names):
                print '  %s: %s' % (col_name, row[i])

    def validate_file(self):
        from ckanext.spatial.harvesters import SpatialHarvester

        if len(self.args) > 2:
            print 'Too many parameters %i' % len(self.args)
            sys.exit(1)
        if len(self.args) < 2:
            print 'Not enough parameters %i' % len(self.args)
            sys.exit(1)
        metadata_filepath = self.args[1]
        if not os.path.exists(metadata_filepath):
            print 'Filepath %s not found' % metadata_filepath
            sys.exit(1)
        with open(metadata_filepath, 'rb') as f:
            metadata_xml = f.read()

        validators = SpatialHarvester()._get_validator()
        print 'Validators: %r' % validators.profiles
        xml = etree.fromstring(metadata_xml.encode("utf-8"))
        valid, errors = validators.is_valid(xml)
        print 'Valid: %s' % valid
        if not valid:
            print 'Errors:'
            print pprint(errors)

    def report_csv(self):
        from ckanext.spatial.lib.reports import validation_report
        if len(self.args) != 2:
            print 'Wrong number of arguments'
            sys.exit(1)
        csv_filepath = self.args[1]
        report = validation_report()
        with open(csv_filepath, 'wb') as f:
            f.write(report.get_csv())
