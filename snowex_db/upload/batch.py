"""
Module for storing and managing multiple file submissions to the database
"""

import glob
import time
from os.path import abspath, basename, expanduser, join

from snowex_db.interpretation import get_InSar_flight_comment
from snowex_db.metadata import read_InSar_annotation
from snowex_db.upload.rasters import UploadRaster
from snowex_db.utilities import get_logger



class BatchBase:
    """
    Base Class for uploading multiple files to the database. This class manages
    uploading, managing logging and timing, and reporting of errors

    Attributes:
        filenames: List of files to upload
        log: Logger object with colored logs installed.
        errors: List of tuple that contain filename, and exception thrown
                during uploaded
        uploaded: Integer of number of files that were successfully uploaded

    Functions:
        push: Wraps snowex_db.upload.UploadProfileData to submit data.
              Use debug=False to allow exceptions
        report: Log the final result of uploaded files, errors, time elapsed,
                etc.
    """

    UploaderClass = None

    def __init__(
            self, session, filenames, n_files=-1, debug=False, **kwargs
    ):
        """
        Args:
            filenames: List of valid files to be uploaded to the database
            session: The DB session object
            n_files: Integer number of files to upload (useful for testing),
                     Default=-1 (meaning all of the files)
            debug:  Boolean that allows exceptions when uploading files, when
                    True no exceptions are allowed. Default=True
            kwargs: Any keywords that can be passed along to the UploadProfile
                    Class. Any kwargs not recognized will be merged into a
                    comment.
        """
        self._session = session
        self.filenames = filenames
        self._kwargs = kwargs
        # Grab logger
        self.log = get_logger(__name__)

        # Performance tracking
        self.errors = []
        self.uploaded = 0
        self.n_files = n_files
        self.debug = debug

        self.log.info('Preparing to upload {} files...'.format(len(filenames)))

    def push(self):
        """
        Push all the data to the database while tracking errors. If the class
        is instantiated with debug=True exceptions will error out. Otherwise,
        any errors will be passed over and counted/reported
        """

        self.start = time.time()
        self.log.info('Uploading {} files to database...'
                      ''.format(len(self.filenames)))
        i = 0

        # Loop over a portion of files and upload them
        if self.n_files != -1:
            files = self.filenames[0:self.n_files]
        else:
            files = self.filenames

        for i, f in enumerate(files):
            # If were not debugging script allow exceptions and report them
            # later
            if not self.debug:
                try:
                    self._push_one(f, **self._kwargs)

                except Exception as e:
                    self.log.error('Error with {}'.format(f))
                    self.log.error(e)
                    self.errors.append((f, e))

            else:
                self._push_one(f, **self._kwargs)

        # Log the ending errors
        self.report(i + 1)

    def _push_one(self, f, **kwargs):
        """
        Manage what pushing a single file is to use with debug options.

        Args:
            f: valid file to upload
        """

        d = self.UploaderClass(self._session, f, **kwargs)

        # Submit the data to the database
        self.log.info('Submitting to database')
        d.submit()
        self.uploaded += 1

    def report(self, files_attempted):
        """
        Report timing and errors that occurred

        Args:
            files_attempted: Number of files attempted
        """
        self.log.info("{} / {} files uploaded.".format(self.uploaded,
                                                       files_attempted))

        if len(self.errors) > 0:
            self.log.error(
                '{} files failed to upload.'.format(len(self.errors)))
            self.log.error(
                'The following files failed with their corresponding errors:')

            for e in self.errors:
                self.log.error('\t{} - {}'.format(e[0], e[1]))

        self.log.info('Finished! Elapsed {:d}s\n'.format(
            int(time.time() - self.start)))
