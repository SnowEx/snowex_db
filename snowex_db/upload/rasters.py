import os
import shlex

from geoalchemy2.elements import RasterElement
from subprocess import STDOUT, check_output
import logging
from snowexsql.tables import (
    Campaign, DOI, Instrument, MeasurementType, Observer, ImageObservation,
    ImageData,
)
from .base import BaseUpload
from .cog_handler import COGHandler
from ..utilities import (get_logger)


LOG = logging.getLogger(__name__)


def ensure_sso_env(profile: str) -> dict:
    """
    Ensure AWS_* environment vars are set using an SSO profile.
    Requires awscli v2 and that you've already done `aws sso login --profile <profile>`
    (this function can also try to call it).
    Returns the dict of vars it set.
    """
    env_updates = {}

    # 1) Make sure the CLI reads your shared config files
    os.environ.setdefault("AWS_SDK_LOAD_CONFIG", "1")
    os.environ.setdefault("AWS_PROFILE", profile)

    # 2) Export short-lived creds as env lines
    cmd = f"aws configure export-credentials --profile {shlex.quote(profile)} --format env"
    out = check_output(cmd, shell=True, text=True)

    # 3) Parse lines like: export AWS_ACCESS_KEY_ID=..., etc.
    for line in out.splitlines():
        line = line.strip()
        if not line.startswith("export "):
            continue
        k, v = line[len("export "):].split("=", 1)
        os.environ[k] = v
        env_updates[k] = v

    # (Optional) region if you know it
    os.environ.setdefault("AWS_REGION", "us-west-2")
    env_updates.setdefault("AWS_REGION", "us-west-2")

    return env_updates


class UploadRaster(BaseUpload):
    """
    Class for uploading a single tifs to the database. Utilizes the raster2pgsql
    command and then parses it for delivery via python.
    """
    TABLE_CLASS = ImageData

    def __init__(
            self, session, filename, epsg,
            use_s3=True, no_data=None, cog_dir="/data/uploads/snowex_cog_storage",
            doi=None, measurement_type=None, units=None,
            date=None, use_sso=False,
            **kwargs
    ):
        """
        Initialize the UploadRaster object.
        Args:
            session: sqlalchemy session object
            filename: path to the file
            epsg: integer epsg code
            measurement_type: type of measurement, e.g. 'depth'
            use_s3: whether to use S3 for storing COGs
            no_data: optional no data value for the raster
            cog_dir: directory to store COGs if not using S3
            doi: optional DOI for the measurement
            measurement_type: type of measurement
            units: units of the measurement, e.g. 'meters'
            date_obj: date object for the measurement
            comments: measurement description
            use_sso: whether to use SSO for AWS access. In this case
                we need to export our credentials to env variables first.
            **kwargs:
        """
        super().__init__()

        self._session = session
        self.log = get_logger(__name__)
        self.filename = filename
        self._measurement_type = measurement_type
        self._epsg = epsg
        self._no_data = no_data
        self._units = units
        self.use_s3 = use_s3
        self._use_sso = use_sso
        self._cog_dir = cog_dir
        self._doi = doi
        self._date_obj = date

        self._campaign_name = kwargs.get("campaign_name")
        # Is this file for derived measurements
        self._derived = kwargs.get("derived", False)

        self._instrument = kwargs.get("instrument")
        self._instrument_model = kwargs.get("instrument_model")
        self._comments = kwargs.get("comments")

        # Observer name for the whole file
        self._observer = kwargs.get("observer")
        # assign name to each measurement if given
        self._name = kwargs.get("name")

    def _store_cog(self):
        """
        Store the COG in S3 or locally and generate the SQL command to insert.
        Execute the command to insert the COG into the database.
        Good articles below
            - https://www.crunchydata.com/blog/postgis-raster-and-crunchy-bridge
            - https://www.crunchydata.com/blog/waiting-for-postgis-3.2-secure-cloud-raster-access
            - https://postgis.net/docs/using_raster_dataman.html#RT_Cloud_Rasters

        Returns:
            The tiles text we will store in the database
        """
        # create cog and upload to s3
        cog_handler = COGHandler(
            self.filename, use_s3=self.use_s3, cog_dir=self._cog_dir,
        )
        cog_handler.create_cog()
        # Store the cog either in s3 or locally
        cog_handler.persist_cog()
        cmd = cog_handler.to_sql_command(
            self._epsg, no_data=self._no_data
        )
        if self._use_sso:
            ensure_sso_env("default")
        self.log.debug('Executing: {}'.format(' '.join(cmd)))
        s = check_output(cmd, stderr=STDOUT).decode('utf-8')

        # Split the SQL command at values (' which is the start of every one
        # Allow for tiling, the first split is always psql statement we don't
        # need
        tiles = s.split("VALUES ('")[1:]
        if len(tiles) > 1:
            # -1 because the first element is not a
            self.log.info(
                'Raster is split into {:,} tiles for uploading...'.format(
                    len(tiles)))
        return tiles

    def submit(self):
        """
        Submit the data to the db using ORM. This uses out_db rasters either
        locally or in AWS S3.
        """

        # Store the cogs
        tiles = self._store_cog()

        # Store the instrument object
        instrument = self._check_or_add_object(
            self._session, Instrument, dict(
                name=self._instrument,
                model=self._instrument_model
            )
        )
        # Add measurement type
        measurement_obj = self._check_or_add_object(
            # Add units and 'derived' flag for the measurement
            self._session, MeasurementType, dict(
                name=self._measurement_type,
                units=self._units,
                derived=self._derived
            )
        )
        # Construct a measurement name
        measurement_name = (self._name or "") + f"_{self._instrument}"
        if self._instrument_model:
            measurement_name += f"_{self._instrument_model}"
        measurement_name += f"_{self._measurement_type}"

        doi = self._check_or_add_object(
            self._session, DOI, dict(doi=self._doi)
        )
        campaign = self._check_or_add_object(
            self._session, Campaign, dict(name=self._campaign_name)
        )
        observer = self._check_or_add_object(
            self._session, Observer, dict(name=self._observer)
        )
        observation = self._check_or_add_object(
            self._session, ImageObservation, dict(
                name=measurement_name,
                date=self._date_obj,
                instrument_id=instrument.id,
                doi_id=doi.id,
            ),
            object_kwargs=dict(
                name=measurement_name,
                description=self._comments,
                date=self._date_obj,
                instrument_id=instrument.id,
                doi_id=doi.id,
                # type=row["type"],  # THIS TYPE IS RESERVED FOR POLYMORPHIC STUFF
                observer=observer,
                campaign_id=campaign.id,
            )
        )

        # TODO: can probably do this in parallel because everything else
        #   should be created
        for t in tiles:
            v = t.split("'::")[0]
            raster = RasterElement(v)
            new_entry = self.TABLE_CLASS(
                raster=raster,
                observation=observation,
                measurement_type_id=measurement_obj.id,
            )
            self._session.add(new_entry)
        self._session.commit()
