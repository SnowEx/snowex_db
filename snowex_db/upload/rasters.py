from geoalchemy2.elements import RasterElement
import os
from subprocess import STDOUT, check_output
from pathlib import Path
from os.path import exists, join
from os import makedirs, remove
import boto3
import logging
from snowexsql.tables import ImageData


from snowexsql.db import get_table_attributes
from ..utilities import (assign_default_kwargs, get_file_creation_date,
                        get_logger)


LOG = logging.getLogger(__name__)


class COGHandler:
    """
    Class to convert TIFs to COGs, persist them, and generate the command to
    insert them into the db
    """
    AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-west-2")

    def __init__(self, tif_file, s3_bucket="m3w-snowex", s3_prefix="cogs",
                 cog_dir="./snowex_cog_storage", use_s3=True):
        """
        Args:
            tif_file: local or abs bath to file that will be persisted
            s3_bucket: optional s3 bucket name
            s3_prefix: optional s3 bucket prefix
            cog_dir: option local directory for storing cog files
            use_s3: boolean whether or not we persist files in S3
        """
        self.tif_file = Path(tif_file)
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.tmp_dir = Path(cog_dir).expanduser().absolute()
        self.use_s3 = use_s3
        if not self.tmp_dir.exists():
            LOG.info(f"Making directory {self.tmp_dir}")
            makedirs(self.tmp_dir)

        # state variables
        self._cog_path = None
        self._cog_uri = None
        self._key_name = None
        self._sql_path = None

    def create_cog(self, nodata=None):
        """
        Create a cloud optimized geotif from tif
        Args:
            nodata: no data value
        Returns:
            path to COG
        """
        cmd = [
            "gdal_translate",
            "-co", "COMPRESS=DEFLATE",
            "-co", "ZLEVEL=9",  # Use highest compression
            "-co", "PREDICTOR=2",  # Compression predictor
            "-co", "TILED=YES",  # Apply default (256x256) tiling
        ]
        if nodata is not None:
            cmd += ["-a_nodata", f"{nodata}"]

        output_file = Path(self.tmp_dir)\
            .joinpath(self.tif_file.name)\
            .with_suffix(".tif")
        cmd += [
            str(self.tif_file),  # Input file
            str(output_file)  # Output file
        ]
        LOG.info('Executing: {}'.format(' '.join(cmd)))
        check_output(cmd, stderr=STDOUT).decode('utf-8')
        self._cog_path = output_file
        return output_file

    def _remove_cog(self):
        """
        Delete COG file. This should be used of the files are persisted in S3
        """
        if self._cog_path.exists():
            remove(str(self._cog_path))
        else:
            raise RuntimeError(
                f"Cannot remove the COG {self._cog_path}"
                f" because it does not exist"
            )

    def persist_cog(self):
        """
        persist COG either locally or in S3
        Returns:
            S3 or local path
        """
        if exists(self._cog_path):
            if self.use_s3:
                self._key_name = join(self.s3_prefix, self._cog_path.name)
                LOG.info(f'Uploading {self._cog_path} to {self.s3_bucket}/{self._key_name}')
                s3 = boto3.resource('s3', region_name=self.AWS_REGION)
                s3.meta.client.upload_file(
                    str(self._cog_path),  # local file
                    self.s3_bucket,  # bucket name
                    self._key_name  # key name
                )
                result = Path(self.s3_bucket).joinpath(self._key_name)
                # delete cog since it is stored in S3
                self._remove_cog()
            else:
                # COG is already stored locally
                result = self._cog_path
        else:
            raise RuntimeError(
                f"Cannot upload COG {self._cog_path}"
                f" because it does not exist"
            )
        self._sql_path = result
        return result

    def to_sql_command(self, epsg, no_data=None):
        """
        Generate command to insert into database
        Args:
            epsg: string EPSG
            no_data: optional nodata value
        Returns:
            list of raster2pgsql command
        """
        # This produces a PSQL command with auto tiling
        if self.use_s3:
            cog_path = Path("/vsis3").joinpath(self._sql_path)
        else:
            cog_path = self._sql_path
        cmd = [
            'raster2pgsql', '-s', str(epsg),
            # '-I',
            '-t', '256x256',
            '-R', str(cog_path),
        ]

        # If nodata applied:
        if no_data is not None:
            cmd.append('-N')
            cmd.append(str(no_data))

        return cmd


class UploadRaster(object):
    """
    Class for uploading a single tifs to the database. Utilizes the raster2pgsql
    command and then parses it for delivery via python.
    """

    defaults = {
        'epsg': None,
        'no_data': None,
        'use_s3': True  # boolean whether or not we're storing files in S3
    }

    def __init__(self, filename, **kwargs):
        self.log = get_logger(__name__)
        self.filename = filename
        self.data = assign_default_kwargs(self, kwargs, self.defaults)
        self.date_accessed = get_file_creation_date(self.filename)

    def submit(self, session):
        """
        Submit the data to the db using ORM. This uses out_db rasters either
        locally or in AWS S3. Good articles below
            - https://www.crunchydata.com/blog/postgis-raster-and-crunchy-bridge
            - https://www.crunchydata.com/blog/waiting-for-postgis-3.2-secure-cloud-raster-access
            - https://postgis.net/docs/using_raster_dataman.html#RT_Cloud_Rasters
        """
        # Remove any invalid columns
        valid = get_table_attributes(ImageData)
        data = {k: v for k, v in self.data.items() if k in valid}
        data['date_accessed'] = self.date_accessed

        # create cog and upload to s3
        cog_handler = COGHandler(self.filename, use_s3=self.use_s3)
        cog_handler.create_cog()
        cog_handler.persist_cog()
        cmd = cog_handler.to_sql_command(
            self.epsg, no_data=self.no_data
        )
        self.log.debug('Executing: {}'.format(' '.join(cmd)))
        s = check_output(cmd, stderr=STDOUT).decode('utf-8')

        # Split the SQL command at values (' which is the start of every one
        tiles = s.split("VALUES ('")[1:]
        if len(tiles) > 1:
            # -1 because the first element is not a
            self.log.info(
                'Raster is split into {:,} tiles for uploading...'.format(
                    len(tiles)))

        # Allow for tiling, the first split is always psql statement we don't
        # need
        for t in tiles:
            v = t.split("'::")[0]
            raster = RasterElement(v)
            data['raster'] = raster
            r = ImageData(**data)
            session.add(r)
            session.commit()
