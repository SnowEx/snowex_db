import shutil
from datetime import date
from pathlib import Path
from geoalchemy2.shape import to_shape
from geoalchemy2.types import Raster
from shapely.geometry import Point
from snowexsql.conversions import raster_to_rasterio
from snowexsql.functions import ST_PixelAsPoint
import numpy as np
import pytest
from snowexsql.tables.campaign_observation import CampaignObservation
from sqlalchemy import func

from snowexsql.tables import ImageData, MeasurementType, Instrument, Campaign, \
    DOI, ImageObservation

from snowex_db.upload.raster_mapping import rasters_from_annotation
from snowex_db.upload.rasters import UploadRaster
from tests.helpers import WithUploadedFile
from tests.sql_test_base import TableTestBase


class TestUploadRasters(TableTestBase, WithUploadedFile):
    """
    Test that a density file is uploaded correctly including sample
    averaging for the main value.
    """
    kwargs = {
        'timezone': 'MST',
        'doi': "some_raster_doi",
        "campaign_name": "Grand Mesa",
        "name": "some_uavsar",
        'observer': 'UAVSAR team, JPL',
        'tiled': True,
        'instrument': 'UAVSAR, L-band InSAR',
    }
    UploaderClass = UploadRaster
    TableClass = ImageData

    @pytest.fixture(scope="class")
    def tmp_dir(self, data_dir):
        dest = Path(data_dir).joinpath("tmp_rasters")
        dest.mkdir(exist_ok=True)
        yield dest
        shutil.rmtree(dest)

    @pytest.fixture(scope="class")
    def uploaded_file(self, session, data_dir, tmp_dir):
        for raster_metadata, raster_path in rasters_from_annotation(
                Path(data_dir).joinpath("uavsar.ann"),
                Path(data_dir).joinpath("uavsar"), **self.kwargs
        ):
            rs = UploadRaster(
                session, raster_path, 26911,
                cog_dir=tmp_dir, use_s3=False, use_sso=False,
                **raster_metadata
            )
            rs.submit()

    @pytest.mark.usefixtures("uploaded_file")
    def test_measurement_type(self):
        record = self.get_records(MeasurementType, "name", "insar interferogram real")
        assert len(record) == 1
        record = record[0]
        assert record.units == 'Linear Power and Phase in Radians'
        assert record.derived is False

    @pytest.mark.usefixtures("uploaded_file")
    @pytest.mark.parametrize(
        "name, model", [
            ("UAVSAR, L-band InSAR", None),
        ]
    )
    def test_instrument(self, name, model):
        record = self.get_records(Instrument, "name", name)
        assert len(record) == 1
        record = record[0]
        assert record.model == model

    @pytest.mark.usefixtures("uploaded_file")
    @pytest.mark.parametrize(
        "name, count, instrument_name",
        [
            ("some_uavsar_UAVSAR, L-band InSAR_insar correlation", 1, "UAVSAR, L-band InSAR")
        ],
    )
    def test_campaign_observation_record(self, name, count, instrument_name):
        names = self.get_records(CampaignObservation, "name", name)
        assert len(names) == count

        # Check relationship mapping
        for record in names:
            assert record.campaign.name == "Grand Mesa"
            assert record.doi.doi == "some_raster_doi"
            assert record.observer.name == 'UAVSAR team, JPL'
            assert record.instrument.name == instrument_name

    @pytest.mark.usefixtures("uploaded_file")
    @pytest.mark.parametrize(
        "date, count",
        [
            (date(2020, 2, 12), 4),  # 4 components
            (date(2020, 2, 4), 0),
        ],
    )
    def test_point_observation(self, date, count):
        record = self.get_records(ImageObservation, "date", date)
        assert len(record) == count

    @pytest.mark.parametrize(
        "table, attribute, expected_value", [
            (Campaign, "name", "Grand Mesa"),
            (DOI, "doi", "some_raster_doi"),
        ]
    )
    def test_metadata(self, table, attribute, expected_value, uploaded_file):
        self._check_metadata(table, attribute, expected_value)

    @pytest.mark.usefixtures("uploaded_file")
    def test_record_count(self):
        """
        Check that all entries in the raster(s) made it into the database
        """
        # This is tiled so we have multiple entries
        assert self.check_count("insar interferogram real") == 9

    @pytest.mark.usefixtures("uploaded_file")
    @pytest.mark.parametrize("data_name, kw", [
        # Check the single pass products have a few key words
        ('amplitude', ['duration', 'overpass', 'polarization', 'dem']),
        # Check the derived products all have a ref to 1st and 2nd overpass in addition to the others
        ('correlation',
         ['duration', 'overpass', '1st', '2nd', 'polarization', 'dem']),
        ('interferogram real',
         ['duration', 'overpass', '1st', '2nd', 'polarization', 'dem']),
        ('interferogram imaginary',
         ['duration', 'overpass', '1st', '2nd', 'polarization', 'dem']),
    ])
    def test_description_generation(self, data_name, kw):
        """
        Asserts each kw is found in the description of the data
        """
        name = 'insar {}'.format(data_name)
        records = self._session.query(ImageObservation.description).join(
            ImageData
        ).join(
            ImageData.measurement_type
        ).filter(
            MeasurementType.name == name
        ).all()

        for k in kw:
            assert k in records[0][0].lower()

    # Section for checking the actual data

    @pytest.mark.usefixtures("uploaded_file")
    def test_tiled_raster_count(self):
        """
        Test two rasters uploaded
        """
        records = self._session.query(ImageData.id).all()
        assert len(records) == 45  # 9 tiles * 5 rasters

    @pytest.mark.usefixtures("uploaded_file")
    def test_tiled_raster_size(self):
        """
        Tiled raster should be 500x500 in most cases (can be smaller to fit domains)
        """
        rasters = self._session.query(func.ST_AsTiff(ImageData.raster)).all()
        datasets = raster_to_rasterio(rasters)

        for d in datasets:
            assert d.width <= 256
            assert d.height <= 256

    @pytest.mark.usefixtures("uploaded_file")
    def test_raster_point_retrieval(self):
        """
        Test we can retrieve coordinates of a point from the database
        """

        # Get the first pixel as a point
        records = self._session.query(ST_PixelAsPoint(ImageData.raster, 1, 1)).limit(1).all()
        received = to_shape(records[0][0])
        expected = Point(748446.1945536422, 4328702.971977075)

        # Convert geom to shapely object and compare
        np.testing.assert_almost_equal(received.x, expected.x, 6)
        np.testing.assert_almost_equal(received.y, expected.y, 6)

    @pytest.mark.usefixtures("uploaded_file")
    def test_raster_union(self):
        """
        Test we can retrieve coordinates of a point from the database
        """

        # Get the first pixel as a point
        rasters = self._session.query(func.ST_AsTiff(func.ST_Union(ImageData.raster, type_=Raster))).all()
        assert len(rasters) == 1

    @pytest.mark.usefixtures("uploaded_file")
    def test_raster_union2(self):
        """
        Test we can retrieve coordinates of a point from the database
        """

        # Get the first pixel as a point
        merged = self._session.query(
            func.ST_Union(ImageData.raster, type_=Raster)
        ).filter(
            ImageData.id.in_([1, 2])).all()
        assert len(merged) == 1
