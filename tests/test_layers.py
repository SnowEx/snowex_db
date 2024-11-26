from datetime import datetime, timezone
from shapely.wkb import loads as load_wkb
from shapely.wkt import loads as load_wkt
import numpy as np
import pytest
import os

from geoalchemy2 import WKTElement
from snowexsql.tables import (
    LayerData, Campaign, Instrument, Observer, Site, MeasurementType
)
from tests.db_setup import DBSetup, db_session_with_credentials

from snowex_db.upload.layers import UploadProfileData

from .sql_test_base import TableTestBase


class WithUploadedFile(DBSetup):
    UploaderClass = UploadProfileData
    kwargs = {}

    def upload_file(self, fname):
        with db_session_with_credentials(
                self.database_name(), self.CREDENTIAL_FILE
        ) as (session, engine):
            u = self.UploaderClass(fname, **self.kwargs)

            # Allow for batches and single upload
            if 'batch' in self.UploaderClass.__name__.lower():
                u.push()
            else:
                u.submit(session)

    def get_value(self, table, attribute):
        with db_session_with_credentials(
                self.database_name(), self.CREDENTIAL_FILE
        ) as (session, engine):
            obj = getattr(table, attribute)
            result = session.query(obj).all()
        return result[0][0]

    def get_values(self, table, attribute):
        with db_session_with_credentials(
                self.database_name(), self.CREDENTIAL_FILE
        ) as (session, engine):
            obj = getattr(table, attribute)
            result = session.query(obj).all()
        return [r[0] for r in result]


class TestStratigraphyProfile(TableTestBase, WithUploadedFile):
    """
    Test that all the profiles from the Stratigraphy file were uploaded and
    have integrity
    """

    kwargs = {'timezone': 'MST'}
    UploaderClass = UploadProfileData
    TableClass = LayerData

    @pytest.fixture(scope="class")
    def uploaded_file(self, db, data_dir):
        self.upload_file(str(data_dir.joinpath("stratigraphy.csv")))

    @pytest.mark.parametrize(
        "table, attribute, expected_value", [
            (Site, "name", "COGM1N20_20200205"),
            (Site, "datetime", datetime(
                2020, 2, 5, 20, 30, tzinfo=timezone.utc)
             ),
            (Site, "geom", WKTElement(
                'POINT (-108.1894813320662 39.031261970372725)', srid=4326)
             ),
            (Campaign, "name", "Grand Mesa"),
            (Instrument, "name", None),
        ]
    )
    def test_metadata(self, table, attribute, expected_value, uploaded_file):
        result = self.get_value(table, attribute)
        if attribute == "geom":
            # Check geometry equals expected
            geom_from_wkb = load_wkb(bytes(result.data))
            geom_from_wkt = load_wkt(expected_value.data)

            assert geom_from_wkb.equals(geom_from_wkt)
        else:
            assert result == expected_value

    @pytest.mark.parametrize(
        "data_name, attribute_to_check,"
        " filter_attribute, filter_value, expected",
        [
            ('hand_hardness', 'value', 'depth', 30, ["4F"]),
            ('grain_size', 'value', 'depth', 35, ["< 1 mm"]),
            ('grain_type', 'value', 'depth', 17, ["FC"]),
            ('manual_wetness', 'value', 'depth', 17, ["D"]),
            ('hand_hardness', 'comments', 'depth', 17, ["Cups"]),
        ]
    )
    def test_value(
            self, data_name, attribute_to_check,
            filter_attribute, filter_value, expected, uploaded_file
    ):
        self.check_value(
            data_name, attribute_to_check,
            filter_attribute, filter_value, expected,
        )

    @pytest.mark.parametrize(
        "data_name, expected", [
            ("hand_hardness", 5)
        ]
    )
    def test_count(self, data_name, expected, uploaded_file):
        n = self.check_count(data_name)
        assert n == expected

    @pytest.mark.parametrize(
        "data_name, attribute_to_count, expected", [
            ("hand_hardness", "site_id", 1),
            ("manual_wetness", "value", 1),
            ("hand_hardness", "value", 3),
            ("grain_type", "value", 2),
            ("grain_size", "value", 2),
        ]
    )
    def test_unique_count(self, data_name, attribute_to_count, expected,
                          uploaded_file):
        self.check_unique_count(
            data_name, attribute_to_count, expected
        )


class TestDensityProfile(TableTestBase, WithUploadedFile):
    """
    Test that a density file is uploaded correctly including sample
    averaging for the main value.
    """

    kwargs = {
        'timezone': 'MST',
        'instrument': 'kelly cutter',
        'doi': "somedoi"
    }
    UploaderClass = UploadProfileData
    TableClass = LayerData

    @pytest.fixture(scope="class")
    def uploaded_file(self, db, data_dir):
        self.upload_file(str(data_dir.joinpath("density.csv")))

    @pytest.mark.parametrize(
        "table, attribute, expected_value", [
            (Site, "name", "COGM1N20_20200205"),
            (Site, "datetime", datetime(
                2020, 2, 5, 20, 30, tzinfo=timezone.utc)
             ),
            (Site, "geom", WKTElement(
                'POINT (-108.1894813320662 39.031261970372725)', srid=4326)
             ),
            (Campaign, "name", "Grand Mesa"),
            (Instrument, "name", "kelly cutter"),
        ]
    )
    def test_metadata(self, table, attribute, expected_value, uploaded_file):
        result = self.get_value(table, attribute)
        if attribute == "geom":
            # Check geometry equals expected
            geom_from_wkb = load_wkb(bytes(result.data))
            geom_from_wkt = load_wkt(expected_value.data)

            assert geom_from_wkb.equals(geom_from_wkt)
        else:
            assert result == expected_value

    @pytest.mark.parametrize(
        "data_name, attribute_to_check, filter_attribute, filter_value, expected", [
            ('density', 'value', 'depth', 35, [190, 245, 'None']),
       ]
    )
    def test_value(
            self, data_name, attribute_to_check,
            filter_attribute, filter_value, expected, uploaded_file
    ):
        self.check_value(
            data_name, attribute_to_check,
            filter_attribute, filter_value, expected,
        )

    @pytest.mark.parametrize(
        "data_name, expected", [
            ("density", 12)
        ]
    )
    def test_count(self, data_name, expected, uploaded_file):
        n = self.check_count(data_name)
        assert n == expected

    @pytest.mark.parametrize(
        "data_name, attribute_to_count, expected", [
            ("density", "site_id", 1)
        ]
    )
    def test_unique_count(self, data_name, attribute_to_count, expected, uploaded_file):
        self.check_unique_count(
            data_name, attribute_to_count, expected
        )


class TestDensityAlaska(TableTestBase, WithUploadedFile):
    """
    Test that the logic carries over to Alaska data
    """
    kwargs = {
        'timezone': 'US/Alaska',
        'header_sep': ':',
    }
    UploaderClass = UploadProfileData
    TableClass = LayerData

    @pytest.fixture(scope="class")
    def uploaded_file(self, db, data_dir):
        self.upload_file(str(data_dir.joinpath(
            "SnowEx23_SnowPits_AKIOP_454SB_20230316_density_v01.csv"
        )))

    @pytest.mark.parametrize(
        "table, attribute, expected_value", [
            (Site, "name", "SB454"),
            (Site, "datetime", datetime(
                2023, 3, 16, 18, 25, tzinfo=timezone.utc)
             ),
            (Site, "geom", WKTElement(
                'POINT (-148.31829 64.70955)', srid=4326)
             ),
            (Campaign, "name", "Fairbanks"),
            (Instrument, "name", None),
        ]
    )
    def test_metadata(self, table, attribute, expected_value, uploaded_file):
        result = self.get_value(table, attribute)
        if attribute == "geom":
            # Check geometry equals expected
            geom_from_wkb = load_wkb(bytes(result.data))
            geom_from_wkt = load_wkt(expected_value.data)

            assert geom_from_wkb.equals(geom_from_wkt)
        else:
            assert result == expected_value

    @pytest.mark.parametrize(
        "data_name, expected", [
            ("density", 15)
        ]
    )
    def test_count(self, data_name, expected, uploaded_file):
        n = self.check_count(data_name)
        assert n == expected


class TestLWCProfile(TableTestBase, WithUploadedFile):
    """
    Test the permittivity file is uploaded correctly
    """

    kwargs = {'timezone': 'MST'}
    UploaderClass = UploadProfileData
    TableClass = LayerData

    @pytest.fixture(scope="class")
    def uploaded_file(self, db, data_dir):
        self.upload_file(str(data_dir.joinpath("LWC.csv")))

    @pytest.mark.parametrize(
        "table, attribute, expected_value", [
            (Site, "name", "COGM1N20_20200205"),
            (Site, "datetime", datetime(
                2020, 2, 5, 20, 30, tzinfo=timezone.utc)
             ),
            (Site, "geom", WKTElement(
                'POINT (-108.1894813320662 39.031261970372725)', srid=4326)
             ),
            (Campaign, "name", "Grand Mesa"),
            (Instrument, "name", None),
        ]
    )
    def test_metadata(self, table, attribute, expected_value, uploaded_file):
        result = self.get_value(table, attribute)
        if attribute == "geom":
            # Check geometry equals expected
            geom_from_wkb = load_wkb(bytes(result.data))
            geom_from_wkt = load_wkt(expected_value.data)

            assert geom_from_wkb.equals(geom_from_wkt)
        else:
            assert result == expected_value

    @pytest.mark.parametrize(
        "data_name, attribute_to_check, filter_attribute, filter_value, expected", [
            ('permittivity', 'value', 'depth', 27, [1.372, 1.35]),
       ]
    )
    def test_value(
            self, data_name, attribute_to_check,
            filter_attribute, filter_value, expected, uploaded_file
    ):
        self.check_value(
            data_name, attribute_to_check,
            filter_attribute, filter_value, expected,
        )

    @pytest.mark.parametrize(
        "data_name, expected", [
            ("permittivity", 8)
        ]
    )
    def test_count(self, data_name, expected, uploaded_file):
        n = self.check_count(data_name)
        assert n == expected

    @pytest.mark.parametrize(
        "data_name, attribute_to_count, expected", [
            ("permittivity", "site_id", 1)
        ]
    )
    def test_unique_count(self, data_name, attribute_to_count, expected, uploaded_file):
        self.check_unique_count(
            data_name, attribute_to_count, expected
        )


class TestLWCProfileB(TableTestBase, WithUploadedFile):
    """
    Test the permittivity file is uploaded correctly
    """

    kwargs = {'timezone': 'MST'}
    UploaderClass = UploadProfileData
    TableClass = LayerData

    @pytest.fixture(scope="class")
    def uploaded_file(self, db, data_dir):
        self.upload_file(str(data_dir.joinpath("LWC2.csv")))

    @pytest.mark.parametrize(
        "table, attribute, expected_value", [
            (Site, "name", "COGMST_20200312"),
            (Site, "datetime", datetime(
                2020, 3, 12, 21, 45, tzinfo=timezone.utc)
             ),
            (Site, "geom", WKTElement(
                'POINT (-108.06310597266031 39.04495658046074)', srid=4326)
             ),
            (Campaign, "name", "Grand Mesa"),
            (Instrument, "name", None),
        ]
    )
    def test_metadata(self, table, attribute, expected_value, uploaded_file):
        result = self.get_value(table, attribute)
        if attribute == "geom":
            # Check geometry equals expected
            geom_from_wkb = load_wkb(bytes(result.data))
            geom_from_wkt = load_wkt(expected_value.data)

            assert geom_from_wkb.equals(geom_from_wkt)
        else:
            assert result == expected_value

    @pytest.mark.parametrize(
        "data_name, attribute_to_check, filter_attribute, filter_value, expected",
        [
            ('permittivity', 'value', 'depth', 73, [1.507, 1.521]),
            ('liquid_water_content', 'value', 'depth', 15, [0.1, 0.0]),
            ('density', 'value', 'depth', 83, [164.5])
        ]
    )
    def test_value(
            self, data_name, attribute_to_check,
            filter_attribute, filter_value, expected, uploaded_file
    ):
        self.check_value(
            data_name, attribute_to_check,
            filter_attribute, filter_value, expected,
        )

    @pytest.mark.parametrize(
        "data_name, expected", [
            ("permittivity", 16),
            ('liquid_water_content', 16),
            ('density', 8)
        ]
    )
    def test_count(self, data_name, expected, uploaded_file):
        n = self.check_count(data_name)
        assert n == expected

    @pytest.mark.parametrize(
        "data_name, attribute_to_count, expected", [
            ("permittivity", "site_id", 1),
            ("liquid_water_content", "site_id", 1),
            ("density", "site_id", 1),
        ]
    )
    def test_unique_count(self, data_name, attribute_to_count, expected,
                          uploaded_file):
        self.check_unique_count(
            data_name, attribute_to_count, expected
        )


class TestTemperatureProfile(TableTestBase, WithUploadedFile):
    """
    Test that a temperature profile is uploaded to the DB correctly
    """

    kwargs = {'in_timezone': 'MST'}
    UploaderClass = UploadProfileData
    TableClass = LayerData

    @pytest.fixture(scope="class")
    def uploaded_file(self, db, data_dir):
        self.upload_file(str(data_dir.joinpath("temperature.csv")))

    @pytest.mark.parametrize(
        "table, attribute, expected_value", [
            (Site, "name", "COGM1N20_20200205"),
            (Site, "datetime", datetime(
                2020, 2, 5, 20, 30, tzinfo=timezone.utc)
             ),
            (Site, "geom", WKTElement(
                'POINT (-108.1894813320662 39.031261970372725)', srid=4326)
             ),
            (Campaign, "name", "Grand Mesa"),
            (Instrument, "name", None),
        ]
    )
    def test_metadata(self, table, attribute, expected_value, uploaded_file):
        result = self.get_value(table, attribute)
        if attribute == "geom":
            # Check geometry equals expected
            geom_from_wkb = load_wkb(bytes(result.data))
            geom_from_wkt = load_wkt(expected_value.data)

            assert geom_from_wkb.equals(geom_from_wkt)
        else:
            assert result == expected_value

    @pytest.mark.parametrize(
        "data_name, attribute_to_check, filter_attribute, filter_value, expected",
        [
            ('snow_temperature', 'value', 'depth', 10, [-5.9]),
        ]
    )
    def test_value(
            self, data_name, attribute_to_check,
            filter_attribute, filter_value, expected, uploaded_file
    ):
        self.check_value(
            data_name, attribute_to_check,
            filter_attribute, filter_value, expected,
        )

    @pytest.mark.parametrize(
        "data_name, expected", [
            ("snow_temperature", 5),
        ]
    )
    def test_count(self, data_name, expected, uploaded_file):
        n = self.check_count(data_name)
        assert n == expected

    @pytest.mark.parametrize(
        "data_name, attribute_to_count, expected", [
            ("snow_temperature", "site_id", 1),
        ]
    )
    def test_unique_count(self, data_name, attribute_to_count, expected,
                          uploaded_file):
        self.check_unique_count(
            data_name, attribute_to_count, expected
        )


class TestSSAProfile(TableTestBase, WithUploadedFile):
    """
    Test that all profiles from an SSA file are uploaded correctly
    """

    kwargs = {'in_timezone': 'MST'}
    UploaderClass = UploadProfileData
    TableClass = LayerData

    @pytest.fixture(scope="class")
    def uploaded_file(self, db, data_dir):
        self.upload_file(str(data_dir.joinpath("SSA.csv")))

    @pytest.mark.parametrize(
        "table, attribute, expected_value", [
            (Site, "name", "COGM1N20_20200205"),
            (Site, "datetime", datetime(
                2020, 2, 5, 20, 40, tzinfo=timezone.utc)
             ),
            (Site, "geom", WKTElement(
                'POINT (-108.1894813320662 39.031261970372725)', srid=4326)
             ),
            (Campaign, "name", "Grand Mesa"),
            (Instrument, "name", None),
        ]
    )
    def test_metadata(self, table, attribute, expected_value, uploaded_file):
        result = self.get_value(table, attribute)
        if attribute == "geom":
            # Check geometry equals expected
            geom_from_wkb = load_wkb(bytes(result.data))
            geom_from_wkt = load_wkt(expected_value.data)

            assert geom_from_wkb.equals(geom_from_wkt)
        else:
            assert result == expected_value

    @pytest.mark.parametrize(
        "data_name, attribute_to_check, filter_attribute, "
        "filter_value, expected",
        [
            ('reflectance', 'value', 'depth', 10, [22.12]),
            ('specific_surface_area', 'value', 'depth', 35, [11.2]),
            ('equivalent_diameter', 'value', 'depth', 80, [0.1054]),
            ('sample_signal', 'value', 'depth', 10, [186.9]),
            ('sample_signal', 'comments', 'depth', 5, ["brush"])
        ]
    )
    def test_value(
            self, data_name, attribute_to_check,
            filter_attribute, filter_value, expected, uploaded_file
    ):
        self.check_value(
            data_name, attribute_to_check,
            filter_attribute, filter_value, expected,
        )

    @pytest.mark.parametrize(
        "data_name, expected", [
            ("reflectance", 16),
        ]
    )
    def test_count(self, data_name, expected, uploaded_file):
        n = self.check_count(data_name)
        assert n == expected

    @pytest.mark.parametrize(
        "data_name, attribute_to_count, expected", [
            ("reflectance", "site_id", 1),
        ]
    )
    def test_unique_count(self, data_name, attribute_to_count, expected,
                          uploaded_file):
        self.check_unique_count(
            data_name, attribute_to_count, expected
        )


class TestSMPProfile(TableTestBase, WithUploadedFile):
    """
    Test SMP profile is uploaded with all its attributes and valid data
    """

    kwargs = {
        'timezone': 'UTC',
        'units': 'Newtons',
        'header_sep': ':',
        'instrument': 'snowmicropen',
        'id': "COGM_Fakepitid123",
        'campaign_name': "Grand Mesa",
    }
    UploaderClass = UploadProfileData
    TableClass = LayerData

    @pytest.fixture(scope="class")
    def uploaded_file(self, db, data_dir):
        self.upload_file(str(data_dir.joinpath("S06M0874_2N12_20200131.CSV")))

    def test_instrument_id_comment(self):
        """
        Test that the SMP serial ID is added to the comment column of a smp profile inspit of an instrument being passed
        """
        result = self.session.query(LayerData.comments).limit(1).one()
        assert 'serial no. = 06' in result[0]

    def test_original_fname_comment(self):
        """
        Test that the original SMP file name is added to the comment column of a smp profile. This is done for
        provenance so users can determine the original dataset location
        """
        result = self.session.query(LayerData.comments).limit(1).one()
        assert f'fname = {os.path.basename(self.args[0])}' in result[0]

    @pytest.mark.parametrize(
        "table, attribute, expected_value", [
            (Site, "name", "COGM_Fakepitid123"),
            (Site, "datetime", datetime(
                2020, 1, 31, 22, 42, 14, 0, tzinfo=timezone.utc)
             ),
            (Site, "geom", WKTElement(
                'POINT (-108.16268920898438 39.03013229370117)', srid=4326)
             ),
            (Campaign, "name", "Grand Mesa"),
            (Instrument, "name", "snowmicropen"),
            (MeasurementType, "name", "force"),
            (MeasurementType, "units", "Newtons"),
        ]
    )
    def test_metadata(self, table, attribute, expected_value, uploaded_file):
        result = self.get_value(table, attribute)
        if attribute == "geom":
            # Check geometry equals expected
            geom_from_wkb = load_wkb(bytes(result.data))
            geom_from_wkt = load_wkt(expected_value.data)

            assert geom_from_wkb.equals(geom_from_wkt)
        else:
            assert result == expected_value

    @pytest.mark.parametrize(
        "data_name, attribute_to_check, filter_attribute, "
        "filter_value, expected",
        [
            ('force', 'value', 'depth', -53.17, [0.331]),
        ]
    )
    def test_value(
            self, data_name, attribute_to_check,
            filter_attribute, filter_value, expected, uploaded_file
    ):
        self.check_value(
            data_name, attribute_to_check,
            filter_attribute, filter_value, expected,
        )

    @pytest.mark.parametrize(
        "data_name, expected", [
            ("force", 154),
        ]
    )
    def test_count(self, data_name, expected, uploaded_file):
        n = self.check_count(data_name)
        assert n == expected

    @pytest.mark.parametrize(
        "data_name, attribute_to_count, expected", [
            ("force", "site_id", 1),
        ]
    )
    def test_unique_count(self, data_name, attribute_to_count, expected,
                          uploaded_file):
        self.check_unique_count(
            data_name, attribute_to_count, expected
        )


class TestEmptyProfile(TableTestBase, WithUploadedFile):
    """
    Test that a file with header info that doesn't have data (which
    happens on purpose) doesn't upload anything
    """

    args = []
    kwargs = {'timezone': 'MST'}
    UploaderClass = UploadProfileData
    TableClass = LayerData

    @pytest.fixture(scope="class")
    def uploaded_file(self, db, data_dir):
        self.upload_file(str(data_dir.joinpath('empty_data.csv')))

    @pytest.mark.parametrize(
        "data_name, expected", [
            ("hand_hardness", 0),
        ]
    )
    def test_count(self, data_name, expected, uploaded_file):
        n = self.check_count(data_name)
        assert n == expected


class TestMetadata(WithUploadedFile):
    """
    Test the large amount of metadata we get from the
    site details file
    """
    kwargs = {'timezone': 'MST'}
    UploaderClass = UploadProfileData
        
    @pytest.fixture
    def uploaded_site_details_file(self, db, data_dir):
        self.upload_file(str(data_dir.joinpath("site_details.csv")))

    @pytest.mark.parametrize(
        "table, attribute, expected_value", [
            (Site, "name", "COGM1N20_20200205"),
            (Site, "datetime", datetime(
                2020, 2, 5, 20, 30, tzinfo=timezone.utc)
             ),
            (Site, "geom", WKTElement(
                'POINT (-108.1894813320662 39.031261970372725)', srid=4326)
             ),
            (Campaign, "name", "Grand Mesa"),
            # (Site, "elevation", "COGM1N20_20200205"),
            (Site, "aspect", 180.0),
            (Site, "slope_angle", 5.0),
            (Site, "air_temp", np.nan),
            (Site, "total_depth", 35.0),
            (Site, "weather_description", "Sunny, cold, gusts"),
            (Site, "precip", "None"),
            (Site, "sky_cover", "Few (< 1/4 of sky)"),
            (Site, "wind", "Moderate"),
            (Site, "ground_condition", "Frozen"),
            (Site, "ground_roughness", "rough, rocks in places"),
            (Site, "ground_vegetation", "[Grass]"),
            (Site, "vegetation_height", "5, nan"),
            (Site, "tree_canopy", "No Trees"),
            (Site, "site_notes", None),
            (Observer, "name", ["Chris Hiemstra", "Hans Lievens"])
        ]
    )
    def test_metadata(
            self, uploaded_site_details_file, table, attribute,
            expected_value
    ):
        # Get multiple values for observers
        if table == Observer:
            result = self.get_values(table, attribute)
        else:
            result = self.get_value(table, attribute)
        if attribute == "geom":
            # Check geometry equals expected
            geom_from_wkb = load_wkb(bytes(result.data))
            geom_from_wkt = load_wkt(expected_value.data)

            assert geom_from_wkb.equals(geom_from_wkt)
        elif isinstance(expected_value, float) and np.isnan(expected_value):
            assert np.isnan(result)
        else:
            assert result == expected_value
