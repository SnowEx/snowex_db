from datetime import datetime, timezone
from shapely.wkb import loads as load_wkb
from shapely.wkt import loads as load_wkt
import numpy as np
import pytest
import pytz
import os

from geoalchemy2 import WKTElement
from snowexsql.tables import (
    LayerData, Campaign, Instrument, Observer, Site
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


class TestStratigraphyProfile(TableTestBase):
    """
    Test that all the profiles from the Stratigraphy file were uploaded and
    have integrity
    """

    args = ['stratigraphy.csv']
    kwargs = {'in_timezone': 'MST'}
    UploaderClass = UploadProfileData
    TableClass = LayerData

    params = {
        'test_count': [dict(data_name='hand_hardness', expected_count=5)],

        # Test a value from the profile to check that the profile is there and it has integrity
        'test_value': [
            dict(data_name='hand_hardness', attribute_to_check='value', filter_attribute='depth', filter_value=30,
                 expected='4F'),
            dict(data_name='grain_size', attribute_to_check='value', filter_attribute='depth', filter_value=35,
                 expected='< 1 mm'),
            dict(data_name='grain_type', attribute_to_check='value', filter_attribute='depth', filter_value=17,
                 expected='FC'),
            dict(data_name='manual_wetness', attribute_to_check='value', filter_attribute='depth', filter_value=17,
                 expected='D'),

            # Test were are uploading most of the important attributes
            dict(data_name='hand_hardness', attribute_to_check='site_id', filter_attribute='depth',
                 filter_value=30, expected='1N20'),
            dict(data_name='hand_hardness', attribute_to_check='datetime', filter_attribute='depth', filter_value=30,
                 expected=None),
            dict(data_name='hand_hardness', attribute_to_check='site_name', filter_attribute='depth',
                 filter_value=30, expected='Grand Mesa'),
            dict(data_name='hand_hardness', attribute_to_check='easting', filter_attribute='depth',
                 filter_value=30, expected=743281),
            dict(data_name='hand_hardness', attribute_to_check='northing', filter_attribute='depth',
                 filter_value=30, expected=4324005),

            # Test the single comment was used
            dict(data_name='hand_hardness', attribute_to_check='comments', filter_attribute='depth',
                 filter_value=17, expected='Cups'),

            ],

        'test_unique_count': [
            # Test only 1 value was submitted to each layer for wetness
            dict(data_name='manual_wetness', attribute_to_count='value', expected_count=1),
            # Test only 3 hand hardness categories were used
            dict(data_name='hand_hardness', attribute_to_count='value', expected_count=3),
            # Test only 2 grain type categories were used
            dict(data_name='grain_type', attribute_to_count='value', expected_count=2),
            # Test only 2 grain_sizes were used
            dict(data_name='grain_size', attribute_to_count='value', expected_count=2),

        ]
    }


class TestDensityProfile(TableTestBase, WithUploadedFile):
    """
    Test that a density file is uploaded correctly including sample
    averaging for the main value.
    """

    kwargs = {
        'timezone': 'MST',
        'instrument': 'kelly cutter',
        # 'site_name': 'grand mesa',
        # 'observers': 'TEST',
        # 'elevation': 1000,
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
        assert  n == expected

    @pytest.mark.parametrize(
        "data_name, attribute_to_count, expected", [
            ("density", "site_id", 1)
        ]
    )
    def test_unique_count(self, data_name, attribute_to_count, expected, uploaded_file):
        self.check_unique_count(
            data_name, attribute_to_count, expected
        )


class TestDensityProfileRowBased(TableTestBase, WithUploadedFile):
    """
    Test that row based tzinfo and crs works (for alaska data)
    """

    @pytest.mark.parametrize(
        "data_name, expected", [
            ("density", 4)
        ]
    )
    def test_count(self, data_name, expected, uploaded_file):
        raise NotImplementedError("")


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

    params = {
        'test_count': [dict(data_name='temperature', expected_count=5)],

        # Test a value from each profile to check that the profile is there and it has integrity
        'test_value': [
            dict(data_name='temperature', attribute_to_check='value', filter_attribute='depth', filter_value=10,
                 expected=-5.9),
            dict(data_name='temperature', attribute_to_check='sample_a', filter_attribute='depth', filter_value=35,
                 expected=None),
            ],
        'test_unique_count': [
            # Place holder for this test: test only one location was added
            dict(data_name='temperature', attribute_to_count='northing', expected_count=1)
        ]
    }

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


class TestSSAProfile(TableTestBase):
    """
    Test that all profiles from an SSA file are uploaded correctly
    """

    args = ['SSA.csv']
    kwargs = {'in_timezone': 'MST'}
    UploaderClass = UploadProfileData
    TableClass = LayerData
    dt = datetime(2020, 2, 5, 20, 40, 0, 0,  pytz.utc)

    params = {
        'test_count': [dict(data_name='reflectance', expected_count=16)],

        # Test a value from each profile to check that the profile is there and it has integrity
        'test_value': [
            dict(data_name='reflectance', attribute_to_check='value', filter_attribute='depth', filter_value=10,
                 expected=22.12),
            dict(data_name='specific_surface_area', attribute_to_check='value', filter_attribute='depth',
                 filter_value=35, expected=11.2),
            dict(data_name='equivalent_diameter', attribute_to_check='value', filter_attribute='depth', filter_value=80,
                 expected=0.1054),
            dict(data_name='sample_signal', attribute_to_check='value', filter_attribute='depth', filter_value=10,
                 expected=186.9),
            dict(data_name='reflectance', attribute_to_check='comments', filter_attribute='depth', filter_value=5,
                 expected='brush'),

            ],
        'test_unique_count': [
            # Confirm we only have 1 comment and everything else is none
            dict(data_name='reflectance', attribute_to_count='comments', expected_count=2),

        ]
    }


class TestSMPProfile(TableTestBase):
    """
    Test SMP profile is uploaded with all its attributes and valid data
    """

    args = ['S06M0874_2N12_20200131.CSV']
    kwargs = {'in_timezone': 'UTC', 'units': 'Newtons', 'header_sep': ':', 'instrument': 'snowmicropen'}
    UploaderClass = UploadProfileData
    TableClass = LayerData
    dt = datetime(2020, 1, 31, 22, 42, 14, 0, pytz.utc)

    params = {
        'test_count': [dict(data_name='force', expected_count=154)],
        'test_value': [
            dict(data_name='force', attribute_to_check='value', filter_attribute='depth', filter_value=-53.17,
                 expected=0.331),
            dict(data_name='force', attribute_to_check='date', filter_attribute='depth', filter_value=-0.4,
                 expected=dt.date()),
            dict(data_name='force', attribute_to_check='time', filter_attribute='depth', filter_value=-0.4,
                 expected=dt.timetz()),
            dict(data_name='force', attribute_to_check='latitude', filter_attribute='depth', filter_value=-0.4,
                 expected=39.03013229370117),
            dict(data_name='force', attribute_to_check='longitude', filter_attribute='depth', filter_value=-0.4,
                 expected=-108.16268920898438),
        ],
        'test_unique_count': [dict(data_name='force', attribute_to_count='date', expected_count=1)]
    }

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


class TestEmptyProfile(TableTestBase):
    """
    Test that a file with header info that doesnt have data (which
    happens on purpose) doesnt upload anything
    """

    args = ['empty_data.csv']
    kwargs = {'in_timezone': 'MST'}
    UploaderClass = UploadProfileData
    TableClass = LayerData
    dt = datetime(2020, 2, 5, 18, 40, 0, 0,  pytz.utc)

    params = {'test_count': [dict(data_name='hand_hardness', expected_count=0)],
              'test_value': [dict(data_name='hand_hardness', attribute_to_check='value', filter_attribute='depth',
                                  filter_value=1, expected=None)],
              'test_unique_count': [dict(data_name='hand_hardness', attribute_to_count='comments', expected_count=0)]
        }


class TestMetadata(WithUploadedFile):
    kwargs = {'in_timezone': 'MST'}
    UploaderClass = UploadProfileData

    @pytest.fixture
    def uploaded_lwc_file(self, db, data_dir):
        self.upload_file(str(data_dir.joinpath("LWC.csv")))
        
    @pytest.fixture
    def uploaded_site_details_file(self, db, data_dir):
        self.upload_file(str(data_dir.joinpath("site_details.csv")))

    @pytest.mark.parametrize(
        "table, attribute, expected_value", [
            (Site, "name", "COGM1N20_20200205"),
            (Site, "datetime", ""),
            (Site, "elevation", "COGM1N20_20200205"),
            (Site, "geometry", "COGM1N20_20200205"),
            (Campaign, "name", "Grand Mesa"),
            (Instrument, "name", "COGM1N20_20200205"),

                    # {'site_name': 'Grand Mesa',
                    #  'site_id': '1N20',
                    #  'pit_id': 'COGM1N20_20200205',
                    #  'date': dt.date(),
                    #  'time': dt.timetz(),
                    #  'utm_zone': 12,
                    #  'easting': 743281.0,
                    #  'northing': 4324005.0,
                    #  'latitude': 39.03126190934254,
                    #  'longitude': -108.18948133421802,
                    #  }
        ]
    )
    def test_lwc_file_metadata(
            self, uploaded_density_file, table, attribute, expected_value
    ):
        result = self.get_value(table, attribute)
        if not isinstance(expected_value, list):
            expected_value = [expected_value]
        assert result == expected_value

    # @pytest.mark.parametrize(
    #     "table_name, attribute, expected_value", [
    #         (
    #                 {'site_name': 'Grand Mesa',
    #                  'site_id': '1N20',
    #                  'pit_id': 'COGM1N20_20200205',
    #                  'date': dt.date(),
    #                  'time': dt.timetz(),
    #                  'utm_zone': 12,
    #                  'easting': 743281.0,
    #                  'northing': 4324005.0,
    #                  'latitude': 39.03126190934254,
    #                  'longitude': -108.18948133421802,
    #                  }
    #         )
    #     ]
    # )
    # def test_lwc_file_metadata(
    #         self, uploaded_lwc_file, table_name, attribute, expected_value
    # ):
    #     pass

    @pytest.mark.parametrize(
        "table, attribute, expected_value", [
            (Site, "name", "COGM1N20_20200205"),
            (Site, "datetime", ""),
            (Site, "elevation", "COGM1N20_20200205"),
            (Site, "geometry", "COGM1N20_20200205"),
            (Site, "aspect", ""),
            (Site, "air_temp", ""),
            (Site, "total_depth", ""),
            (Site, "weather_description", ""),
            (Site, "precip", ""),
            (Site, "sky_cover", ""),
            (Site, "wind", ""),
            (Site, "ground_condition", ""),
            (Site, "ground_roughness", ""),
            (Site, "ground_vegetation", ""),
            (Site, "vegetation_height", ""),
            (Site, "tree_canopy", ""),
            (Site, "site_notes", ""),
            (Observer, "name", "")
        ]
    )
    def test_metadata(
            self, uploaded_site_details_file, table, attribute,
            expected_value
    ):
        result = self.get_value(table, attribute)
        if not isinstance(expected_value, list):
            expected_value = [expected_value]
        assert result == expected_value
