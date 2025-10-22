from datetime import datetime, timezone

import numpy as np
import pytest
from geoalchemy2 import WKTElement
from snowexsql.tables import LayerData, Site

from snowex_db.upload.layers import UploadProfileData
from tests.helpers import WithUploadedFile
from tests.sql_test_base import TableTestBase


class TestEmptyProfile(TableTestBase, WithUploadedFile):
    """
    Test that a file with header info that doesn't have data (which
    happens on purpose) doesn't upload anything
    """

    args = []
    kwargs = {
        'timezone': 'MST',
        'doi': 'no_doi',
    }
    UploaderClass = UploadProfileData
    TableClass = LayerData

    @pytest.fixture(scope="class")
    def uploaded_file(self, session, data_dir):
        self.upload_file(
            filename=str(data_dir.joinpath('empty_data.csv')), session=session
        )

    @pytest.mark.parametrize(
        "data_name, expected", [
            ("hand_hardness", 0),
        ]
    )
    def test_count(self, data_name, expected, uploaded_file):
        n = self.check_count(data_name)
        assert n == expected


class TestMetadata2020(TableTestBase, WithUploadedFile):
    """
    Test the site details file from the 2020 campaign
    """
    kwargs = {
        'timezone': 'MST',
        'doi': 'no_doi',
    }
    UploaderClass = UploadProfileData

    @pytest.fixture(scope="class")
    def uploaded_site_details_file(self, session, data_dir):
        self.upload_file(
            filename=str(data_dir.joinpath("site_details_2020.csv")),
            session=session
        )

    @pytest.fixture
    def site_records(self, session):
        return self.get_records(Site, "name", "COGM1N20_20200205")

    @pytest.mark.usefixtures("uploaded_site_details_file")
    @pytest.mark.parametrize(
        "attribute, expected_value", [
            ("datetime", datetime(2020, 2, 5, 20, 30, tzinfo=timezone.utc)),
            ("aspect", 180.0),
            ("slope_angle", 5.0),
            ("air_temp", np.nan),
            ("total_depth", 35.0),
            ("weather_description", "Sunny, cold, gusts"),
            ("precip", "None"),
            ("sky_cover", "Few (< 1/4 of sky)"),
            ("wind", "Moderate"),
            ("ground_condition", "Frozen"),
            ("ground_roughness", "Rough"),
            ("ground_vegetation", "Grass"),
            ("vegetation_height", "5"),
            ("tree_canopy", "No Trees"),
            (
                "comments",
                "Start temperature measurements (top) 13:48 End temperature "
                "measurements (bottom) 13:53 LWC sampler broke, no "
                "measurements were possible; "
            ),
        ]
    )
    def test_site_attributes(self, attribute, expected_value, site_records):
        assert len(site_records) == 1

        site = site_records[0]
        if attribute == "air_temp":
            assert np.isnan(getattr(site, attribute))
        else:
            assert getattr(site, attribute) == expected_value

    @pytest.mark.usefixtures("uploaded_site_details_file")
    def test_query_by_site_geom(self, site_records):
        """
        Test that we can find the site by its coordinates.
        """
        site_coordinate = WKTElement(
                'POINT (-108.1894813320662 39.031261970372725)', srid=4326
        )
        site = self.get_records(Site, "geom", site_coordinate)

        assert site[0].name == site_records[0].name
        assert site[0].geom == site_records[0].geom

    @pytest.mark.usefixtures("uploaded_site_details_file")
    def test_site_campaign(self, site_records):
        assert site_records[0].campaign.name == "Grand Mesa"

    @pytest.mark.usefixtures("uploaded_site_details_file")
    def test_site_observer(self, site_records):
        observers = [observer.name for observer in site_records[0].observers]
        assert "Chris Hiemstra", "Hans Lievens" in observers
