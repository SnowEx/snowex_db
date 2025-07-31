import pytest
from geoalchemy2 import WKTElement

from snowexsql.tables import PointData, DOI, Campaign, Instrument, \
    MeasurementType, PointObservation

from snowex_db.upload.points import PointDataCSV
from tests.points._base import PointBaseTesting


class TestSummaryPits(PointBaseTesting):
    """
    Test the summary csvs for a collection of pits
    """

    kwargs = {
        'timezone': 'MST',
        'doi': "some_point_pit_doi",
        "row_based_timezone": True, # row based timezone
        "derived": True,
        "instrument_map": {
            "depth": "manual",
            "swe": "manual",
            "density": "cutter",
            "comments": "unknown"
        }
    }
    UploaderClass = PointDataCSV
    TableClass = PointData

    @pytest.fixture(scope="class")
    def uploaded_file(self, session, data_dir):
        """
        NOTE - this is part of the _modified file that we create
        in the upload script, NOT the original file
        """
        self.upload_file(
            session,
            str(data_dir.joinpath("pit_summary_points.csv"))
        )

    def filter_measurement_type(self, session, measurement_type, query=None):
        if query is None:
            query = session.query(self.TableClass)

        query = query.join(
            self.TableClass.observation
        ).join(
            PointObservation.measurement_type
        ).filter(MeasurementType.name == measurement_type)
        return query

    @pytest.mark.usefixtures('uploaded_file')
    @pytest.mark.parametrize(
        "name, count", [
            ("CAAMCL", 21),
            ("COER12", 15),
        ]
    )
    def test_point_observation(self, name, count, session):
        """
        3 records per each date and site
        """
        records = session.query(PointObservation).filter(
            PointObservation.name.like(f'{name}%')
        ).all()
        assert len(records) == count

    @pytest.mark.usefixtures('uploaded_file')
    @pytest.mark.parametrize(
        "name, units, derived", [
            ("depth", "cm", True),
            ("swe", "mm", True),
            ("density", "kg/m^3", True),
        ]
    )
    def test_measurement_types(self, name, units, derived, session):
        record = self.get_records(session, MeasurementType, 'name', name)

        assert len(record) == 1
        assert record[0].units == units
        assert record[0].derived == derived

    @pytest.mark.usefixtures('uploaded_file')
    @pytest.mark.parametrize(
        "name", [
            "American River Basin",
            "East River",
        ]
    )
    def test_campaign(self, name, session):
        assert len(self.get_records(session, Campaign, 'name', name)) == 1

    @pytest.mark.usefixtures('uploaded_file')
    @pytest.mark.parametrize("name", [ "cutter", "manual" ])
    def test_instrument(self, name, session):
        assert len(self.get_records(session, Instrument, 'name', name)) == 1

    @pytest.mark.parametrize(
        "table, attribute, expected_value", [
            (DOI, "doi", "some_point_pit_doi"),
            (PointData, "geom",
                WKTElement('POINT (-120.04186927254749 38.71033054555811)', srid=4326)
             ),
        ]
    )
    def test_metadata(self, table, attribute, expected_value, uploaded_file):
        self._check_metadata(table, attribute, expected_value)

    @pytest.mark.usefixtures('uploaded_file')
    @pytest.mark.parametrize(
        "data_name, attribute_to_check, filter_attribute, filter_value",
        [
            ("depth", "value", "value", [135, 123, 119, 117, 92, 72, 50]),
            (
                "density", "value", "value",
                [
                    267.0,
                    278.0,
                    329.5,
                    356.0,
                    356.5,
                    359.5,
                    364.0,
                    368.0,
                    393.0,
                    396.5,
                    403.5,
                    435.5,
                ],
            ),
            (
                "swe", "value", "value",
                [
                    196.0,
                    245.5,
                    257.0,
                    333.5,
                    356.5,
                    367.0,
                    424.0,
                    434.5,
                    442.5,
                    446.5,
                    475.5,
                    479.5,
                ],
            ),
        ],
    )
    def test_value(self, data_name, attribute_to_check, filter_attribute, filter_value):
        for value in filter_value:
            self.check_value(
                data_name, attribute_to_check, filter_attribute, value, [value]
            )

    @pytest.mark.usefixtures('uploaded_file')
    @pytest.mark.parametrize(
        "data_name, expected",
        [
            ("depth", 12),
            ("density", 12),
            ("swe", 12),
        ],
    )
    def test_count(self, data_name, expected):
        """
        Check that all entries in the CSV made it into the database
        """
        assert self.check_count(data_name) == expected
