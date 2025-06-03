"""
Module for classes that upload single files to the database.
"""

from pathlib import Path
import pandas as pd
import geopandas as gpd
import logging
from typing import List
from geoalchemy2 import WKTElement
from snowexsql.tables import (
    PointData, MeasurementType, Instrument, DOI, Campaign, Observer,
    PointObservation
)
from snowexsql.tables.campaign_observation import CampaignObservation

from ..metadata import SnowExProfileMetadata
from ..point_metadata import PointSnowExMetadataParser
from ..string_management import parse_none
from ..point_data import PointDataCollection, SnowExPointData

from .base import BaseUpload


LOG = logging.getLogger("snowex_db.upload.points")


class DataValidationError(ValueError):
    pass

# TODO: do we need to make a SnowExPointDataCollection similar to
#   SnowExProfileDataCollection, since some files will have more than one point
#   measurement per file? This is true for GPR, summary swe, etc
# TODO: start with test datasets for simpler examples


class PointDataCSV(BaseUpload):
    """
    Class for submitting whole csv files of point data
    """
    expected_attributes = [c for c in dir(PointData) if c[0] != '_']
    TABLE_CLASS = PointData

    # Remapping for special keywords for snowdepth measurements
    MEASUREMENT_NAMES = {'mp': 'magnaprobe', 'm2': 'mesa', 'pr': 'pit ruler'}

    # Units to apply
    UNITS_MAP = {
        'depth': 'cm', 'two_way_travel': 'ns', 'swe': 'mm',
        'density': 'kg/m^3'
    }

    def __init__(self, profile_filename, timezone="US/Mountain", **kwargs):
        self.filename = profile_filename
        self._timezone = timezone
        self._doi = kwargs.get("doi")
        self._instrument = kwargs.get("instrument")
        self._header_sep = kwargs.get("header_sep", ",")
        self._id = kwargs.get("id")
        self._campaign_name = kwargs.get("campaign_name")
        # Is this file for derived measurements
        self._derived = kwargs.get("derived", False)

        # SMP passed in
        self._instrument_model = kwargs.get("instrument_model")
        self._comments = kwargs.get("comments")

        # Observer name for the whole file
        self._observer = kwargs.get("observer")
        # assign name to each measurement if given
        self._name = kwargs.get("name")

        # Assign if details are row based (generally for the SWE files)
        self._row_based_tz = kwargs.get("row_based_timezone", False)
        # TODO: what do we do here?
        if self._row_based_tz:
            in_timezone = None
        else:
            in_timezone = timezone

        # Read in data
        self.data = self._read(profile_filename, in_timezone=in_timezone)

    def _read(self, filename, in_timezone=None):
        """
        Read in the csv
        """
        try:
            # TODO: row based crs, tz options
            data = PointDataCollection.from_csv(
                filename, timezone=self._timezone,
                header_sep=self._header_sep, site_id=self._id,
                campaign_name=self._campaign_name,
                units_map=self.UNITS_MAP,
                row_based_timezone=self._row_based_tz
            )
        except pd.errors.ParserError as e:
            LOG.error(e)
            raise RuntimeError(f"Failed reading {filename}")

        return data

    def build_data(self, series: SnowExPointData) -> gpd.GeoDataFrame:
        """
        Build out the original dataframe with the metadata to avoid doing it
        during the submission loop. Removes all other main profile columns and
        assigns data_name as the value column

        Args:
            series: The object of a variable of point data

        Returns:
            df: Dataframe ready for submission
        """
        # TODO: DRY up?
        df = series.df.copy()
        if df.empty:
            LOG.debug("df is empty, returning")
            return df
        variable = series.variable

        # The type of measurement
        df['type'] = [variable.code] * len(df)

        # Manage nans and nones
        for c in df.columns:
            df[c] = df[c].apply(lambda x: parse_none(x))
        df['value'] = df[variable.code].astype(str)

        if 'units' not in df.columns:
            unit_str = series.units_map.get(variable.code)
            df['units'] = [unit_str] * len(df)
            # TODO: here

        columns = df.columns.values
        # Clean up comments a bit
        if 'comments' in columns:
            df['comments'] = df['comments'].apply(
                lambda x: x.strip(' ') if isinstance(x, str) else x)

        # In case of SMP, pass comments in
        if self._comments is not None:
            df["comments"] = [self._comments] * len(df)

        # Fill in more optional overrides
        for column_name, param in [
            ('instrument', self._instrument),
            ('doi', self._doi), ('instrument_model', self._instrument_model),
            ('observer', self._observer), ('name', self._name)
        ]:
            if column_name not in columns:
                df[column_name] = [param] * len(df)

        # Map the measurement names or default to original
        df["instrument"] = df['instrument'].map(
            lambda x: self.MEASUREMENT_NAMES.get(x, x)
        )

        return df

    def submit(self, session):
        """
        Submit values to the db from dictionary. Manage how some profiles have
        multiple values and get submitted individual

        Args:
            session: SQLAlchemy session
        """

        # Construct a dataframe with all metadata
        for series in self.data.series:
            df = self.build_data(series)

            # Grab each row, convert it to dict and join it with site info
            if not df.empty:
                # IMPORTANT: Add observations first, so we can use them in the
                # entries
                # TODO: how do these link back?
                self._add_campaign_observation(
                    session, df
                )

                for row in df.to_dict(orient="records"):
                    row["geometry"] = WKTElement(
                        str(row["geometry"]),
                        srid=int(df.crs.srs.replace("EPSG:", ""))
                    )

                    # TODO: instrument name logic here?
                    d = self._add_entry(session, row)
                    # session.bulk_save_objects(objects) does not resolve
                    # foreign keys, DO NOT USE IT
                    session.add(d)
                    session.commit()
            else:
                # procedure to still upload metadata (sites, etc)
                LOG.warning(
                    f'Point data file {self.filename} is empty.'
                )

    def _observation_name_from_row(self, row):
        value = f"{row['name']}_{row['instrument']}"
        if row.get('instrument_model'):
            value += row['instrument_model']
        # Add the type of measurement
        # This is necessary because the GPR returns multiple variables
        if row.get('type'):
            value += "_" + row['type']
        return value
    
    def _get_first_check_unique(self, df, key):
        """
        Get the first entry for a given key of the dataframe and check if
        it is unique. If not, raise a DataValidationError
        """
        unique_values = df[key].unique()
        if len(unique_values) > 1:
            raise DataValidationError(
                f"Multiple values for {key} found: {unique_values}"
            )
        return unique_values[0]

    def _add_campaign_observation(self, session, df):
        """
        Add the campaign observation and each of the entries it is linked to
        Returns:

        """
        df["date"] = pd.to_datetime(df["datetime"]).dt.date
        # Group by our observation keys to add into the database
        for keys, grouped_df in df.groupby(
                ['instrument', 'instrument_model', 'name', 'type', 'date'],
                dropna=False
        ):
            # Process each unique combination of keys (key) and its corresponding group (grouped_df)
            # Add instrument
            instrument_name = self._get_first_check_unique(grouped_df, 'instrument')
            # Map the instrument name if we have a mapping for it
            instrument_name = self.MEASUREMENT_NAMES.get(
                instrument_name.lower(), instrument_name
            )
            instrument = self._check_or_add_object(
                session, Instrument, dict(
                    name=instrument_name,
                    model=self._get_first_check_unique(grouped_df, 'instrument_model')
                )
            )
    
            # Add measurement type
            measurement_type = self._get_first_check_unique(
                grouped_df, "type"
            )
            measurement_obj = self._check_or_add_object(
                # Add units and 'derived' flag for the measurement
                session, MeasurementType, dict(
                    name=measurement_type,
                    units=self._get_first_check_unique(grouped_df, "units"),
                    derived=self._derived
                )
            )
            
            # Check name is unique
            self._get_first_check_unique(df, "name")
            # Get the measurement name
            measurement_name = self._observation_name_from_row(grouped_df.iloc[0])
    
            # Add doi
            doi_string = self._get_first_check_unique(df, "doi")
            if doi_string is not None:
                doi = self._check_or_add_object(
                    session, DOI, dict(doi=doi_string)
                )
            else:
                doi = None
            # pass in campaign
            campaign_name = self._get_first_check_unique(
                df, "campaign"
            ) or self._campaign_name
            if campaign_name is None:
                raise DataValidationError("Campaign cannot be None")
            campaign = self._check_or_add_object(
                session, Campaign, dict(name=campaign_name)
            )
            # add observer
            observer_name = self._get_first_check_unique(
                df, "observer"
            ) or self._observer
            observer_name = observer_name or "unknown"
            observer = self._check_or_add_object(
                session, Observer, dict(name=observer_name)
            )
            description = None
            if ["comments"] in grouped_df.columns.values:
                description = self._get_first_check_unique(
                    grouped_df, "comments"
                ),

            date_obj = self._get_first_check_unique(grouped_df, "date")
            observation = self._check_or_add_object(
                session, PointObservation, dict(
                    name=measurement_name,
                    date=date_obj,
                    instrument=instrument,
                    doi=doi,
                    measurement_type=measurement_obj,
                ),
                object_kwargs=dict(
                    name=measurement_name,
                    # TODO: we lose out on row-based comments here
                    description=description,
                    date=date_obj,
                    instrument=instrument,
                    doi=doi,
                    # type=row["type"],  # THIS TYPE IS RESERVED FOR POLYMORPHIC STUFF
                    measurement_type=measurement_obj,
                    observer=observer,
                    campaign=campaign,
                )
            )

    def _add_entry(
            self, session, row: dict
    ):
        """

        Args:
            session: db session object
            row: dataframe row of data to add
        Returns:

        """
        observation_kwargs = dict(
            name=self._observation_name_from_row(row),
            date=row["datetime"].date()
        )
        # Get the observation object
        observation = session.query(PointObservation).filter_by(
            **observation_kwargs
        ).first()
        if not observation:
            raise RuntimeError(
                f"No corresponding PointObservation for {observation_kwargs}"
            )

        # Now that the other objects exist, create the entry
        new_entry = self.TABLE_CLASS(
            # Linked tables
            value=row["value"],
            units=row["units"],  # TODO: isn't this in measurement obj?
            observation=observation,
            datetime=row["datetime"],
            # Arguments from kwargs
            geom=row['geometry']

        )

        return new_entry
