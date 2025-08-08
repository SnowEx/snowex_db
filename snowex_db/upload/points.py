"""
Module for classes that upload single files to the database.
"""
from pathlib import Path
import logging
import geopandas as gpd
import pandas as pd
from geoalchemy2 import WKTElement

from snowexsql.tables import (
    Campaign, DOI, Instrument, MeasurementType, Observer, PointData,
    PointObservation
)

from insitupy.io.strings import StringManager

from .base import BaseUpload
from ..point_data import PointDataCollection, SnowExPointData


LOG = logging.getLogger("snowex_db.upload.points")


class DataValidationError(ValueError):
    pass


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

    def __init__(
            self, session, profile_filename, timezone="US/Mountain", **kwargs
    ):
        """

        Args:
            session: SQLAlchemy session to use for the upload
            profile_filename: Path to the csv file to upload
            timezone: Timezone to assume for the data, defaults to "US/Mountain"
            **kwargs:
                doi
                instrument
                header_sep
                id
                campaign_name
                derived
                instrument_model
                comments
                observer
                name
                row_based_timezone
                instrument_map
        """
        self.filename = profile_filename
        self._session = session

        self._timezone = timezone
        self._doi = kwargs.get("doi")
        self._instrument = kwargs.get("instrument")
        # a map of measurement type to instrument name
        self._instrument_map = kwargs.get("instrument_map", {})
        if self._instrument_map and self._instrument:
            raise ValueError(
                "Cannot provide both 'instrument' and 'instrument_map'. "
                "Please choose one."
            )
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

        # Assign if details are row-based (generally for the SWE files)
        self._row_based_tz = kwargs.get("row_based_timezone", False)
        # TODO: what do we do here?
        if self._row_based_tz:
            in_timezone = None
        else:
            in_timezone = timezone

        # Read in data
        self.data = self._read(in_timezone=in_timezone)

    def _read(self, in_timezone=None):
        """
        Read in the csv
        """
        try:
            # TODO: row based crs, tz options
            data = PointDataCollection.from_csv(
                self.filename,
                timezone=self._timezone,
                header_sep=self._header_sep,
                site_id=self._id,
                campaign_name=self._campaign_name,
                units_map=self.UNITS_MAP,
                row_based_timezone=self._row_based_tz,
                primary_variable_file=Path(__file__).parent.joinpath(
                    "../point_primary_variable_overrides.yaml"
                )
            )
        except pd.errors.ParserError as e:
            LOG.error(e)
            raise RuntimeError(f"Failed reading {self.filename}")

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
            df[c] = df[c].apply(lambda x: StringManager.parse_none(x))
        df['value'] = df[variable.code].astype(float)

        if 'units' not in df.columns:
            unit_str = series.units_map.get(variable.code)
            df['units'] = [unit_str] * len(df)

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

        # Anywhere the instrument is None, use the instrument map
        # based on the measurement name
        if self._instrument_map and 'instrument' in df.columns:
            df['instrument'] = df['instrument'].fillna(
                df['type'].map(self._instrument_map)
            )

        # Map the measurement names or default to original
        df["instrument"] = df['instrument'].map(
            lambda x: self.MEASUREMENT_NAMES.get(x.lower(), x)
        )

        return df

    def submit(self):
        """
        Submit values to the db from dictionary. Manage how some profiles have
        multiple values and get submitted individual
        """

        # Construct a dataframe with all metadata
        for series in self.data.series:
            df = self.build_data(series)

            # Grab each row, convert it to dict and join it with site info
            if not df.empty:
                c_observations = self._add_campaign_observation(df)
                measurement_types = self._add_measurement_types(df)

                for row in df.to_dict(orient="records"):
                    row["geometry"] = WKTElement(
                        str(f"POINT ({row['longitude']} {row['latitude']})"),
                        srid=4326,
                    )

                    d = self._add_entry(row, c_observations, measurement_types)

                    # session.bulk_save_objects(objects) does not resolve
                    # foreign keys, DO NOT USE IT
                    self._session.add(d)
                    self._session.commit()
            else:
                # procedure to still upload metadata (sites, etc)
                LOG.warning(
                    f'Point data file {self.filename} is empty.'
                )

    def _observation_name_from_row(self, row):
        name = row.get("name") or row.get("pit_id")
        value = f"{name} {row['instrument']}"

        if row.get('instrument_model'):
            value = f"{value} {row['instrument_model']}"

        return value

    def _get_first_check_unique(self, df, key):
        """
        Get the first entry for a given key if present in the dataframe and check if
        it is unique. If not, raise a DataValidationError
        """
        unique_values = df.get(key, None)
        if unique_values is None:
            return None

        unique_values = df[key].unique()

        if len(unique_values) > 1:
            raise DataValidationError(
                f"Multiple values for {key} found: {unique_values}"
            )

        return unique_values[0]

    def _add_campaign_observation(self, df) -> dict:
        """
        Processes a DataFrame and adds unique entries of instruments, measurement types,
        campaigns, and observer.

        Parameters:
        df : pandas.DataFrame
            DataFrame containing relevant point data metadata information.

        Returns:
        dict
            A nested dictionary with primary keys of measurement names, and the secondary
            keys are dates of observations. Each value corresponds to an observation
            object or entry created in the session.
        """
        c_observations = {}
        df["date"] = pd.to_datetime(df["datetime"]).dt.date

        # Group by our observation keys to add records uniquely into the database
        base_groups = ['instrument', 'instrument_model', 'name', 'date']
        if 'pit_id' in df.columns:
            base_groups.append('pit_id')

        # Process each unique combination of keys (key) and its corresponding group (grouped_df)
        for keys, grouped_df in df.groupby(base_groups, dropna=False):
            # Add instrument
            instrument = self._check_or_add_object(
                self._session, Instrument, dict(
                    name=self._get_first_check_unique(grouped_df, 'instrument'),
                    model=self._get_first_check_unique(grouped_df, 'instrument_model'),
                )
            )
    
            # Check name is unique because we are adding ONE
            # campaign observation here
            self._get_first_check_unique(grouped_df, "name")
            if 'pit_id' in grouped_df.columns:
                self._get_first_check_unique(grouped_df, "pit_id")
            # Get the measurement name
            measurement_name = self._observation_name_from_row(grouped_df.iloc[0])
    
            # Add doi
            doi_string = self._get_first_check_unique(grouped_df, "doi")
            if doi_string is not None:
                doi = self._check_or_add_object(
                    self._session, DOI, dict(doi=doi_string)
                )
            else:
                doi = None
            # Add campaign
            campaign_name = self._get_first_check_unique(grouped_df, "campaign") \
                            or self._campaign_name
            if campaign_name is None:
                raise DataValidationError("Campaign cannot be None")
            campaign = self._check_or_add_object(
                self._session, Campaign, dict(name=campaign_name)
            )
            # Add observer
            observer_name = self._get_first_check_unique(
                grouped_df, "observer"
            ) or self._observer
            observer_name = observer_name or "unknown"
            observer = self._check_or_add_object(
                self._session, Observer, dict(name=observer_name)
            )
            # Construct description string
            description = None
            if ["comments"] in grouped_df.columns.values:
                description = (description or "") + self._get_first_check_unique(
                    grouped_df, "comments"
                )
            if ["flags"] in grouped_df.columns.values:
                description = (description or "") + self._get_first_check_unique(
                    grouped_df, "flags"
                )

            date_obj = self._get_first_check_unique(grouped_df, "date")
            object_args = dict(
                date=date_obj,
                name=measurement_name,
                # Link objects
                doi=doi,
                instrument=instrument,
            )
            observation = self._check_or_add_object(
                self._session,
                PointObservation,
                object_args,
                object_kwargs=dict(
                    **object_args,
                    description=description,
                    # Link objects
                    campaign=campaign,
                    observer=observer,
                )
            )

            if measurement_name not in c_observations:
                c_observations[measurement_name] = {}
            c_observations[measurement_name][date_obj] = observation

        return c_observations

    def _add_measurement_types(self, df) -> dict:
        """
        Adds unique measurement types from the points data to the database.

        Parameters:
        df: DataFrame
            Parsed CSV dataframe

        Returns:
        dict
            Dictionary with DB records mapped to measurement names.
        """
        types = {}

        for name, grouped_df in df.groupby('type'):
            units = grouped_df.units.unique()

            if len(units) > 1:
                raise DataValidationError(
                    f"Multiple units found for measurement type {name}: {units}"
                )

            measurement_args = dict(
                    name=name,
                    units=units[0],
                    derived=self._derived
                )
            types[name] = self._check_or_add_object(
                self._session, MeasurementType, measurement_args, measurement_args
            )

        return types

    def _add_entry(self, row: dict, observations: dict, measurement_types: dict) -> PointData:
        """
        Add a single point entry and map with the metadata.

        Args:
            row: (DataFrame) Row data to add
            observations: (dict) PointObservation created previously as metadata
            measurement_types: (dict) Measurement types created previously as metadata

        Returns:
            Added point data record object
        """
        observation_name = self._observation_name_from_row(row)
        try:
            observation = observations[observation_name][row["date"]]
        except KeyError:
            raise RuntimeError(
                f"No corresponding PointObservation for {observation_name} and "
                f"date: {row['date']}"
            )

        # Now that the other objects exist, create the entry
        new_entry = self.TABLE_CLASS(
            datetime=row["datetime"],
            elevation=row.get('elevation', None),
            geom=row['geometry'],
            measurement_type=measurement_types[row["type"]],
            observation=observation,
            value=row["value"],
        )

        return new_entry
