import logging
from typing import List

import numpy as np
import pandas as pd
from insitupy.io.dates import DateTimeManager
from insitupy.io.locations import LocationManager
from insitupy.io.metadata import MetaDataParser
from insitupy.io.yaml_codes import YamlCodes
from insitupy.profiles.base import MeasurementData
from insitupy.profiles.metadata import ProfileMetaData
from insitupy.variables import MeasurementDescription
from timezonefinder import TimezoneFinder

from .point_metadata import PointSnowExMetadataParser

LOG = logging.getLogger(__name__)


class SnowExPointData(MeasurementData):
    OUT_TIMEZONE = "UTC"
    META_PARSER = PointSnowExMetadataParser

    def __init__(
        self, variable: MeasurementDescription = None,
        meta_parser: MetaDataParser = None,
        row_based_timezone=False,
        timezone=None
        timezone=None,
        single_date=False,
    ):
        """
        Args:
            See MeasurementData.__init__
            row_based_timezone: does each row have a unique timezone implied
            timezone: input timezone for the whole file
            single_date: Dataset is from a single day

        """
        self._row_based_timezone = row_based_timezone
        self._in_timezone = timezone
        self._timezonefinder = None
        self._single_date = single_date
        super().__init__(variable, meta_parser)

    @property
    def timezonefinder(self):
        if self._timezonefinder is None:
            self._timezonefinder = TimezoneFinder(in_memory=True)
        return self._timezonefinder

    @staticmethod
    def read_csv_dataframe(profile_filename, columns, header_position):
        """
        Read in a profile file. Managing the number of lines to skip and
        adjusting column names

        Args:
            profile_filename: Filename containing a manually measured
                             profile
            columns: list of columns to use in dataframe
            header_position: skiprows for pd.read_csv
        Returns:
            df: pd.dataframe contain csv data with desired column names
        """
        # header=0 because docs say to if using skip rows and columns
        # NOTE: Using the 'c' engine won't automatically detect any delimiters
        #       and won't parse any files that hava a non comma separator,
        df = pd.read_csv(
            profile_filename, header=0,
            skiprows=header_position,
            names=columns,
            encoding='latin',
            dtype=str,  # treat all columns as strings to get weird date format,
            engine='c',
        )
        if "flags" in df.columns:
            # Max length of the flags column
            df["flags"] = df["flags"].str.replace(" ", "")

        return df

    def _get_location(self, row):
        """
        fill in the location info for a row
        Args:
            row: pandas row
        """
        try:
            lat, lon, *_ = LocationManager.parse(row)
        except ValueError:
            if self.metadata is not None:
                LOG.warning(
                    f"Row {row.name} does not have a valid location. "
                    "Attempting to use header metadata."
                )
                lat, lon = self.metadata.latitude, self.metadata.longitude
            else:
                raise RuntimeError("No valid location found in row or metadata.")

        return lat, lon

    def _get_datetime(self, row):
        """
        fill in the datetime info for a row
        Args:
            row: pandas row
        """
        tz = self._in_timezone
        if self._row_based_timezone:
            # Look up the timezone for the location and apply that
            timezone_str = self.timezonefinder.timezone_at(
                lat=row["latitude"], lng=row["longitude"]
            )
            tz = timezone_str  # e.g., 'America/Denver'
        try:
            datetime = None
            # In case we found a date entry that has date and time
            if row.get(YamlCodes.DATE_TIME) is not None:
                str_date = str(
                    row[YamlCodes.DATE_TIME].replace('T', '-')
                )

                datetime = pd.to_datetime(str_date)

            if datetime is None:
                datetime = DateTimeManager.handle_separate_datetime(row)

            result = DateTimeManager.adjust_timezone(
                datetime,
                in_timezone=tz,
                out_timezone=self.OUT_TIMEZONE
            )
        except ValueError as e:
            if self.metadata is not None:
                result = self.metadata.date_time
            else:
                raise e
        return result

    def _format_df(self):
        """
        Format the incoming df with the column headers and other info we want
        This will filter to a single measurement as well as the expected
        shared columns like depth
        """
        self._set_column_mappings()

        # If the variable is real (not -1), check columns
        if self.variable.code != "-1":
            # Verify the sample column exists and rename to variable
            self._check_sample_columns()

        # If we do not have a geometry column, we need to parse
        # the raw df, otherwise we assume this has been done already,
        # likely on the first read of the file

        # Get the campaign name
        if "campaign" not in self._df.columns:
            self._df["campaign"] = self._df.get(YamlCodes.SITE_NAME)
        # TODO: How do we speed this up?
        #   campaign should be very quick with a df level logic
        #   but the other ones will take morelogic
        # parse the location
        self._df[["latitude", "longitude"]] = self._df.apply(
            self._get_location, axis=1, result_type="expand"
        )
        # If the datetime isn't already parsed, parse it
        if (
                "datetime" in self._df.columns.tolist()
                and pd.api.types.is_datetime64_any_dtype(
                    self._df["datetime"]
                )
        ):
            LOG.debug("not parsing date")
        else:
            # Parse the datetime
            if self._single_date:
                self._df["datetime"] = DateTimeManager.handle_separate_datetime(
                    self._df.iloc[0]
                )
            else:
                self._df["datetime"] = self._df.apply(
                    self._get_datetime, axis=1, result_type="expand"
                )

        self._df = self._df.replace(-9999, np.NaN)


class PointDataCollection:
    """
    This could be a collection of profiles
    """
    DATA_CLASS = SnowExPointData

    def __init__(self, series: List[SnowExPointData], metadata: ProfileMetaData):
        self._series = series
        self._metadata = metadata

    @property
    def metadata(self) -> ProfileMetaData:
        return self._metadata

    @property
    def series(self) -> List[SnowExPointData]:
        return self._series

    @classmethod
    def _read_csv(
        cls,
        fname,
        meta_parser: PointSnowExMetadataParser,
        timezone=None,
        row_based_timezone=False,
        single_date=False,
    ) -> List[SnowExPointData]:
        """
        Args:
            fname: path to csv
            meta_parser: parser for the metadata
            timezone: input timezone
            row_based_timezone: is the timezone row based?
            single_date: All observations are from single date

        Returns:
            a list of ProfileData objects

        """
        # parse the file for metadata before parsing the individual
        # variables
        all_file = cls.DATA_CLASS(
            variable=None,  # we do not have a variable yet
            meta_parser=meta_parser,
            timezone=timezone,
            row_based_timezone=row_based_timezone,
            single_date=single_date,
        )
        all_file.from_csv(fname)

        result = []

        shared_column_options = [
            # TODO: could we make this a 'shared' option in the definition
            meta_parser.primary_variables.entries["CAMPAIGN"],
            meta_parser.primary_variables.entries["COMMENTS"],
            meta_parser.primary_variables.entries["DATE"],
            meta_parser.primary_variables.entries["DATETIME"],
            meta_parser.primary_variables.entries["EASTING"],
            meta_parser.primary_variables.entries["ELEVATION"],
            meta_parser.primary_variables.entries["FLAGS"],
            meta_parser.primary_variables.entries["FREQUENCY"],
            meta_parser.primary_variables.entries["INSTRUMENT"],
            meta_parser.primary_variables.entries["INSTRUMENT_MODEL"],
            meta_parser.primary_variables.entries["LATITUDE"],
            meta_parser.primary_variables.entries["LONGITUDE"],
            meta_parser.primary_variables.entries["NORTHING"],
            meta_parser.primary_variables.entries["PIT_ID"],
            meta_parser.primary_variables.entries["TIME"],
            meta_parser.primary_variables.entries["UTCDOY"],
            meta_parser.primary_variables.entries["UTCTOD"],
            meta_parser.primary_variables.entries["UTCYEAR"],
            meta_parser.primary_variables.entries["UTM_ZONE"],
        ]

        shared_columns = [
            c for c, v in all_file.meta_columns_map.items()
            if v in shared_column_options
        ]
        variable_columns = [
            c for c in all_file.meta_columns_map.keys() if c not in shared_columns
        ]
        # Filter out ignore columns
        variable_columns = [
            v for v in variable_columns
            if all_file.meta_columns_map[v].code != "ignore"
        ]

        # Create an object for each measurement
        for column in variable_columns:
            points = cls.DATA_CLASS(
                variable=all_file.meta_columns_map[column],
                meta_parser=meta_parser,
                timezone=timezone, row_based_timezone=row_based_timezone
            )
            # IMPORTANT - Metadata needs to be set before assigning the
            # dataframe as information from the metadata is used to format_df
            # the information
            points.metadata = all_file.metadata
            df_columns = all_file.df.columns.tolist()
            # The df setter filters some columns, so adjust our shared columns
            df_shared_columns = [
                 c for c in shared_columns if c in df_columns
            ]
            # run the whole file through the df setter
            points.df = all_file.df.loc[:, df_shared_columns + [column]].copy()
            # --------
            result.append(points)

        return result, all_file.metadata

    @classmethod
    def from_csv(
        cls,
        fname,
        timezone="US/Mountain",
        header_sep=",",
        site_id=None,
        campaign_name=None,
        allow_map_failure=False,
        units_map=None,
        row_based_timezone=False,
        metadata_variable_file=None,
        primary_variable_file=None,
        single_date=False,
    ):
        """
        Find all variables in a single csv file
        Args:
            fname: path to file
            timezone: expected timezone in file
            header_sep: header sep in the file
            site_id: Site id override for the metadata
            campaign_name: Campaign.name override for the metadata
            allow_map_failure: allow metadata and column unknowns
            units_map: units map for the metadata
            row_based_timezone: is the timezone row based
            metadata_variable_file: list of files to override the metadata
                variables
            primary_variable_file: list of files to override the
                primary variables
            single_date: This dataset collection is from a single date

        Returns:
            This class with a collection of profiles and metadata
        """
        # parse multiple files and create an iterable of ProfileData
        meta_parser = PointSnowExMetadataParser(
            timezone, primary_variable_file, metadata_variable_file,
            header_sep=header_sep, _id=site_id,
            campaign_name=campaign_name, allow_map_failures=allow_map_failure,
            units_map=units_map,
        )

        # read in the actual data
        profiles, metadata = cls._read_csv(
            fname,
            meta_parser,
            timezone=timezone,
            row_based_timezone=row_based_timezone,
            single_date=single_date,
        )
        # ignore profiles with the name 'ignore'
        profiles = [
            p for p in profiles if
            # Keep the profile if it is None because we need the metadata
            (p.variable is None or p.variable.code != "ignore")
        ]

        return cls(profiles, metadata)
