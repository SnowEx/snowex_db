"""
Module for classes that upload single files to the database.
"""
import logging
from pathlib import Path
from typing import List, Union

import geopandas as gpd
import pandas as pd
from geoalchemy2 import WKTElement

from insitupy.io.strings import StringManager
from insitupy.campaigns.snowex import SnowExProfileData
from snowexsql.tables import (
    Campaign, DOI, Instrument, LayerData, MeasurementType, Observer, Site
)
from .base import BaseUpload
from .batch import BatchBase
from ..metadata import SnowExProfileMetadata
from ..profile_data import ExtendedSnowExProfileDataCollection
from ..utilities import get_logger

LOG = logging.getLogger("snowex_db.upload.layers")


class DataValidationError(ValueError):
    pass


class UploadProfileData(BaseUpload):
    """
    Class for submitting a single profile. Since layers are uploaded layer by
    layer this allows for submitting them one file at a time.
    """
    expected_attributes = [c for c in dir(LayerData) if c[0] != '_']
    TABLE_CLASS = LayerData

    def __init__(
        self, session, filename: Union[str, Path], timezone: str = "US/Mountain", **kwargs
    ):
        """
        Arguments:
            session: The DB session object
            filename (Union[str, Path]): The path to the profile file.
            timezone (str): The timezone used, default is "US/Mountain".
            kwargs: Additional optional keyword arguments related to the profile.
                doi (str): Digital Object Identifier
                instrument (str): Name of the instrument used in the collection.
                header_sep (str): Delimiter for separating values in the header.
                                  Default is ','.
                id (str): Identifier for the profile data file.
                campaign_name (str): The name of the campaign.
                derived (bool): Indicates if the file contains derived measurements.
                                Default False.
                instrument_model (str): Instrument name.
                comments (str): Additional comments.
        """
        super().__init__()
        self.log = get_logger(__name__)

        self.filename = filename
        self._session = session
        self._timezone = timezone

        # Optional information
        self._doi = kwargs.get("doi")
        self._header_sep = kwargs.get("header_sep", ",")
        # Is this file for derived measurements
        self._derived = kwargs.get("derived", False)

        # Metadata overwrites
        # TODO - Rename this to site_id
        self._id = kwargs.get("id")
        self._campaign_name = kwargs.get("campaign_name")
        self._instrument = kwargs.get("instrument")
        self._instrument_model = kwargs.get("instrument_model")

        self._comments = kwargs.get("comments", '')

        # Read in data
        self.data = self._read()

    def _read(self) -> ExtendedSnowExProfileDataCollection:
        """
        Read in a profile file. Managing the number of lines to skip and
        adjusting column names

        Returns:
            list of ProfileData objects
        """
        try:
            return ExtendedSnowExProfileDataCollection.from_csv(
                filename=self.filename,
                timezone=self._timezone,
                header_sep=self._header_sep,
                site_id=self._id,
                campaign_name=self._campaign_name,
                metadata_variable_file=Path(__file__).parent.joinpath(
                    "../metadata_variable_overrides.yaml"
                ),
                primary_variable_file=Path(__file__).parent.joinpath(
                    "../profile_primary_variable_overrides.yaml"
                ),
            )
        except pd.errors.ParserError as e:
            LOG.error(e)
            raise RuntimeError(f"Failed reading {self.filename}")

    def build_data(self, profile: SnowExProfileData) -> gpd.GeoDataFrame:
        """
        Build out the original dataframe with the metadata to avoid doing it
        during the submission loop. Removes all other main profile columns and
        assigns data_name as the value column

        Args:
            profile: The object of a single profile

        Returns:
            df: Dataframe ready for submission
        """

        if profile.df is None:
            LOG.debug("df is empty, returning")
            return gpd.GeoDataFrame()

        metadata = profile.metadata
        variable = profile.variable

        df = profile.df.copy()

        # The type of measurement
        df['type'] = [variable.code] * len(df)

        # Manage nans and nones
        for c in df.columns:
            df[c] = df[c].apply(lambda x: StringManager.parse_none(x))
        df['value'] = df[variable.code].astype(str)

        if 'units' not in df.columns:
            unit_str = profile.units_map.get(variable.code)
            df['units'] = [unit_str] * len(df)

        columns = df.columns.values
        # Clean up comments a bit
        if 'comments' in columns:
            df['value'] = df['value'].apply(
                lambda x: x.strip(' ') if isinstance(x, str) else x)

        # Add flags to the comments.
        flag_string = metadata.flags
        if flag_string:
            flag_string = " Flags: " + flag_string
            if 'comments' in columns:
                df["comments"] += flag_string
            else:
                df["comments"] = flag_string

        return df

    def submit(self):
        """
        Submit values to the DB. Can handle multiple profiles and uses
        information supplied in the constructor.
        """

        # Construct a dataframe with all metadata
        for profile in self.data.profiles:
            df = self.build_data(profile)

            # Grab each row, convert it to dict and join it with site info
            if not df.empty:
                # Metadata for all layers
                campaign, observer_list, site = self._add_metadata(
                    profile.metadata
                )

                instrument = None
                if 'instrument' not in df.columns.values:
                    instrument = self._add_instrument(profile.metadata)

                for row in df.to_dict(orient="records"):
                    if row.get('value') == 'None':
                        continue

                    d = self._add_entry(
                        row, campaign, observer_list, site, instrument,
                    )
                    # session.bulk_save_objects(objects) does not resolve
                    # foreign keys, DO NOT USE IT
                    self._session.add(d)

                self._session.commit()
                # Mark all cached objects as expired
                self._session.expunge_all()
            else:
                # procedure to still upload metadata (sites, etc)
                self.log.warning(
                    'File contains header but no data which is sometimes'
                    ' expected. Skipping row submissions, and only inserting'
                    ' metadata.'
                )
                self._add_metadata(profile.metadata)

    def _add_metadata(self, metadata: SnowExProfileMetadata):
        """
        Add the metadata entry and return objects to associate with each row.

        Args:
            metadata: ProfileMetadata information

        Returns:

        """
        # Campaign record
        campaign = self._check_or_add_object(
            self._session, Campaign, dict(name=metadata.campaign_name)
        )
        # List of observers records
        observer_list = []
        observer_names = metadata.observers or []
        for obs_name in observer_names:
            observer = self._check_or_add_object(
                self._session, Observer, dict(name=obs_name)
            )
            observer_list.append(observer)

        # DOI record
        if self._doi is not None:
            doi = self._check_or_add_object(
                self._session, DOI, dict(doi=self._doi)
            )
        else:
            doi = None

        # Datetime from metadata
        dt = metadata.date_time
        # Geometry from metadata
        geom = WKTElement(
            f"Point ({metadata.longitude} {metadata.latitude})",
            srid=4326
        )
        # Combine found comments and passed in comments to this class
        comments = '; '.join(
            [
                comment for comment in [metadata.comments,  self._comments]
                if comment is not None

            ]
        )
        # Site record
        site_id = metadata.site_name

        site = self._check_or_add_object(
            self._session,
            Site,
            dict(name=site_id),
            object_kwargs=dict(
                air_temp=metadata.air_temp,
                aspect=metadata.aspect,
                campaign=campaign,
                comments=comments,
                datetime=dt,
                doi=doi,
                geom=geom,
                ground_condition=metadata.ground_condition,
                ground_roughness=metadata.ground_roughness,
                ground_vegetation=metadata.ground_vegetation,
                name=site_id,
                observers=observer_list,
                precip=metadata.precip,
                sky_cover=metadata.sky_cover,
                slope_angle=metadata.slope,
                total_depth=metadata.total_depth,
                tree_canopy=metadata.tree_canopy,
                vegetation_height=metadata.vegetation_height,
                weather_description=metadata.weather_description,
                wind=metadata.wind,
            ))
        return campaign, observer_list, site

    def _add_instrument(self, metadata: SnowExProfileMetadata):
        """
        Add or lookup an instrument in the DB.

        Args:
            session: The database session
            metadata: SnowExProfileMetadata object

        Returns:
            Instrument DB record
        """
        # Give priority to passed information from kwargs
        instrumen_name = self._instrument or metadata.instrument
        instrument_model = self._instrument_model or metadata.instrument_model

        return self._check_or_add_object(
            self._session,
            Instrument,
            dict(name=instrumen_name, model=instrument_model)
        )

    def _add_entry(
        self, row: dict, campaign: Campaign,
        observer_list: List[Observer], site: Site, instrument: Instrument,
    ):
        """

        Args:
            row: dataframe row of data to add
            campaign: Campaign object inserted into db
            observer_list: List of Observers inserted into db
            site: the Site inserted into db
            instrument: Instrument found in metadata

        Returns:

        """
        # An instrument associated with a row has precedence over the
        # given via arguments
        if row.get('instrument') is not None:
            instrument = self._check_or_add_object(
                self._session,
                Instrument, dict(
                    name=row['instrument'],
                    model=row['instrument_model']
                )
            )

        # Add measurement type
        measurement_type = row["type"]
        measurement_obj = self._check_or_add_object(
            self._session,
            # Add units and 'derived' flag for the measurement
            MeasurementType, dict(
                name=measurement_type,
                units=row["units"],
                derived=self._derived
            )
        )

        # Now that the other objects exist and create the entry.
        new_entry = self.TABLE_CLASS(
            # Required record information
            depth=row["depth"],
            bottom_depth=row.get("bottom_depth"),
            value=row["value"],
            # Linked tables
            instrument=instrument,
            measurement_type=measurement_obj,
            site=site,
        )

        return new_entry


class UploadProfileBatch(BatchBase):
    """
    Class for submitting multiple files of profile type data.
    """

    UploaderClass = UploadProfileData
