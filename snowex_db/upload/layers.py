"""
Module for classes that upload single files to the database.
"""
import time
from typing import List

import pandas as pd
import geopandas as gpd
import logging

from geoalchemy2 import WKTElement
from snowexsql.tables import LayerData
from snowexsql.tables import (
    Instrument, Campaign, Observer, DOI, MeasurementType, Site
)
from insitupy.campaigns.snowex import SnowExProfileData

from .batch import BatchBase
from ..string_management import parse_none
from ..utilities import get_logger
from .base import BaseUpload
from ..metadata import SnowExProfileMetadata


from ..profile_data import SnowExProfileDataCollection


LOG = logging.getLogger("snowex_db.upload.layers")


class DataValidationError(ValueError):
    pass


class UploadProfileData(BaseUpload):
    """
    Class for submitting a single profile. Since layers are uploaded layer by layer this allows for submitting them
    one file at a time.
    """
    expected_attributes = [c for c in dir(LayerData) if c[0] != '_']
    TABLE_CLASS = LayerData

    def __init__(self, profile_filename, timezone="US/Mountain", **kwargs):
        self.log = get_logger(__name__)

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

        # Read in data
        self.data = self._read(profile_filename)

    def _read(self, profile_filename) -> SnowExProfileDataCollection:
        """
        Read in a profile file. Managing the number of lines to skip and
        adjusting column names

        Args:
            profile_filename: Filename containing the a manually measured
                             profile
        Returns:
            list of ProfileData objects
        """
        try:
            data = SnowExProfileDataCollection.from_csv(
                profile_filename, timezone=self._timezone,
                header_sep=self._header_sep, site_id=self._id,
                campaign_name=self._campaign_name
            )
        except pd.errors.ParserError as e:
            LOG.error(e)
            raise RuntimeError(f"Failed reading {profile_filename}")

        return data

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

        df = profile.df.copy()
        if df.empty:
            LOG.debug("df is empty, returning")
            return df
        metadata = profile.metadata
        variable = profile.variable

        # The type of measurement
        df['type'] = [variable.code] * len(df)

        # Manage nans and nones
        for c in df.columns:
            df[c] = df[c].apply(lambda x: parse_none(x))
        df['value'] = df[variable.code].astype(str)

        if 'units' not in df.columns:
            unit_str = profile.units_map.get(variable.code)
            df['units'] = [unit_str] * len(df)

        columns = df.columns.values
        # Clean up comments a bit
        if 'comments' in columns:
            df['comments'] = df['comments'].apply(
                lambda x: x.strip(' ') if isinstance(x, str) else x)
            # Add pit comments
            if profile.metadata.comments:
                df["comments"] += profile.metadata.comments
        else:
            # Make comments to pit comments
            df["comments"] = [profile.metadata.comments] * len(df)

        # In case of SMP, pass comments in
        if self._comments is not None:
            df["comments"] = [self._comments] * len(df)

        # Add flags to the comments.
        flag_string = metadata.flags
        if flag_string:
            flag_string = " Flags: " + flag_string
            if 'comments' in columns:
                df["comments"] += flag_string
            else:
                df["comments"] = flag_string

        if 'instrument' not in columns:
            df["instrument"] = [self._instrument] * len(df)
        if 'doi' not in columns:
            df["doi"] = [self._doi] * len(df)
        if 'instrument_model' not in columns:
            df['instrument_model'] = self._instrument_model

        return df

    def submit(self, session):
        """
        Submit values to the db from dictionary. Manage how some profiles have
        multiple values and get submitted individual

        Args:
            session: SQLAlchemy session
        """

        # Construct a dataframe with all metadata
        for profile in self.data.profiles:
            df = self.build_data(profile)

            # Grab each row, convert it to dict and join it with site info
            if not df.empty:
                for row in df.to_dict(orient="records"):
                    row["geometry"] = WKTElement(
                        str(row["geometry"]),
                        srid=int(df.crs.srs.replace("EPSG:", ""))
                    )
                    campaign, observer_list, site = self._add_metadata(
                        session, profile.metadata, row=row
                    )
                    d = self._add_entry(
                        session, row, campaign, observer_list, site
                    )
                    # session.bulk_save_objects(objects) does not resolve
                    # foreign keys, DO NOT USE IT
                    session.add(d)
                    session.commit()
            else:
                # procedure to still upload metadata (sites, etc)
                self.log.warning(
                    'File contains header but no data which is sometimes'
                    ' expected. Skipping row submissions, and only inserting'
                    ' metadata.'
                )
                _ = self._add_metadata(
                    session, profile.metadata
                )

    def _add_metadata(
            self, session, metadata: SnowExProfileMetadata, row: dict = None
    ):
        """
        Add the metadata entry and return objects
        Args:
            session: db session object
            metadata: ProfileMetadata information
            row: Optional entry for row based info

        Returns:

        """
        # Add campaign
        campaign = self._check_or_add_object(
            session, Campaign, dict(name=metadata.campaign_name)
        )
        # add list of observers
        observer_list = []
        observer_names = metadata.observers or []
        for obs_name in observer_names:
            observer = self._check_or_add_object(
                session, Observer, dict(name=obs_name)
            )
            observer_list.append(observer)

        # Add site
        site_id = metadata.site_name
        if row is None:
            # Datetime from metadata
            dt = metadata.date_time
            # Form geom from lat and lon
            geom = WKTElement(
                f"Point ({metadata.longitude} {metadata.latitude})",
                srid=4326
            )
        else:
            geom = row["geometry"]
            dt = row["datetime"]

        site = self._check_or_add_object(
            session, Site, dict(name=site_id),
            object_kwargs=dict(
                name=site_id, campaign=campaign,
                datetime=dt,
                geom=geom,
                observers=observer_list,
                aspect=metadata.aspect,
                slope_angle=metadata.slope,
                air_temp=metadata.air_temp,
                total_depth=metadata.total_depth,
                weather_description=metadata.weather_description,
                precip=metadata.precip,
                sky_cover=metadata.sky_cover,
                wind=metadata.wind,
                ground_condition=metadata.ground_condition,
                ground_roughness=metadata.ground_roughness,
                ground_vegetation=metadata.ground_vegetation,
                vegetation_height=metadata.vegetation_height,
                tree_canopy=metadata.tree_canopy,
                site_notes=metadata.site_notes,
            ))
        return campaign, observer_list, site

    def _add_entry(
        self, session, row: dict, campaign: Campaign,
        observer_list: List[Observer], site: Site
    ):
        """

        Args:
            session: db session object
            row: dataframe row of data to add
            campaign: Campaign object inserted into db
            observer_list: List of Observers inserted into db
            site: the Site inserted into db

        Returns:

        """
        # Add instrument
        instrument = self._check_or_add_object(
            session, Instrument, dict(
                name=row['instrument'],
                model=row['instrument_model']
            )
        )

        # Add doi
        doi_string = row["doi"]
        if doi_string is not None:
            doi = self._check_or_add_object(
                session, DOI, dict(doi=doi_string)
            )
        else:
            doi = None

        # Add measurement type
        measurement_type = row["type"]
        measurement_obj = self._check_or_add_object(
            # Add units and 'derived' flag for the measurement
            session, MeasurementType, dict(
                name=measurement_type,
                units=row["units"],
                derived=self._derived
            )
        )

        # Now that the other objects exist, create the entry,
        # notice we only need the instrument object
        new_entry = self.TABLE_CLASS(
            # Linked tables
            instrument=instrument,
            doi=doi,
            measurement_type=measurement_obj,
            site=site,
            # Arguments from kwargs
            depth=row["depth"],
            bottom_depth=row.get("bottom_depth"),
            comments=row["comments"],
            value=row["value"],
        )
        return new_entry


class UploadProfileBatch(BatchBase):
    """
    Class for submitting multiple files of profile type data.

    Attributes:
        smp_log_f: CSV providing metadata for profile_filenames.
    """

    UploaderClass = UploadProfileData

    def push(self):
        """
        An overwritten push function to account for managing SMP meta data.
        """
        self.start = time.time()

        i = 0

        # Loop over all the ssa files and upload them
        if self.n_files != -1:
            self.filenames[0:self.n_files]

        for i, f in enumerate(self.filenames):

            # if smp_file:
            #     extras = self.smp_log.get_metadata(f)
            #     meta.update(extras)

            # If were not debugging script allow exceptions and report them
            # later
            if not self.debug:
                try:
                    self._push_one(f, **self._kwargs)

                except Exception as e:
                    self.log.error('Error with {}'.format(f))
                    self.log.error(e)
                    self.errors.append((f, e))

            else:
                self._push_one(f, **self._kwargs)

        self.report(i + 1)
