"""
Module for classes that upload single files to the database.
"""
from typing import List

import pandas as pd
import geopandas as gpd
import logging

from geoalchemy2 import WKTElement
from snowexsql.tables import LayerData
from snowexsql.tables import Instrument, Campaign, Observer, DOI, MeasurementType, Site
from insitupy.campaigns.snowex import SnowExProfileData
from insitupy.profiles.metadata import ProfileMetaData

from ..string_management import parse_none
from ..utilities import get_logger
from .base import BaseUpload


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
                profile_filename, timezone=self._timezone
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
        metadata = profile.metadata
        variable = profile.variable
        # TODO: build more metadata
        #   Row based timezone? (alaska)
        #   Row based crs? (alaska)
        #   depth_is_metadata (
        #       This may not be relevant since point data is in a different table,
        #       But this means depth is a descriptor not the main value
        #   )

        # The type of measurement
        df['type'] = [variable.code] * len(df)

        # Manage nans and nones
        for c in df.columns:
            df[c] = df[c].apply(lambda x: parse_none(x))
        df['value'] = df[variable.code].astype(str)

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
                objects = []
                for row in df.to_dict(orient="records"):
                    row["geometry"] = WKTElement(
                        str(row["geometry"]),
                        srid=int(df.crs.srs.replace("EPSG:", ""))
                    )
                    d = self._add_entry(
                        session, row, profile.metadata
                    )
                    # session.bulk_save_objects(objects) does not resolve
                    # foreign keys, DO NOT USE IT
                    session.add(d)
                    session.commit()
            else:
                self.log.warning(
                    'File contains header but no data which is sometimes'
                    ' expected. Skipping db submission.'
                )

    def _add_entry(
        self, session, row: dict, metadata: ProfileMetaData
    ):
        # Add instrument
        # TODO: what comes from row vs metadata?
        instrument = self._check_or_add_object(
            session, Instrument, dict(name=row['instrument'])
        )

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
        dt = row["datetime"]

        site = self._check_or_add_object(
            session, Site, dict(name=site_id),
            object_kwargs=dict(
                name=site_id, campaign=campaign,
                datetime=dt,
                geom=row["geometry"],
                observers=observer_list,
                # TODO: more parameters
            ))

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
            session, MeasurementType, dict(name=measurement_type)
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
