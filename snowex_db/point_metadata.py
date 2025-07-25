import logging
from typing import Tuple, Union

from insitupy.campaigns.snowex.snowex_metadata import SnowExMetaDataParser
from insitupy.profiles.metadata import ProfileMetaData

LOG = logging.getLogger()


class PointSnowExMetadataParser(SnowExMetaDataParser):
    """
    Extend the parser to update the extended variables
    """

    def find_header_info(self, filename):
        """
        Read in all site details file for a pit If the filename has the word
        site in it then we read everything in the file. Otherwise, we use this
        to read all the site data up to the header of the profile.

        E.g. Read all commented data until we see a column descriptor.

        Args:
            filename: Path to a csv containing # leading lines with site details

        Returns:
            tuple: **data** - Dictionary containing site details
                   **columns** - List of clean column names
                   **header_pos** - Index of the columns header for skiprows in
                                    read_csv
       """
        filename = str(filename)
        with open(filename, encoding='latin') as fp:
            lines = fp.readlines()
            fp.close()

        header_pos, header_indicator = self._find_header_position(lines)
        strip_out = ["(hh:mm, local, MST)"]
        header_line = lines[header_pos]
        # Strip out unhelpful string segments that break parsing
        for so in strip_out:
            header_line = header_line.replace(so, "")
        # identify columns, map columns, and units map
        columns, columns_map, units_map = self._parse_columns(header_line)
        # Combine with user defined units map
        self._units_map = {**self._units_map, **units_map}
        LOG.debug(
            f'Column Data found to be {len(columns)} columns based on'
            f' Line {header_pos}'
        )
        # Only parse what we know if the header
        lines = lines[0:header_pos]

        final_lines = lines

        # Clean up the lines from line returns to grab header info
        final_lines = [ln.strip() for ln in final_lines]
        # Join all data and split on header separator
        # This handles combining split lines
        str_data = " ".join(final_lines).split('#')
        str_data = [ln.strip() for ln in str_data if ln]

        return str_data, columns, columns_map, header_pos

    def parse(self, filename: str) -> (
            Tuple)[Union[ProfileMetaData | None], list, dict, int]:
        """
        Parse the file and return a metadata object.
        We can override these methods as needed to parse the different
        metadata

        This populates self.rough_obj

        Args:
            filename: (str) Full path to the file with the header info to parse

        Returns:
            Tuple:
                metadata object or None, column list, position of header in file
        """
        (
            meta_lines, columns, columns_map, header_position
        ) = self.find_header_info(filename)
        self._rough_obj = self._preparse_meta(meta_lines)
        # We do not have header metadata for point files
        if not self.rough_obj:
            LOG.debug(
                "No metadata found in the file header, "
                "using default no extra metadata"
            )
            metadata = None
        else:
            # In the case we have metadata (like for a perimeter file)
            LOG.debug(
                f"Metadata found in the file header: {self.rough_obj}"
            )
            metadata = ProfileMetaData(
                site_name=self.parse_id(),
                date_time=self.parse_date_time(),
                latitude=self.parse_latitude(),
                longitude=self.parse_longitude(),
                utm_epsg=str(self.parse_utm_epsg()),
                campaign_name=self.parse_campaign_name(),
                flags=self.parse_flags(),
                observers=self.parse_observers()
            )
        return metadata, columns, columns_map, header_position
