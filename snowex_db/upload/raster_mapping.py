from copy import deepcopy
from enum import Enum
from pathlib import Path
from typing import Dict, List, Tuple

from snowex_db.interpretation import get_InSar_flight_comment
from snowex_db.metadata import read_InSar_annotation


#     dname_map = {'int': 'interferogram',
#                  'amp1': 'amplitude of pass 1',
#                  'amp2': 'amplitude of pass 2',
#                  'cor': 'correlation'}


class RasterType(Enum):
    """
    Enum for different types of raster files.
    value, description, abbreviation
    """
    DEM = "DEM"
    DEPTH = "depth"
    SWE = "swe"
    CANOPY_HEIGHT = "canopy_height"
    # SAR raster types
    INT = "int", "interferogram", "interferogram"
    AMP1 = "amp1", "amplitude of pass 1", "amplitude", 1
    AMP2 = "amp2", "amplitude of pass 2", "amplitude", 2
    COR = "corr", "correlation", "correlation"

    def __new__(cls, *args, **kwds):
        obj = object.__new__(cls)
        obj._value_ = args[0]
        return obj

    # ignore the first param since it's already set by __new__
    def __init__(
            self, _: str, description: str = None,
            abbreviation: str = None,
            pass_num: int = None
    ):
        """
        Args:
            _: original args for the enum
            description: a description of the raster type
            abbreviation: the abbreviation for the raster type
        """
        self._description_ = description
        self._abbrv_ = abbreviation
        self._pass_num_ = pass_num

    def __str__(self):
        return self.value

    @property
    def description(self):
        return self._description_

    @property
    def abbreviation(self):
        return self._abbrv_

    @property
    def pass_num(self):
        return self._pass_num_


def rasters_from_annotation(
        annotation_file: Path, tif_dir: Path, **kwargs
) -> List[Tuple[Dict[str, str], Path]]:
    """
    Given an annotation file, return the insar raster files associated with
    the file. Also return the metadata
    Args:
        annotation_file: uavsar annotation file
        tif_dir: Path to the directory where the tif files are stored
        **kwargs: Additional keyword arguments for metadata

    Returns:
        List of tuples containing metadata and the path to the raster file.

    """
    result = []
    # form the pattern to look for and grab the tifs
    pattern = annotation_file.name.removesuffix('.ann') + '*.tif'
    # Grab information from the filename
    tif_files = list(tif_dir.glob(pattern))
    for tif_path in tif_files:
        # Split the file into parts
        f_pieces = tif_path.name.split('.')
        component = f_pieces[-2]  # Real or imaginary component
        data_abbr = f_pieces[-3]  # Key to the data name
        raster_type = RasterType[data_abbr.upper()]
        metadata = meta_from_annotation_file(
            annotation_file, raster_type, component, **kwargs
        )
        result.append((metadata, tif_path))
    return result


def meta_from_annotation_file(
        annotation_file: Path, raster_type: RasterType,
        component: str, **kwargs
) -> dict:
    """
    Given an annotation file, return the metadata associated with this
    Args:
        annotation_file: Path to the annotation file
        raster_type: The type of raster file (e.g., interferogram, amplitude, etc.)
        component: The component of the raster file (e.g., real, imaginary)
        **kwargs: Additional keyword arguments for metadata
    Returns:
        A dictionary containing metadata for the raster file.
        keys are:
        - type: Type of the raster file
        - date: Date of the acquisition
        - units: Units of the raster file
        - comments: Comments about the raster file

    """
    meta = deepcopy(kwargs)
    desc = read_InSar_annotation(annotation_file)
    # Populate the metadata
    # TODO: Not type anymore, this will go in measurement type
    meta['type'] = 'insar ' + raster_type.abbreviation

    if raster_type == RasterType.INT:
        meta['type'] += (' ' + component)

    # Assign the date for the respective flights
    if raster_type in [RasterType.AMP1, RasterType.AMP2]:
        meta['date'] = desc[
            f'start time of acquisition for pass {raster_type.pass_num}'
        ]['value']

    # Derived products always receive the date of the last overpass
    else:
        meta['date'] = desc['start time of acquisition for pass 2']['value']

    # Assign only the date not the date and time
    meta['date'] = meta['date'].date()

    # Assign units
    meta['units'] = desc[f'{raster_type.abbreviation} units']['value']

    # Flexibly form a comment for each of the products for dates
    comment = get_InSar_flight_comment(raster_type.description, desc)
    # add which dem was used which dictates the file name convert e.g.
    # ...VV_01.int.grd
    comment += ', DEM used = {}'.format(
        desc['dem used in processing']['value'])
    # Add the polarization to the comments
    comment += ', Polarization = {}'.format(
        desc['polarization']['value'])
    meta['comments'] = comment
    return meta


def metadata_from_single_file(
    raster_file: Path, raster_type: RasterType, **kwargs
):
    """
    Create the metadata dictionary from a single raster file and its type.
    Examples would be depth, dem, etc.

    Args:
        raster_file: Path to the raster file
        raster_type: RasterType enum indicating the type of raster
        **kwargs: Additional keyword arguments for metadata

    Returns:
        A dictionary containing metadata for the raster file.
        keys are:
        - type: Type of the raster file
        - date: Date of the acquisition
        - units: Units of the raster file
        - comments: Comments about the raster file

    """
    meta = deepcopy(kwargs)
    # Assign the type
    meta['type'] = raster_type.value
    # TODO: figure the rest of this out
    # Assign the date from the filename
    if 'date' not in meta:
        raise ValueError(
            "Date must be provided in metadata or extracted from filename."
        )

    # Assign units based on raster type
    if raster_type == RasterType.DEPTH:
        meta['units'] = 'meters'
    elif raster_type == RasterType.DEM:
        meta['units'] = 'meters'
    elif raster_type == RasterType.SWE:
        meta['units'] = 'meters'
    elif raster_type == RasterType.CANOPY_HEIGHT:
        meta['units'] = 'meters'
    else:
        meta['units'] = 'unknown'

    return meta
