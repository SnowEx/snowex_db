"""
Module for functions that handle anything regarding coordinate projections.
"""
import rasterio
import utm
from geoalchemy2.elements import WKTElement
from rasterio.warp import Resampling, calculate_default_transform, reproject


def reproject_point_in_dict(info, is_northern=True, zone_number=None):
    """
    Add/ensure that northing, easting, utm_zone, latitude, longitude and epsg code
    are in the metadata. Default to always project the lat long (if provided) to
    the northing and easting.

    Args:
        info: Dictionary containing key northing/easting or latitude longitude
        is_northern: Boolean for which hemisphere this data is in
        zone_number: Integer for the utm zone to enforce, otherwise let utm
                    figure it out

    Returns:
        result: Dictionary containing all previous information plus a coordinates
                reprojected counter part
    """
    result = info.copy()

    # Convert any coords to numbers
    for c in ['northing', 'easting', 'latitude', 'longitude']:
        if c in result.keys():
            try:
                result[c] = float(result[c])
            except Exception:
                del result[c]

    keys = result.keys()
    # Use lat/long first
    if all([k in keys for k in ['latitude', 'longitude']]):
        easting, northing, utm_zone, letter = utm.from_latlon(
            result['latitude'],
            result['longitude'],  force_zone_number=zone_number)
        # String representation should not be np.float64, so cast to float
        result['easting'] = float(easting)
        result['northing'] = float(northing)
        result['utm_zone'] = utm_zone

    # Secondarily use the utm to add lat long
    elif all([k in keys for k in ['northing', 'easting', 'utm_zone']]):

        if isinstance(result['utm_zone'], str):
            result['utm_zone'] = \
                int(''.join([s for s in result['utm_zone'] if s.isnumeric()]))

        lat, long = utm.to_latlon(result['easting'], result['northing'],
                                  result['utm_zone'],
                                  northern=is_northern)

        result['latitude'] = lat
        result['longitude'] = long

    # Assuming NAD83, add epsg code
    if 'utm_zone' in result.keys():
        if result['utm_zone'] is not None:
            result['epsg'] = int(f"269{result['utm_zone']}")
    else:
        result['utm_zone'] = None
        result['epsg'] = None

    return result


def add_geom(info, epsg):
    """
    Adds the WKBElement to the dictionary

    Args:
        info: Dictionary containing easting and northing keys
        epsg: integer representing the projection code

    Returns:
        info: Dictionary containing everything it originally did plus a geom
              key with WKTElement value
    """
    # Add a geometry entry
    info['geom'] = WKTElement('SRID={}; POINT({} {})'
                              ''.format(
                                  epsg, info['easting'], info['northing']),
                              extended=True)
    return info


def reproject_raster_by_epsg(input_f, output_f, epsg):
    """
    Reproject a geotiff raster from one epsg to another

    Args:
        input_f: Input path to a geotiff
        output_f: Output  location of a reprojected geotiff
        epsg: Valid projection reference number
    """

    dst_crs = 'EPSG:{}'.format(epsg)

    with rasterio.open(input_f) as src:
        transform, width, height = calculate_default_transform(
            src.crs, dst_crs, src.width, src.height, *src.bounds)
        kwargs = src.meta.copy()
        kwargs.update({
            'crs': dst_crs,
            'transform': transform,
            'width': width,
            'height': height
        })

        with rasterio.open(output_f, 'w', **kwargs) as dst:
            for i in range(1, src.count + 1):
                reproject(
                    source=rasterio.band(src, i),
                    destination=rasterio.band(dst, i),
                    src_transform=src.transform,
                    src_crs=src.crs,
                    dst_transform=transform,
                    dst_crs=dst_crs,
                    resampling=Resampling.bilinear)
