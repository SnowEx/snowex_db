"""
Upload the snow classification data from
Liston, G. E. and M. Sturm. 2021

# To run with all the scripts
python run.py

# To run individually
python add_snow_classification.py

"""
from datetime import date
from os.path import join
from subprocess import check_output
from snowex_db.batch import UploadRasterBatch


def main():
    """
    Uploader script for ASO Snow off data
    """

    # Typical kwargs
    kwargs = {'instrument': 'None',
              'observers': 'Liston, Sturm',
              'description': 'Seasonal-Snow Classification 300m cropped to Western US',
              'tiled': True,
              'epsg': 4326,
              'no_data': 9,
              'in_timezone': 'UTC',
              'doi': 'https://doi.org/10.5067/99FTCYYYLAQ0',
              'date': None,
              'type': "snow_classification",
              'units': None
              }

    # Directory of Cropped Snow classification
    directory = '../download/data/pub/DATASETS/nsidc0768_global_seasonal_snow_classification_v01/'
    original = join(directory, 'SnowClass_NA_300m_10.0arcsec_2021_v01.0.tif')
    final = join(directory, 'cropped_snow_classification.tif')
    # Crop the file to just the western US
    cmd = f'gdalwarp -overwrite -te -125 32 -100 50 {original} {final}'
    print(f'Executing {cmd}')
    check_output(cmd, shell=True)
    # Upload
    uploader = UploadRasterBatch([final], **kwargs)
    uploader.push()
    errors = len(uploader.errors)
    return errors


# Add this so you can run your script directly without running run.py
if __name__ == '__main__':
    main()
