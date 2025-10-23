"""
Script to upload the Snowex Time Series pits
"""

from earthaccess_data import get_files
from snowex_db.upload.layers import UploadProfileBatch

from snowexsql.db import db_session_with_credentials


DOI_MAP = {
    "SNEX20_TS_SP": "10.5067/KZ43HVLZV6G4",
}

def main(file_list: list, doi: str) -> None:
    """
    Add 2020 timeseries pits
    """
    campaign_name = "Time Series 2020"
    debug = True

    perimeter_depths_files = [file for file in file_list if "perimeterDepths" in file]
    summary_files = [file for file in file_list if "Summary" in file]
    site_details_files = [file for file in file_list if "siteDetails" in file]
    gap_filled_density_files = [file for file in file_list if "gapFilledDensity" in file]
    density_files = [file for file in file_list if 'data_density' in file]
    stratigraphy_files = [file for file in file_list if 'stratigraphy' in file]
    temperature_files = [file for file in file_list if 'data_temperature' in file]

    lwc_files = [file for file in file_list if 'data_LWC' in file]

    with db_session_with_credentials(credentials_path='./credentials.json') as (_engine, session):
        # Upload all the site details before uploading any profiles
        site_uploader = UploadProfileBatch(session, site_details_files, campaign=campaign_name, doi=doi, debug=debug)
        site_uploader.push()

        # Upload density separately to specify the instrument name which isnt provided in the file
        uploader = UploadProfileBatch(session, density_files, campaign=campaign_name, doi=doi,
                                      instrument='Density Cutter', debug=debug)
        uploader.push()

        # Upload  stratigraphy to specify instrument is manual
        uploader = UploadProfileBatch(session, stratigraphy_files, campaign=campaign_name, doi=doi,
                                      instrument='manual', debug=False)
        uploader.push()

        # Upload temperature profiles
        uploader = UploadProfileBatch(session, temperature_files, campaign=campaign_name, doi=doi,
                                      instrument='Thermometer', debug=False)
        uploader.push()

        # Upload LWC profiles
        uploader = UploadProfileBatch(session, lwc_files, campaign=campaign_name, doi=doi,
                                      instrument='WISe LWC Sensor', debug=False)
        uploader.push()
    # # Files to ignore
    # ignore_files = [
    #     "SNEX20_TS_SP_Summary_Environment_v02.csv",
    #     "SNEX20_TS_SP_Summary_SWE_v02.csv",
    #     "SNEX20_TS_SP_Summary_SWE_v02_modified.csv"
    # ]
    #
    # # Get all the date folders
    # unique_dt_olders = Path(
    #     data_dir
    # ).expanduser().absolute().glob("20*.*.*")
    # for udf in unique_dt_olders:
    #     # get all the csvs in the folder
    #     dt_folder_files = list(udf.glob("*.csv"))
    #     site_ids = []
    #     # Get the unique site ids for this date folder
    #     compiled = re.compile(
    #         r'SNEX20_TS_SP_\d{8}_\d{4}_([a-zA-Z0-9]*)_data_.*_v02\.csv'
    #     )
    #     for file_path in dt_folder_files:
    #         file_name = file_path.name
    #         if file_name in ignore_files:
    #             print(f"Skipping {file_name}")
    #             continue
    #         match = compiled.match(file_name)
    #         if match:
    #             code = match.group(1)
    #             site_ids.append(code)
    #         else:
    #             raise RuntimeError(f"No site ID found for {file_name}")
    #
    #     # Get the unique site ids
    #     site_ids = list(set(site_ids))
    #
    #     for site_id in site_ids:
    #         abbrev = site_id[0:2]
    #         tz = [k for k, states in tz_map.items() if abbrev in states][0]
    #
    #         # Grab all the csvs in the pits folder
    #         filenames = glob.glob(join(str(udf), f'*_{site_id}_*.csv'))
    #
    #         # Grab all the site details files
    #         sites = glob.glob(join(
    #             str(udf), f'*_{site_id}_*siteDetails*.csv'
    #         ))
    #
    #         # Grab all the perimeter depths and remove them for now.
    #         perimeter_depths = glob.glob(join(
    #             str(udf), f'*_{site_id}_*perimeterDepths*.csv'
    #         ))
    #
    #         # Use no-gap-filled density for the sole reason that
    #         # Gap filled density for profiles where the scale was broken
    #         # are just an empty file after the headers. We should
    #         # Record that Nan density was collected for the profile
    #         density_files = glob.glob(join(
    #             str(udf), f'*_{site_id}_*_gapFilledDensity_*.csv'
    #         ))
    #
    #         # Remove the site details from the total file list to get only the
    #         profiles = list(
    #             set(filenames) - set(sites) - set(perimeter_depths) -
    #             set(density_files)  # remove non-gap-filled denisty
    #         )
    #
    #         # Submit all profiles associated with pit at a time
    #         b = UploadProfileBatch(
    #             filenames=profiles, debug=debug, doi=doi, in_timezone=tz,
    #             db_name=db_name,
    #             allow_split_lines=True  # Logic for split header lines
    #         )
    #         b.push()
    #         error_msg += b.errors
    #
    #         # Upload the site details
    #         sd = UploadSiteDetailsBatch(
    #             filenames=sites, debug=debug, doi=doi, in_timezone=tz,
    #             db_name=db_name
    #         )
    #         sd.push()
    #         error_msg += sd.errors
    #
    #         # Submit all perimeters as point data
    #         with db_session(
    #             db_name, credentials='credentials.json'
    #         ) as (session, engine):
    #             for fp in perimeter_depths:
    #                 pcsv = PointDataCSV(
    #                     fp, doi=doi, debug=debug, depth_is_metadata=False,
    #                     in_timezone=tz,
    #                     allow_split_lines=True  # Logic for split header lines
    #                 )
    #                 pcsv.submit(session)
    #
    # for f, m in error_msg:
    #     print(f)
    #
    # return len(error_msg)


if __name__ == '__main__':
    for data_set_id, doi in DOI_MAP.items():
        with get_files(data_set_id, doi, key_words="*.csv") as files:
            main(files, doi)
