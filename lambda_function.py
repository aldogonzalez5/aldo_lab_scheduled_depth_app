import statistics

from service.constants import DATASET_NAME, DATASET_PROVIDER

from corva import Api, Cache, Logger, ScheduledDepthEvent, scheduled


@scheduled
def lambda_handler(event: ScheduledDepthEvent, api: Api, cache: Cache):
    try:
    # 3. Here is where you can declare your variables from the argument event: ScheduledDepthEvent and start using Api, Cache and Logger functionalities. 

        # The scheduled app can declare the following attributes from the ScheduledDepthEvent: asset_id: The asset identifier; company_id: The company identifier; top_depth: The start depth in ft.; bottom_depth: The end depth in ft.; interval: distance between two schedule triggers; log_identifier: app stream log identifier. Used to scope data by stream. The asset may be connected to multiple depth based logs.
        asset_id = event.asset_id
        company_id = event.company_id
        log_identifier = event.log_identifier
        top_depth = event.top_depth
        bottom_depth = event.bottom_depth
        interval = event.interval


    # 4. Utilize the attributes from the ScheduledDepthEvent to make an API request to corva#drilling.wits.depth or any desired depth type dataset.
        # You have to fetch the realtime drilling data for the asset based on start and end time of the event.
        # start_time and end_time are inclusive so the query is structured accordingly to avoid processing duplicate data
        # We are only querying for dep field since that is the only field we need. It is nested under data. We are using the SDK convenience method api.get_dataset. See API Section for more information on convenience method. 
        records = api.get_dataset(
            provider="corva",
            dataset= "drilling.wits.depth",
            query={
                'asset_id': asset_id,
                'log_identifier': log_identifier,
                'measured_depth': {
                    '$gte': top_depth,
                    '$lte': bottom_depth,
                },
            },
            sort={'measured_depth': 1},
            limit=500,
            fields="data.depth, timestamp_read, measured_depth"
        )

        record_count = len(records)

        # Utilize the Logger functionality. The default log level is Logger.info. To use a different log level, the log level must be specified in the manifest.json file in the "settings.environment": {"LOG_LEVEL": "DEBUG"}. See the Logger documentation for more information.
        Logger.debug(f"{asset_id=} {company_id=}")
        Logger.debug(f"{top_depth=} {bottom_depth=} {record_count=}")

        Logger.debug(f"top_depth: {top_depth}")
        Logger.debug(f"top_depth: {bottom_depth}")
        Logger.debug(f"log_identifier: {log_identifier}")
        Logger.debug(f"asset_id: {asset_id}")
        Logger.debug(f"default_headers: {api.default_headers}")
        Logger.debug(f"api_url: {api.api_url}")
        Logger.debug(f"data_api_url: {api.data_api_url}")
        Logger.debug(f"api_key: {api.api_key}")


    # 5. Implementing some calculations
        # Computing mean dep value from the list of realtime drilling.wits.depth records
        mean_dep = statistics.mean(record.get("data", {}).get("depth", 0) for record in records)

        # Utililize the Cache functionality to get a set key value. The Cache functionality is built on Redis Cache. See the Cache documentation for more information.
        # Getting last exported depth from Cache 
        last_exported_measured_depth = float(cache.get(key='measured_depth') or 0)

        # Making sure we are not processing duplicate data
        if bottom_depth <= last_exported_measured_depth:
            Logger.debug(f"Already processed data until {last_exported_measured_depth=}")
            return None

    # 6. This is how to set up a body of a POST request to store the mean dep data and the top_depth and bottom_depth of the interval from the event.
        output = {
            "measured_depth": records[-1]["measured_depth"],
            "asset_id": asset_id,
            "company_id": company_id,
            "provider": DATASET_PROVIDER,
            "collection": DATASET_NAME,
            "log_identifier": log_identifier,
            "data": {
                "mean_dep": mean_dep,
                "top_depth": top_depth,
                "bottom_depth": bottom_depth
            },
            "version": 1
        }

        # Utilize the Logger functionality.
        Logger.debug(f"{asset_id=} {company_id=}")
        Logger.debug(f"{top_depth=} {bottom_depth=} {record_count=}")
        Logger.debug(f"{output=}")

    # 7. Save the newly calculated data in a custom dataset

        # Utilize the Api functionality. The data=outputs needs to be an an array because Corva's data is saved as an array of objects. Objects being records. See the Api documentation for more information.
        api.post(
            f"api/v1/data/{DATASET_PROVIDER}/{DATASET_NAME}/", data=[output],
        ).raise_for_status()

        # Utililize the Cache functionality to set a key value. The Cache functionality is built on Redis Cache. See the Cache documentation for more information. This example is setting the last measured_depth of the output to Cache
        cache.set(key='measured_depth', value=records[-1]["measured_depth"])

        return output
    except Exception as ex:
        Logger.debug(f"error: {str(ex)}")
        raise ex
