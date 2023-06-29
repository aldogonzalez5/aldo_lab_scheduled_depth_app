from corva import Api, Cache, Logger, ScheduledDepthEvent, scheduled


@scheduled
def lambda_handler(event: ScheduledDepthEvent, api: Api, cache: Cache):
    """Insert your logic here"""
    Logger.info('Hello, World!')
