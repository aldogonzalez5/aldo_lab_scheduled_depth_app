import json
import os

from service.constants import DATASET_PROVIDER, DATASET_NAME

from corva import ScheduledDepthEvent
from lambda_function import lambda_handler

TEST_STREAMS_PATH =  os.path.join(os.path.abspath(os.path.dirname(__file__)), 'streams')


class TestScheduleDepth:
    def test_app(self, app_runner, requests_mock):
        event = ScheduledDepthEvent(
            company_id=1, asset_id=1234, top_depth=0, bottom_depth=1, log_identifier="log_identifier", interval=1
        )
        stream_file = os.path.join(TEST_STREAMS_PATH, "corva_drilling_wits_depth.json")
        with open(stream_file, encoding="utf8") as raw_stream:
            corva_drilling_wits_depth_stream = json.load(raw_stream)

        requests_mock.get(f'https://data.localhost.ai/api/v1/data/corva/drilling.wits.depth/', json=corva_drilling_wits_depth_stream)
        requests_mock.post(f'https://data.localhost.ai/api/v1/data/{DATASET_PROVIDER}/{DATASET_NAME}/')
        app_runner(lambda_handler, event=event)
