from corva import ScheduledDepthEvent
from lambda_function import lambda_handler


def test_app(app_runner):
    event = ScheduledDepthEvent(
        company_id=1, asset_id=1234, top_depth=0, bottom_depth=1, log_identifier="log_identifier", interval=1
    )

    app_runner(lambda_handler, event=event)
