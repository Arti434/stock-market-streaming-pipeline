"""
Dataflow Streaming Pipeline
Reads stock tick messages from Pub/Sub
Writes to BigQuery in real time
"""

import json
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    StandardOptions,
    GoogleCloudOptions,
    SetupOptions
)
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PROJECT   = "stock-stream-pipeline-001"
BUCKET    = "stock-stream-pipeline-001-data"
DATASET   = "stock_data"
TABLE     = "stock_ticks"
SUB_ID    = "stock-tick-data-sub"

BQ_SCHEMA = {
    "fields": [
        {"name": "symbol",     "type": "STRING"},
        {"name": "price",      "type": "FLOAT"},
        {"name": "volume",     "type": "INTEGER"},
        {"name": "change_pct", "type": "FLOAT"},
        {"name": "market_cap", "type": "FLOAT"},
        {"name": "event_time", "type": "TIMESTAMP"},
    ]
}


class ParseMessage(beam.DoFn):
    """Parse Pub/Sub message bytes into a dict."""

    def process(self, message):
        try:
            data = json.loads(message.decode("utf-8"))

            # Validate required fields
            if not data.get("symbol"):
                return
            if not data.get("price"):
                return

            yield {
                "symbol"    : str(data["symbol"]),
                "price"     : float(data["price"]),
                "volume"    : int(data.get("volume", 0)),
                "change_pct": float(data.get("change_pct", 0)),
                "market_cap": float(data.get("market_cap", 0)),
                "event_time": str(data.get("event_time", "")),
            }

        except Exception as e:
            logger.error(f"Error parsing message: {e}")
            return


def run():
    subscription = f"projects/{PROJECT}/subscriptions/{SUB_ID}"
    table_ref    = f"{PROJECT}:{DATASET}.{TABLE}"

    options = PipelineOptions()
    options.view_as(SetupOptions).save_main_session = True

    std_options = options.view_as(StandardOptions)
    std_options.runner   = "DataflowRunner"
    std_options.streaming = True

    gcp_options = options.view_as(GoogleCloudOptions)
    gcp_options.project          = PROJECT
    gcp_options.region           = "us-central1"
    gcp_options.job_name         = "stock-stream-pipeline"
    gcp_options.staging_location = f"gs://{BUCKET}/staging"
    gcp_options.temp_location    = f"gs://{BUCKET}/tmp"
    gcp_options.service_account_email = (
        f"stock-pipeline-sa@{PROJECT}.iam.gserviceaccount.com"
    )

    logger.info(f"Starting streaming pipeline")
    logger.info(f"Subscription: {subscription}")
    logger.info(f"Target table: {table_ref}")

    with beam.Pipeline(options=options) as p:
        (
            p
            | "ReadFromPubSub"  >> ReadFromPubSub(
                subscription=subscription,
                with_attributes=False
            )
            | "ParseMessages"   >> beam.ParDo(ParseMessage())
            | "WriteToBigQuery" >> WriteToBigQuery(
                table              = table_ref,
                schema             = BQ_SCHEMA,
                write_disposition  = BigQueryDisposition.WRITE_APPEND,
                create_disposition = BigQueryDisposition.CREATE_IF_NEEDED,
                custom_gcs_temp_location = f"gs://{BUCKET}/tmp"
            )
        )

    logger.info("Pipeline submitted to Dataflow!")


if __name__ == "__main__":
    run()