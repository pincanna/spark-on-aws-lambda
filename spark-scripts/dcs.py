from pyspark.sql import SparkSession
from jinja2 import Environment, FileSystemLoader, select_autoescape
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pytz import timezone
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import boto3
import os
from uuid import uuid4

env = Environment(
    loader=FileSystemLoader("templates/"),
    autoescape=False
)

template = env.get_template("call_summary.html")

session_params = {}
if "AWS_PROFILE" in os.environ:
    session_params["profile_name"] = os.environ["AWS_PROFILE"]
session = boto3.Session(**session_params)

s3 = session.client("s3")
ses = session.client("ses")

eastern = timezone("America/Detroit")
now = datetime.now().astimezone(eastern)
current_day = now.date()
yesterday = current_day - timedelta(days=1)

spark = SparkSession.builder.appName("Daily Call Summary").master("local[*]").getOrCreate()

def format_duration(duration):
    delta = relativedelta(seconds=int(duration))
    return str(delta)

duration_udf = F.udf(format_duration, T.StringType())

store_calls = spark.read.json("s3a://pincanna-twilio/events_stream/endpoint=store_call_ended/")

# store_calls = store_calls.withColumn("duration", duration_udf(F.col("event.form_data.CallDuration")))

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
spark.sparkContext.setLogLevel("ERROR")

report = store_calls.select(
    # F.expr("event.form_data.CallSid").alias("id"),
    F.to_timestamp(F.expr("event.form_data.Timestamp"), "EEE, dd MMM yyyy HH:mm:ss Z").alias('timestamp'),
    F.expr("event.form_data.From").alias("customer"),
    F.expr("event.form_data.CallerName").alias("caller_name"),
    F.expr("event.form_data.CallerCity").alias("caller_city"),
    F.expr("event.form_data.CallerState").alias("caller_state"),
    F.expr("event.form_data.To").alias("agent"),
    F.expr("event.form_data.CalledCity").alias("store_city"),
    # F.col("duration").alias("call_duration"),
    F.col("event.form_data.CallDuration").alias('call_duration'),
    F.expr("event.form_data.CallStatus").alias("call_status")
)

report = report.withColumn("ds", F.to_date("timestamp"))

pd_report = report.to_pandas_on_spark()

table = pd_report.to_html(
    index=False,
    classes="table table-striped table-hover table-bordered table-sm",
    na_rep="-"
)

html = template.render(report_table=table, current_day=str(current_day), yesterday=str(yesterday))

report_id = str(uuid4())

report_object_key = f"summary_reports/call_summary/{current_day}/{report_id}.html"

s3.put_object(
    Bucket="pincanna-twilio",
    Key=report_object_key,
    Body=html.encode("utf-8"),
    ContentType="text/html",
)

presigned_link = s3.generate_presigned_url(
    ClientMethod="get_object",
    Params={
        "Bucket": "pincanna-twilio",
        "Key": report_object_key
    },
    ExpiresIn=10080
)

print(presigned_link)

email_body = f"""
<p>Good evening,</p>
<p>Here is the end of day call summary for {current_day}.</p>
<p><a href="{presigned_link}">Call Summary</a></p>
<p><i>This link will expire in 7 days on {current_day + timedelta(days=7)}</i></p>
"""

ses_response = ses.send_email(
    Source="telecom@pincanna.com",
    Destination={
        "ToAddresses": ["zach@pincanna.com"]
    },
    Message={
        "Subject": {
            "Data": "Daily Call Summary",
            "Charset": "UTF-8"
        },
        "Body": {
            "Html": {
                "Data": email_body,
                "Charset": "UTF-8"
            }
        }
    },
    ReplyToAddresses=["zach@pincanna.com"],
    ReturnPath="zach@pincanna.com"
)
