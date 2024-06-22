import json
import logging

import pymsteams
import requests
from airflow.hooks.base import BaseHook
from airflow.models import Variable

SENDER_NAME = Variable.get("SENDER_NAME")
PHRASE = "IT IS WEDNESDAY, MY DUDES!"
FONT_PATH = "/opt/airflow/dags/scripts/DejaVuSans-Bold.ttf"


def _load_image(ti):
    unsplash_conn = BaseHook.get_connection("UNSPLASH_API")
    unsplash_token = json.loads(unsplash_conn.extra)["client_id"]
    api_url = (
        f"https://api.unsplash.com/photos/random"
        f"?query=frog"
        f"&orientation=landscape"
        f"&client_id={unsplash_token}"
    )
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        image_url = response.json()["urls"]["small"]
        if image_url:
            ti.xcom_push(key="image_url", value=image_url)
            logging.info("Image URL is successfully loaded")
        else:
            raise ValueError("Failed to load image URL")
    except (requests.exceptions.RequestException, ValueError) as e:
        logging.exception(f"Error loading image: {e}")


def _send_to_teams(**kwargs):
    teams_conn = BaseHook.get_connection("TEAMS_WEBHOOK")
    webhook_url = teams_conn.host

    ti = kwargs["ti"]
    image_url = ti.xcom_pull(task_ids="load_image", key="image_url")
    logging.debug(f"Using SENDER_NAME: {SENDER_NAME}")

    content = (
        f"Sent by {SENDER_NAME}\n\n"
         f"{PHRASE}\n\n"
        f"![Image]({image_url})"
    )

    try:
        teams_message = pymsteams.connectorcard(webhook_url)
        teams_message.text(content)
        teams_message.send()
        logging.info("Content is successfully sent to Teams")
    except pymsteams.TeamsWebhookException as e:
        logging.exception(f"Failed to send content to Teams due to webhook error: {e}")
        raise
    except requests.RequestException as e:
        logging.exception(f"Failed to send content to Teams due to network connectivity issue: {e}")
        raise
    except Exception as e:
        logging.exception(f"An unexpected error occurred while sending content to Teams: {e}")
        raise
