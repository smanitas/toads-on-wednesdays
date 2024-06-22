import json
import logging
from io import BytesIO

import pymsteams
import requests
from PIL import Image
from PIL import ImageDraw
from PIL import ImageFont
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


def _create_content_image(ti):
    image_url = ti.xcom_pull(task_ids="load_image", key="image_url")

    response = requests.get(image_url)
    image = Image.open(BytesIO(response.content)).convert("RGBA")
    draw = ImageDraw.Draw(image)
    font = ImageFont.truetype(FONT_PATH, size=20)

    # putting the text in the center of the image
    text_width, text_height = draw.textsize(PHRASE, font=font)
    horizontal_position = (image.width - text_width) // 2
    vertical_position = (image.height - text_height) // 15

    # drawing a background rectangle
    bbox = draw.textbbox((horizontal_position, vertical_position), PHRASE, font=font)
    draw.rectangle(bbox, fill="yellow")
    draw.text((horizontal_position, vertical_position), PHRASE, font=font, fill="green")

    image_io = BytesIO()
    image.convert("RGB").save(image_io, format="PNG")
    image_io.seek(0)
    freeimage_url = _upload_image_to_freeimage(image_io)

    logging.info("Content is created")
    ti.xcom_push(key="updated_image_url", value=freeimage_url)


def _upload_image_to_freeimage(image_io):
    freeimage_conn = BaseHook.get_connection("FREEIMAGE_API")
    freeimage_key = json.loads(freeimage_conn.extra)["api_key"]

    url = "https://freeimage.host/api/1/upload"
    files = {"source": ("image.png", image_io, "image/png")}
    data = {"key": freeimage_key, "action": "upload"}
    response = requests.post(url, files=files, data=data)

    if response.status_code == 200:
        response_json = response.json()
        logging.info(f"FreeImage response: {response_json}")
        return response_json.get("image", {}).get("url")
    else:
        logging.error(
            f"Failed to upload image to FreeImage. Status code: {response.status_code}"
        )
        return None


def _send_to_teams(ti):
    teams_conn = BaseHook.get_connection("TEAMS_WEBHOOK")
    webhook_url = teams_conn.host

    image_url = ti.xcom_pull(task_ids="create_content_image", key="updated_image_url")
    content = (
        f"Sent by {SENDER_NAME}\n\n"
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
        logging.exception(
            f"Failed to send content to Teams due to network connectivity issue: {e}")
        raise
    except Exception as e:
        logging.exception(
            f"An unexpected error occurred while sending content to Teams: {e}")
        raise
