import math
import requests
import pytest
import uuid

HOST = "localhost"
PORT = 5000

def test_creating_channel():
    cid = "C-" + uuid.uuid4().hex
    create_channel(cid, "Channel 1", "owner1")

    channel = get_channel(cid)
    assert channel["id"] == cid
    assert channel["name"] == "Channel 1"
    assert channel["owner"] == "owner1"

    delete_channel(cid)

    channel = get_channel_raw(cid)
    assert channel.status_code == 404

def test_creating_video():
    cid = "C-" + uuid.uuid4().hex
    create_channel(cid, "Channel Tests", "owner1")

    vid = "V-" + uuid.uuid4().hex
    create_video(cid, vid, "Video 1", "Description 1", 60)

    video = get_video(cid, vid)
    assert video["id"] == vid
    assert video["title"] == "Video 1"
    assert video["description"] == "Description 1"
    assert video["duration"] == 60

    delete_video(cid, vid)
    delete_channel(cid)

def test_listing_videos():
    cid = "C-" + uuid.uuid4().hex
    create_channel(cid, "Channel Tests", "owner1")

    vid1 = "V-" + uuid.uuid4().hex
    create_video(cid, vid1, "Video 1", "Description 1", 60)

    vid2 = "V-" + uuid.uuid4().hex
    create_video(cid, vid2, "Video 2", "Description 2", 120)

    vid3 = "V-" + uuid.uuid4().hex
    create_video(cid, vid3, "Video 3", "Description 3", 180)

    videos = list_videos(cid)
    assert len(videos) == 3

    videos = list_videos_min_duration(cid, 100)
    assert len(videos) == 2
    assert videos[0]["id"] in [vid2, vid3]
    assert videos[1]["id"] in [vid2, vid3]

    delete_channel(cid)

    channel_get_response = get_channel_raw(cid)
    assert channel_get_response.status_code == 404

    video_get_response = get_video_raw(cid, vid1)
    assert video_get_response.status_code == 404

    video_get_response = get_video_raw(cid, vid2)
    assert video_get_response.status_code == 404

    video_get_response = get_video_raw(cid, vid3)
    assert video_get_response.status_code == 404

def create_channel(channel_id, name, owner):
    response = requests.put(f'http://{HOST}:{PORT}/channels', json={"id": channel_id, "name": name, "owner": owner})
    assert response.status_code == 201
    return response.json()

def get_channel_raw(channel_id):
    response = requests.get(f'http://{HOST}:{PORT}/channels/{channel_id}')
    return response

def get_channel(channel_id):
    response = get_channel_raw(channel_id)
    assert response.status_code == 200
    return response.json()

def delete_channel(channel_id):
    response = requests.delete(f'http://{HOST}:{PORT}/channels/{channel_id}')
    assert response.status_code == 204
    return response

def create_video(channel_id, video_id, title, description, duration):
    response = requests.put(f'http://{HOST}:{PORT}/channels/{channel_id}/videos', json={"id": video_id, "title": title, "description": description, "duration": duration})
    assert response.status_code == 201
    return response.json()

def list_videos(channel_id):
    response = requests.get(f'http://{HOST}:{PORT}/channels/{channel_id}/videos')
    assert response.status_code == 200
    return response.json()

def list_videos_min_duration(channel_id, min_duration):
    response = requests.get(f'http://{HOST}:{PORT}/channels/{channel_id}/videos?minDuration={min_duration}')
    assert response.status_code == 200
    return response.json()

def get_video_raw(channel_id, video_id):
    response = requests.get(f'http://{HOST}:{PORT}/channels/{channel_id}/videos/{video_id}')
    return response

def get_video(channel_id, video_id):
    response = get_video_raw(channel_id, video_id)
    assert response.status_code == 200
    return response.json()

def delete_video(channel_id, video_id):
    response = requests.delete(f'http://{HOST}:{PORT}/channels/{channel_id}/videos/{video_id}')
    assert response.status_code == 204
    return response

def get_video_views(channel_id, video_id):
    response = requests.get(f'http://{HOST}:{PORT}/channels/{channel_id}/videos/{video_id}/views')
    assert response.status_code == 200
    return response.json()

def add_video_view(channel_id, video_id, viewer_id):
    response = requests.post(f'http://{HOST}:{PORT}/channels/{channel_id}/videos/{video_id}/views/register', json={"viewer_id": viewer_id})
    assert response.status_code == 201
    return response.json()