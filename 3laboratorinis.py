from flask import Flask, request, jsonify 
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

app = Flask(__name__)

cluster = Cluster(['127.0.0.1']) 
session = cluster.connect('cassandra_duombaze')  

def channel_key(channel_id):
    return f"channel#{channel_id}"

def video_key(video_id):
    return f"video#{video_id}"

def remove_channel_key(formatted_channel_id):
    return formatted_channel_id.replace("channel#", "")

def remove_video_key(formatted_video_id):
    return formatted_video_id.replace("video#", "")

#-------------------------------------------------------------------------------------------------------
@app.route('/channels', methods=['PUT'])
def create_channel():
    data = request.json
    channel_id = data.get("id")
    name = data.get("name")
    owner = data.get("owner")

    if not channel_id or not name or not owner:
        return jsonify({"error": "Missing required fields: id, name, or owner"}), 400

    formatted_channel_id = channel_key(channel_id)
    query_check = SimpleStatement("SELECT id FROM channels WHERE id = %s")
    if session.execute(query_check, (formatted_channel_id,)).one():
        return jsonify({"error": "Channel with this ID already exists"}), 409

    query = SimpleStatement(
        "INSERT INTO channels (id, name, owner) VALUES (%s, %s, %s) IF NOT EXISTS"
    )
    session.execute(query, (formatted_channel_id, name, owner))
    return jsonify({"id": remove_channel_key(formatted_channel_id)}), 201

#-------------------------------------------------------------------------------------------------------
@app.route('/channels', methods=['GET'])
def get_channels():
    owner = request.args.get("owner")

    if owner == "" or (owner is not None and (not isinstance(owner, str) or owner.isdigit())):
        return jsonify({"error": "Invalid input, owner must be a string or not provided"}), 400
    
    if owner:
        query = SimpleStatement(
            "SELECT id, name, owner FROM cassandra_duombaze.channels_by_owner WHERE owner = %s"
        )
        rows = session.execute(query, (owner,))
    else:
        query = SimpleStatement("SELECT id, name, owner FROM cassandra_duombaze.channels")
        rows = session.execute(query)

    channels = [{"id": remove_channel_key(row.id), "name": row.name, "owner": row.owner} for row in rows]
    if not channels:
        return jsonify({"error": "Channel not found"}), 404
    return jsonify(channels), 200


#-------------------------------------------------------------------------------------------------------
@app.route('/channels/<channel_id>', methods=['GET'])
def get_channel(channel_id):
    formatted_channel_id = channel_key(channel_id)
    query = SimpleStatement("SELECT id, name, owner FROM channels WHERE id = %s")
    row = session.execute(query, (formatted_channel_id,)).one()
    if not row:
        return jsonify({"error": "Channel not found"}), 404
    return jsonify({
        "id": remove_channel_key(row.id),
        "name": row.name,
        "owner": row.owner
    }), 200

#-------------------------------------------------------------------------------------------------------
@app.route('/channels/<channel_id>', methods=['DELETE'])
def delete_channel(channel_id):
    formatted_channel_id = channel_key(channel_id)
    query = SimpleStatement("SELECT id FROM channels WHERE id = %s")
    if not session.execute(query, (formatted_channel_id,)).one():
        return jsonify({"error": "Channel not found"}), 404

    delete_channel_query = SimpleStatement("DELETE FROM channels WHERE id = %s")
    session.execute(delete_channel_query, (formatted_channel_id,))
    delete_videos_query = SimpleStatement("DELETE FROM videos WHERE channel_id = %s")
    session.execute(delete_videos_query, (formatted_channel_id,))
    return jsonify({"message": "Channel and its videos deleted"}), 204

#-------------------------------------------------------------------------------------------------------
@app.route('/channels/<channel_id>/videos', methods=['PUT'])
def add_video(channel_id):
    data = request.json
    video_id = data.get("id")
    title = data.get("title")
    description = data.get("description")
    duration = data.get("duration")

    if not video_id or not title or not description or not duration:
        return jsonify({"error": "Missing required fields: id, title, description, or duration"}), 400
    
    formatted_video_id = video_key(video_id)
    formatted_channel_id = channel_key(channel_id)
    query_check = SimpleStatement("SELECT id FROM videos WHERE channel_id = %s AND id = %s")
    if session.execute(query_check, (formatted_channel_id, formatted_video_id)).one():
        return jsonify({"error": "Video with this ID already exists"}), 409

    query = SimpleStatement(
        "INSERT INTO videos (id, channel_id, title, description, duration) VALUES (%s, %s, %s, %s, %s) IF NOT EXISTS"
    )
    session.execute(query, (formatted_video_id, formatted_channel_id, title, description, duration))
    return jsonify({"id": remove_video_key(formatted_video_id)}), 201

#-------------------------------------------------------------------------------------------------------
@app.route('/channels/<channel_id>/videos', methods=['GET'])
def get_videos(channel_id):
    min_duration = request.args.get('minDuration', type=int)
    formatted_channel_id = channel_key(channel_id)

    if min_duration:
        query = SimpleStatement(
            "SELECT id, title, description, duration FROM cassandra_duombaze.videos_by_duration WHERE channel_id = %s AND duration >= %s"
        )
        rows = session.execute(query, (formatted_channel_id, min_duration))
    else:
        query = SimpleStatement("SELECT id, title, description, duration FROM cassandra_duombaze.videos WHERE channel_id = %s")
        rows = session.execute(query, (formatted_channel_id,))

    if not rows:
        return jsonify({"error": "No videos found for this channel."}), 404

    videos = [{"id": remove_video_key(row.id), "title": row.title, "description": row.description, "duration": row.duration} for row in rows]
    return jsonify(videos), 200

#-------------------------------------------------------------------------------------------------------
@app.route('/channels/<channel_id>/videos/<video_id>', methods=['GET'])
def get_video(channel_id, video_id):
    formatted_channel_id = channel_key(channel_id)
    formatted_video_id = video_key(video_id)
    query = SimpleStatement("SELECT id, title, description, duration FROM videos WHERE channel_id = %s AND id = %s")
    row = session.execute(query, (formatted_channel_id, formatted_video_id)).one()
    if not row:
        return jsonify({"error": "Video not found"}), 404
    video = {"id": remove_video_key(row.id), "title": row.title, "description": row.description, "duration": row.duration}
    return jsonify(video), 200

#-------------------------------------------------------------------------------------------------------
@app.route('/channels/<channel_id>/videos/<video_id>', methods=['DELETE'])
def delete_video(channel_id, video_id):
    formatted_channel_id = channel_key(channel_id)
    formatted_video_id = video_key(video_id)

    query_check = SimpleStatement("SELECT id FROM cassandra_duombaze.videos WHERE channel_id = %s AND id = %s")
    result = session.execute(query_check, (formatted_channel_id, formatted_video_id)).one() 
    if not result:
        return jsonify({"error": "Video not found"}), 404
    delete_query = SimpleStatement("DELETE FROM cassandra_duombaze.videos WHERE channel_id = %s AND id = %s")
    session.execute(delete_query, (formatted_channel_id, formatted_video_id))
    return jsonify({"message": "Video deleted successfully"}), 204
#-------------------------------------------------------------------------------------------------------
@app.route('/channels/<channel_id>/videos/<video_id>/views', methods=['GET'])
def get_video_views(channel_id, video_id):
    formatted_channel_id = channel_key(channel_id)
    formatted_video_id = video_key(video_id)
    query = SimpleStatement("SELECT views FROM video_views WHERE channel_id = %s AND video_id = %s")
    row = session.execute(query, (formatted_channel_id, formatted_video_id)).one()
    views = row.views if row else 0
    return jsonify({"views": views}), 200

#-------------------------------------------------------------------------------------------------------
@app.route('/channels/<channel_id>/videos/<video_id>/views/register', methods=['POST'])
def register_view(channel_id, video_id):
    formatted_channel_id = channel_key(channel_id)
    formatted_video_id = video_key(video_id)

    query_check = "SELECT views FROM video_views WHERE channel_id = %s AND video_id = %s"
    row = session.execute(query_check, (formatted_channel_id, formatted_video_id)).one()

    if row is None:
        query_insert = """
            UPDATE video_views
            SET views = views + 1
            WHERE channel_id = %s AND video_id = %s
        """
        session.execute(query_insert, (formatted_channel_id, formatted_video_id))
        return jsonify({"message": "View registered"}), 204

    query_update = """
        UPDATE video_views
        SET views = views + 1
        WHERE channel_id = %s AND video_id = %s
    """
    session.execute(query_update, (formatted_channel_id, formatted_video_id))
    return jsonify({"message": "View registered"}), 204


#-------------------------------------------------------------------------------------------------------
@app.errorhandler(404)
def not_found(error):
    return jsonify({"error": "Resource not found"}), 404

if __name__ == '__main__':
    app.run(debug=True)
