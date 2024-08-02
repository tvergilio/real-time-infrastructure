from flask import Flask, render_template
from flask_socketio import SocketIO

app = Flask(__name__)
socketio = SocketIO(app)

# Hardcoded Stanford message
stanford_message = {
    'start': '02/08/2024 09:28:00',
    'end': '02/08/2024 09:30:00',
    'overallSentiment': 'Positive',
    'mostPositiveMessage': 'He knows lots of tricks, and is handsome as well. Have you seen his blue feathers?',
    'mostNegativeMessage': 'He knows lots of tricks, and is handsome as well. Have you seen his blue feathers?',
    'messageCount': 1,
    'averageScore': 2.5
}

# Hardcoded GPT-4 message
gpt4_message = {
    'start': '02/08/2024 09:28:00',
    'end': '02/08/2024 09:30:00',
    'overallSentiment': 'positive',
    'mostPositiveMessage': 'He knows lots of tricks, and is handsome as well. Have you seen his blue feathers?',
    'mostNegativeMessage': '',
    'messageCount': 1,
    'descriptiveParagraph': "The overall sentiment of the message is positive, highlighted by an admiration for someone's abilities and appearance. The tone is enthusiastic and appreciative, indicating a favorable view of the subject, particularly in relation to their charisma and distinctiveness."
}

@app.route('/')
def index():
    return render_template('index.html')

@socketio.on('connect')
def handle_connect():
    socketio.emit('new_message', {'type': 'stanford', 'data': stanford_message})
    socketio.emit('new_message', {'type': 'gpt', 'data': gpt4_message})

if __name__ == '__main__':
    socketio.run(app, host='0.0.0.0', port=5001)
