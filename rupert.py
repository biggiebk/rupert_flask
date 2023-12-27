import sys
import time
import json
from markupsafe import escape
from flask import Flask
from beartype import beartype
from confluent_kafka import Producer, Consumer

class Synapse():
	"""
		Description: Parent class used by synapses.
		Responsible for:
			1. Basic constructor for starting synapses.
			2. Initiates Kafka cosumer
			3. Contains method to push to Kafka as producer
	"""
	@beartype
	def __init__(self, settings_file: str) -> None:
		"""
			Construct for synapse classes.
			Responsible for:
				1. Loading settings.
				2. Initate topic consumer
			Requires:
				settings_file - Path to the synapse setting file
		"""
		self.settings_file = settings_file
		self.settings = {}
		self.__load_settings()
		self.settings['settings_file'] = settings_file
		self.consumer_cfg = self.settings['kafka']['connection'] | self.settings['kafka']['consumer']
		self.consumer = None
		self.close_consumer = False

	@beartype
	def listen(self, topic: str) -> None:
		"""
			Description: Connects to Kafka and consumes a topic
			Responsible for:
				1. Connecting to Kafka and listening for events/messages
				2. Calls the process_event method
			Requires:
				Nothing
			Raises:
				RuntimeError if called on a closed consumer
		"""
		self.consumer = Consumer(self.consumer_cfg)
		self.consumer.subscribe([self.settings['kafka']['topics'][topic]])
		while True:
			if self.close_consumer:
				self.consumer.close()
				sys.exit()
			msg = self.consumer.poll(1.0)
			if msg is None:
				pass
			elif msg.error():
				print(msg.error())
			else:
				self.process_event(msg)

	@beartype
	def process_event(self, consumer_message) -> None:
		"""
			Description: Each synapse should overide this method. The code here mostly
				is to support testing connectivity with Kafka
			Responsible for:
				1. Converts the messages value to string
				2. Returns the string
				3. Quits the class
			Requires:
				consuer_message
		"""
		print(consumer_message.value().decode('utf-8'))
		time.sleep(10)
		self.stop()

	@beartype
	def reload(self):
		"""
			Reloads the synapse
				1. Reloads the configuration
		"""
		self.__load_settings()

	@beartype
#	def send(self, topic, event_bytes: bytes) -> None:
	def send(self, topic, event_bytes) -> None:
		"""
			Description: Sends a byte array to Kafka as a producer
			Responsible for:
				1. Send event bytes to topic
			Requires:
				1. topic - Name of the topic to send message/event to (string)
				2. event_bytes - array of bytes
			Raises:
				BufferError - if the internal producer message queue is full 
				KafkaException - for other errors, see exception code
				NotImplementedError - if timestamp is specified without underlying library support.
		"""
		producer = Producer(self.settings['kafka']['connection'])
		producer.produce(self.settings['kafka']['topics'][topic], event_bytes)
		producer.poll(10000)
		producer.flush()

	@beartype
	def stop(self) -> None:
		"""Closes the the consumer and exits"""
		self.close_consumer = True

	## Private methods, best not to overide anything beyond this point

	@beartype
	def __load_settings(self) -> None:
		"""
			Description: Parent class used by other synapes.
			Responsible for:
				1. Loading settings
			Requires:
				settings_file - Path to the synapses setting file
		"""
		with open(self.settings_file, 'r', encoding='utf-8') as settings:
			settings_json = settings.read()
		self.settings = json.loads(settings_json)


def load_json(json_file):
	with open(json_file, 'r', encoding='utf-8') as contents:
		contents_json = contents.read()
	return json.loads(contents_json)

app = Flask(__name__)

@app.route("/event/<event_type>/<name>/<action_profile>")
def event(event_type, name, action_profile):
	kafka_producer = Synapse('/web/cfg/settings.json')
	action_profile = load_json(f"/web/events/{escape(event_type)}/{escape(name)}/{escape(action_profile)}.json")
	for action in action_profile['actions']:
		kafka_producer.send(action['topic'],json.dumps(action['action']))
	return f"Running action of Type: {escape(event_type)} Name: {escape(name)} Profile: {escape(action_profile)}"
