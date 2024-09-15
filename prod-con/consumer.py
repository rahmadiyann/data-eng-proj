from confluent_kafka import Consumer, KafkaException, KafkaError
import json
import time

def read_config():
    config = {}
    try:
        with open("client.properties") as fh:
            for line in fh:
                line = line.strip()
                if len(line) != 0 and line[0] != "#":
                    parameter, value = line.strip().split('=', 1)
                    config[parameter] = value.strip()
    except FileNotFoundError:
        print("Error: Configuration file 'client.properties' not found.")
    except Exception as e:
        print(f"Error reading configuration file: {e}")
    return config

def consume_from_kafka(config, topics):
    # Update configuration for consumer
    config.update({
        'group.id': 'my-consumer-group',
        'auto.offset.reset': 'earliest'
    })
    
    consumer = Consumer(config)

    def print_assignment(consumer, partitions):
        print('Assignment:', partitions)

    consumer.subscribe(topics, on_assign=print_assignment)
    topic_messages = {topic: False for topic in topics}

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                time.sleep(30)  # Sleep for 30 seconds if no message is received
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())
            data = json.loads(msg.value().decode('utf-8'))
            
            # Check if the message is a list
            if isinstance(data, list):
                for item in data:
                    print(f"Consumed event from topic {msg.topic()}: {json.dumps(item, indent=2)}")
            else:
                print(f"Consumed event from topic {msg.topic()}: {json.dumps(data, indent=2)}")
            
            topic_messages[msg.topic()] = True

            # Check if all topics have been consumed
            if all(topic_messages.values()):
                print("All topics have been successfully consumed.")
                # Reset topic_messages to continue consuming indefinitely
                topic_messages = {topic: False for topic in topics}
    except KeyboardInterrupt:
        print("Process interrupted by user.")
    finally:
        consumer.close()
        print("Consumer closed.")

def main():
    config = read_config()

    # Check if the configuration was loaded correctly
    if not config:
        print("Configuration not loaded. Exiting...")
        return

    # List of topics to consume from
    topics = ['artist_topic', 'album_topic', 'song_topic', 'item_topic']
    
    consume_from_kafka(config, topics)

if __name__ == '__main__':
    main()