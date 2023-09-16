from confluent_kafka.avro import AvroConsumer

def read_messages():
    consumer_config= {
        "bootstrap.servers" : "localhost:9092",
        "schema.registry.url" : "http://localhost:8081",
        "group.id" : "digitalskola.consumer_Project6", 
        "auto.offset.reset": "earliest"
    }
    consumer = AvroConsumer(consumer_config)
    consumer.subscribe(["digital.taxi_rides"]) #consumer can subscribe more than one topic by listing each topic

    while(True):
        try:
            message = consumer.poll(5)
        except Exception as e:
            print(f"Exception while trying to poll message - {e}")
        else:
            if message:
                print(f'Successfully poll a record from'
                    f'Kafka Topic : {message.topic()}, partition: {message.partition()}, Offset: {message.ffset()}\n'
                    f'message key: {message.key()} || message value: {message.value()}'
                    )
                consumer.commit()
            else:
                print(f'No message at this point. Please try again later.')

    consumer.close()


if __name__ == "__main__":
    read_messages()