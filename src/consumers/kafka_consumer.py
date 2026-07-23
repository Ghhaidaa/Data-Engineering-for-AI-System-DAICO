from kafka import KafkaConsumer
import json
import pandas as pd


def main():
    # Read the validated dataset only to know how many records are expected
    valid_books = pd.read_csv("/content/valid_books.csv")
    expected_count = len(valid_books)

    # Create Kafka Consumer
    consumer = KafkaConsumer(
        "books",
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        consumer_timeout_ms=10000,
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )

    print("Reading records from Kafka...\n")

    count = 0

    for message in consumer:
        print(message.value)
        count += 1

        if count == expected_count:
            break

    consumer.close()

    print(f"\nSuccessfully consumed {count} / {expected_count} records.")

    if count < expected_count:
        print("Warning: Consumer timed out before reading all records.")


if __name__ == "__main__":
    main()
  Add Kafka consumer
