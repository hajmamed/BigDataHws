from confluent_kafka.admin import AdminClient, NewTopic
from kafka.errors import UnknownTopicOrPartitionError

admin_client = AdminClient({
    "bootstrap.servers": "localhost:9092"
})

topic_name = "taxi_topic"

check_list = admin_client.list_topics()
for topic in check_list.topics:
    print(topic)
    if topic == topic_name :
        try:
            admin_client.delete_topics(topics=topic_name)
            print("Topic Deleted Successfully")
        except UnknownTopicOrPartitionError as e:
            print("Topic Doesn't Exist")
        except  Exception as e:
            print(e)

topic_list = []
topic_list.append(NewTopic(topic_name, 1, 1))
if not topic_name in check_list.topics:
    admin_client.create_topics(topic_list)

print("Topic Creation Finished")
# produce_taxi_topic()

