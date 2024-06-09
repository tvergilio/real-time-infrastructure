import redis

# Connect to Redis
client = redis.StrictRedis(host='localhost', port=6379, decode_responses=True)

# Retrieve the sorted set data in descending order
relevance_data = client.zrevrange('restaurant_relevance', 0, -1, withscores=True)

# Print the data with scores beside the IDs
for restaurant_id, score in relevance_data:
    print(f"Restaurant ID: {restaurant_id}, Score: {score}")
