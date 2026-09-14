from app.redis_client import redis_client

response = redis_client.ping()

print("Redis response:", response)