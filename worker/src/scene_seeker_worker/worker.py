import os
import time
import logging
import redis

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("worker")

REDIS_ADDR = os.getenv("REDIS_ADDR", "redis:6379")

def main():
    host, port = REDIS_ADDR.split(":")
    r = redis.Redis(host=host, port=int(port), db=0)

    logger.info("worker started, polling for jobs...")
    while True:
        job = r.lpop("scene_jobs")
        if job:
            logger.info(f"popped job: {job}")
            # placeholder: process job
            time.sleep(1)
            logger.info("job done")
        else:
            time.sleep(1)

if __name__ == '__main__':
    main()
