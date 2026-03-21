import logging
import os

# SETUP LOGGING
LOG_PATH = os.path.join("logs", "pipeline.log")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler()
    ]
)

def main():
    print("Hello World!!")


main()