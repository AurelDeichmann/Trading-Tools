import logging
import logging.config
from bot_start import Bot


def main():
    logger = setup_custom_logger()
    bot = Bot()
    bot.run()
    
    
def setup_custom_logger():
    formatter = logging.Formatter(fmt='%(asctime)s - %(levelname)s - %(module)s - %(message)s')

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    fileHandler = logging.FileHandler("deribit.log", mode="w")
    fileHandler.setFormatter(formatter)
    
    logger = logging.getLogger("deribit")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        logger.addHandler(handler)
        logger.addHandler(fileHandler)
    return logger


if __name__ == "__main__":
    main()