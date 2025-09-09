import sys
import asyncio
import json
import threading
import time
import logging
import functools
import inspect
from queue import Queue, Full

from digi.xbee.devices import XBeeDevice, XBeeMessage, RemoteXBeeDevice, XBee64BitAddress
from digi.xbee.exception import XBeeException, TransmitException, TimeoutException, InvalidOperatingModeException

logging.basicConfig(level=logging.INFO, format='[%(asctime)s - %(levelname)s]:\n\t%(message)s')

def check_connected(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        if not self.device.is_open():
            logging.error("XBee is not opened.")
            return None
        return func(self, *args, **kwargs)
    return wrapper

class XBeeService:
    def __init__(self, message_received_callback, port="/dev/ttyUSB0", baudrate=57600, max_queue_size=100000):
        self.port = port
        self.baudrate = baudrate
        self.device = XBeeDevice(port, baudrate)
        self.device.open()
        self.address = int.from_bytes(self.device.get_64bit_addr().address, "big")
        self.network = self.device.get_network()
        self.subscribers = []
        if message_received_callback:
            self.subscribers.append(message_received_callback)
        self.recent_messages = Queue(maxsize=max_queue_size)
        self.queue_thread = asyncio.create_task(self.queue_processor())
        if self.subscribers:
            logging.warning("Message queue proccessing thread is opened.")
        else:
            logging.warning("No callback is specified.")

    
    async def queue_processor(self):
        """
        Function that proccesses the queue.
        """
        while not self.queue_thread.done():
            if self.recent_messages.empty():
                await asyncio.sleep(0.1)
                continue
            message = self.recent_messages.get(timeout=0.5)
            logging.debug(f"Mesaj işleniyor: {message}")
            for callback in list(self.subscribers):
                try:
                    is_async = inspect.iscoroutinefunction(callback)
                    if is_async:
                        await callback(message)
                    else:
                        callback(message)
                except Exception as e:
                    logging.exception(f"An error occured while proccessing the callback function of a subscriber: {e}")
            logging.debug("Callback called.")
            self.recent_messages.task_done()

    def subscribe(self, callback):
        """
        Subscribes a callback function to incoming XBee messages
        """
        self.subscribers.append(callback)
        if self.queue_thread.done():
            self.queue_thread = asyncio.create_task(self.queue_processor())
            logging.info("Mesaj kuyruğu işleme thread'i yeniden başlatıldı.")

    def unsubscribe(self, callback):
        """
        Unsubscribes the given subscriber callback.
        """
        self.subscribers.remove(callback)

    def default_message_received_callback(self, message: XBeeMessage):
        """
        Default callback function of the service, for formatting the data in a specific way.
        """
        try:
            message_data = message.data.decode('utf-8')
            sender = int.from_bytes(message.remote_device.get_64bit_addr().address, "big")
            message_full = {
                "sender": sender,
                "isBroadcast": message.is_broadcast,
                "data": message_data,
                "timestamp": message.timestamp
            }
            logging.debug(f"Message received: {message_full}")
            try:
                self.recent_messages.put_nowait(message_full)
                logging.debug("Message added to queue")
            except Full:
                # logging.error(f"Unable to add the message to the queue, queue is full.")
                # Kuyruk doluysa en eski mesajı sil ve yeni mesajı ekle
                logging.debug("Queue is full. Deleting the oldest message and adding new message to the queue.")
                self.recent_messages.get_nowait()
                self.recent_messages.put_nowait(message_full)
        except Exception as e:
            logging.error(f"An error occured while proccessing the message: {e}")

    def listen(self):
        """
        Opens the device and initiates the default callback to listen to the messages. 
        """
        try:
            if not self.device.is_open():
                self.device.open()
                self.address = int.from_bytes(self.device.get_64bit_addr().address, "big")
            self.device.add_data_received_callback(self.default_message_received_callback)
            logging.info("XBee dinleniyor...")
        except Exception as e:
            logging.error(f"XBee açılamadı: {e}")
            raise
    
    @check_connected
    def send_broadcast_message(self, data):
        """
        Xbee üzerinden veri yayınlar (broadcast eder).
        """
        try:
            message = data
            logging.debug(f"Broadcast mesajı yapılandırıldı: {message}")
            self.device.send_data_broadcast(message)
            logging.debug(f"Mesaj gönderildi:\n Mesaj: {data}\nAlıcı: Broadcast")
            return True
        except XBeeException as e:
            logging.error(f"XBee Hatası: {e}")
            return False
        except TimeoutException as e:
            logging.error(f"Zaman aşımı hatası: {e}")
            return False
        except TransmitException as e:
            logging.error(f"Transmit hatası: {e}")
            return False
        except InvalidOperatingModeException as e:
            logging.error(f"Geçersiz çalışma modu hatası: {e}")
            return False
    
    @check_connected
    def send_private_message(self, receiver: int, data):
        """
        Xbee üzerinden bir alıcıya veri gönderir.
        """
        try:
            receiver = receiver.to_bytes(8, "big")
            address = XBee64BitAddress(receiver)
            device = self.network.get_device_by_64(address)
            self.device.send_data(device, data)
            logging.debug(f"Sent Message:\n Contents: {data}\nReceiver: {receiver}")
            return True
        except Exception as e:
            logging.exception(f"An error occured while sending a message: {e}")
            return False
    
    def close(self):
        """
        XBee cihazını kapatır ve mesaj kuyruğu işleme thread'ini durdurur.
        """
        if self.device.is_open():
            self.device.close()
            logging.info("XBee is closed.")
            self.queue_thread.cancel()
            logging.info("Stopped message proccessing thread.")
        else:
            logging.warning("XBee is already closed.")
            
def main():
    """
    Function to start and test the service.
    """
    xbee = XBeeService(message_received_callback=XBeeService.default_message_received_callback)
    xbee.listen()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        xbee.close()

if __name__ == "__main__":
    logging.info("XBee service is starting...")
    main()
    logging.info("Closing the service...")
    sys.exit(0)