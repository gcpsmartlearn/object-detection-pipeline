import time, requests
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

PATH = "/Users/akshayj/web-scraping/object-detection-pipeline/data"

class Watcher:
    DIRECTORY_TO_WATCH = str(PATH)

    def __init__(self):
        self.observer = Observer()

    def run(self):
        event_handler = Handler()
        self.observer.schedule(event_handler, self.DIRECTORY_TO_WATCH, recursive=True)
        self.observer.start()
        try:
            while True:
                time.sleep(5)
        except Exception as er:
            self.observer.stop()
            print (er)

        self.observer.join()


class Handler(FileSystemEventHandler):
    
    @staticmethod
    def on_any_event(event):
        # past_file = ''
        if event.is_directory:
            return None

        elif event.event_type == 'created' or event.event_type == 'modified':
            # Take any action here when a file is first created or existing modified.
            # print('**************** starting data injestion ************************')
            try:
                filename = event.src_path
                extention = str(filename).split(sep=".")
                url = "https://storage.googleapis.com/upload/storage/v1/b/odm-sml-bucket/o?uploadType=media&name="+str(datetime.now())+"." + str(extention[-1])

                payload = str(filename)
                headers = {
                'Content-Type': 'image/png ; image/png ; image/tiff',
                'Authorization': 'Bearer ya29.a0AfH6SMBwrVuACyMosGxOQbTIm2Jg4MuPJilo8NmxwHJDcNP3m3xjazJFkdjGwLY2wmkYpcgN_KxAYDv_al5zLrPgH0TOrFIg_QuhXHucryjofG03W7YQntq6QO70BLGts-2jesu-LP6g_apuqKsTsyKsUpOW'
                }

                response = requests.request("POST", url, headers=headers, data=payload)

                print(response.text)
            except Exception as er:
                print(er)
                pass
            

if __name__ == '__main__':
    w = Watcher()
    w.run()        
