import time
import subprocess
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

class FileChangeHandler(FileSystemEventHandler):
    def on_created(self, event):
        print(f"File created: {event.src_path}")
        self.run_script()

    def on_deleted(self, event):
        print(f"File deleted: {event.src_path}")
        self.run_script()

    def run_script(self):
        subprocess.run(['python', 'script2.py'])

if __name__ == "__main__":
    paths_to_watch = ["payments", "bets"]
    event_handler = FileChangeHandler()
    observer = Observer()

    for path in paths_to_watch:
        observer.schedule(event_handler, path, recursive=False)

    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
