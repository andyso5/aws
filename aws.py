import os
import boto3
import traceback
from datetime import datetime, timedelta
SUB_DIR = "perfect_diary"
BUCKET_DIR = "batch-order-bakeup"
tz_delta = timedelta(hours=8)

class AWS_S3(object):
    def __init__(self, bucket=BUCKET_DIR, sub_dir=SUB_DIR):
        self._bucket = bucket
        self._sub_dir = sub_dir
        self._fmt = "%Y-%m-%d  %H:%M:%S"
        self._latest_modify = None

    def _download_file(self, file_name, save_dir, file_path=None, abs_path=None, refresh=False):
        try:
            s3 = boto3.resource("s3")
            
            if abs_path:
                file_path = abs_path
            else:
                file_path = os.path.join(self._sub_dir, file_name)
            
            download_path = os.path.join(save_dir, file_name)

            if os.path.exists(download_path):
                print("\tfile: %s exists" % file_path)
                if not refresh:
                    print("\tgive up refreshing")
                    return True
                else:
                    print("\tready to refresh...")

            dir_name = os.path.dirname(download_path)
            if not os.path.exists(dir_name):
                os.mkdir(dir_name)

            s3.Bucket(self._bucket).download_file(file_path, download_path)
            if os.path.exists(download_path):
                return True
            else:
                print("no error, but don't find the download file: %s" %file_name)
                return False
        except Exception:
            traceback.print_exc()
            print("file_name: %s" %file_name)
            return False
    
    def list_dir(self, StartAfter=""):
        res = []
        s3 = boto3.client("s3")
        # MaxKeys: file num, defualt is 1000
        if StartAfter:
            StartAfter = os.path.join(self._sub_dir, StartAfter)
        while True:
            response = s3.list_objects_v2(Bucket=self._bucket, Prefix=self._sub_dir, StartAfter=StartAfter)
            content = response.get("Contents", [])
            for unit in content:
                res.append(unit["Key"])

            self._latest_modify = unit["LastModified"] + tz_delta
            if response["KeyCount"] < response["MaxKeys"]:
                break
        
        return res
    
    def download_files(self, save_dir, StartAfter="", refresh=False):
        success_count = 0
        failed_count = 0
        print("\n####start downloading files from %s" % StartAfter)
        res = self.list_dir(StartAfter)
        for abs_file in res:
            print("downloading file: %s ..." % abs_file)
            res = self._download_file(abs_file, save_dir, abs_path=abs_file, refresh=refresh)
            if res:
                success_count += 1
                print("\tfinished\n")
            else:
                failed_count += 1
                print("\t#failed\n")
        
        print("\n####download task end\n")
        print("***sum***")
        print("\tfinish num: %d" % success_count)
        print("\tfailed num: %d" % failed_count)
        print("\tsub_dir: %s was latest modified in %s" %(self._sub_dir, self._latest_modify.strftime(self._fmt)))

if __name__ == "__main__":
    from pprint import pprint
    aws_s3 = AWS_S3()
    # res = aws_s3.list_dir()
    # pprint(res)
    save_dir = os.path.dirname(os.path.abspath(__file__))
    aws_s3.download_files(save_dir)

    

