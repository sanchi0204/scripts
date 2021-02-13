import firebase_admin
import csv
import google.cloud
from firebase_admin import credentials, firestore

cred = credentials.Certificate("C:\\Users\\hp\\Desktop\\demuxScript\\venv\\ServiceAccountKey.json")#path of admin file
app = firebase_admin.initialize_app(cred)

store = firestore.client()  # this connects to Firestore database

file_path = "C:\\Users\\hp\\Desktop\\demuxScript\\venv\\topics_csv_file.csv"  #csv file path
collection_name = "Topics"  #collection name

def batch_data(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]

#retrieve data from csv files and add to lists
data = []
headers = []
with open(file_path) as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    line_count = 0
    for row in csv_reader:
        if line_count == 0:
            for header in row:
                headers.append(header)
            line_count += 1
        else:
            obj = {}
            for idx, item in enumerate(row):
                obj[headers[idx]] = item
            data.append(obj)
            line_count += 1
    print(f'Processed {line_count} lines.')

for batched_data in batch_data(data, 300):   #add data as batches  (max 500 at a time)
    batch = store.batch()
    for data_item in batched_data:
        doc_ref = store.collection(collection_name).document()
        batch.set(doc_ref, data_item)
    batch.commit()

print('Done')
