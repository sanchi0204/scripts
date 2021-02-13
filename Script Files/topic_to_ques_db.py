import firebase_admin
import csv
import google.cloud
from firebase_admin import credentials, firestore

cred = credentials.Certificate("C:\\Users\\hp\\Desktop\\demuxScript\\venv\\ServiceAccountKey.json") #admin key path
app = firebase_admin.initialize_app(cred)

db = firestore.client() # this connects to Firestore database


docs_ques = db.collection("Questions").stream()    #get all data from collection
docs_topics = db.collection("Topics").stream()

data_topics = []


def save(document_id, data):
    db.collection("Questions").document(document_id).set(data,merge=True)  #merge=True ensures overwriting on existing data

def addKey(document_id, data):
    db.collection("Questions").document(document_id).set(data,merge=True)

for item in docs_topics:
    data_topics.append(item.to_dict())  #add json data to lists


for doc in docs_ques:
    data = doc.to_dict()
    myList = list(data['topicTag'])
    size = len(myList)
    if(size==0):
        print("No topic")
    else:
        topicName = myList[0]
        res = list(filter(lambda topic: topic['name'] == topicName, data_topics))
        #print(res[0])
        result = {'topicImg' : res[0]}
        save(doc.id, result)
        print('Updated')

print("Done")
