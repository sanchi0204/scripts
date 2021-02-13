import firebase_admin
import csv
import google.cloud
from firebase_admin import credentials, firestore

cred = credentials.Certificate("C:\\Users\\hp\\Desktop\\demuxScript\\venv\\ServiceAccountKey.json")  #admin file path
app = firebase_admin.initialize_app(cred)

db = firestore.client() # this connects to  Firestore database
docs_ques = db.collection("Questions").stream()    #get all data from collection
docs_companies = db.collection("Companies").stream()

data_companies = []
res = []

def save(document_id, data):
    db.collection("Questions").document(document_id).set(data,merge=True)   #merge=True ensures overwriting on existing data

def fun(cmpName, result):
      res = list(filter(lambda c: c['company_name'] == cmpName, data_companies))
      result.append(res)

for item in docs_companies:
    data_companies.append(item.to_dict())  #add json data to lists

for doc in docs_ques:
    data = doc.to_dict()
    myList = list(data['companies'])
    size = len(myList)
    if(size==0):
        print("No Company")
    else:
        result=[]
        if(size>3):
            for x in range(3):
                cmpName = myList[x]
                fun(cmpName, result)
        else:
            for x in range(size):
                cmpName = myList[x]
                fun(cmpName, result)
        #print(result)
        flattened  = [val for sublist in result for val in sublist]   #convert list of lists to list
        print(flattened)

        ans = {'CompanyImg' : flattened}  #make an object
        save(doc.id, ans)                  #save to db
        print("updated")

print("Done")
