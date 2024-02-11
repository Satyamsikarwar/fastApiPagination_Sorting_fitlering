from fastapi import FastAPI, Query, HTTPException
from bson import ObjectId  # Importing ObjectId from bson
import math
import random  # Importing the random module
app = FastAPI()

# connect to database
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

uri = "xyz"
# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))
db = client['usersDb']
if 'userColl' not in db.list_collection_names():
    collection = db.create_collection('userColl')
else:
    collection = db['userColl']
# Send a ping to confirm a successful connection

try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)

@app.get('/')
def root():
    return {'Welcome':"Hello World"}

# it's not a good practice to send all the data at once from server
# to avoid that we use server side pagination 
# in server side pagination we need to parameter from front end side first pageSize and second pagenumber
@app.get('/users')
async def get_users(pageSize: int = Query(None, description="Number of items per page"),
                    currentPage: int = Query(None, description="Current page"), 
                    sort_column: str = Query(None, description="Column to sort by"),
                    sort_order: str = Query(None, description="Sorting order (asc/desc)"),
                    filter_column: str = Query(None, description="Column to filter by"),
                    filter_value: str = Query(None, description="Value to filter by")):
    # Validate pagination parameters
    if pageSize is not None and pageSize <= 0:
        raise HTTPException(status_code=400, detail="pageSize must be greater than 0")
    if currentPage is not None and currentPage <= 0:
        raise HTTPException(status_code=400, detail="currentPage must be greater than 0")
    
    # Set default values for pagination if not provided
    if pageSize is None:
        pageSize = 10  # Default page size
    if currentPage is None:
        currentPage = 1  # Default current page
    
    # Calculate total number of documents in the collection
    total_documents = collection.count_documents({})

    # Calculate total number of pages
    total_pages = math.ceil(total_documents / pageSize)

    # Adjust currentPage if it exceeds the total number of pages
    if currentPage > total_pages:
        currentPage = total_pages

    # Handle edge case: Empty collection
    if total_documents == 0:
        return {'users': [], 'total_pages': 0}

    # Calculate skip and limit values based on pagination parameters
    skip = (currentPage - 1) * pageSize
    limit = pageSize
    
    # Define sort_criteria list
    sort_criteria = []
    if sort_column and sort_order:  # Check if both parameters are not None
        if sort_order.lower() == "asc":
            sort_criteria.append((sort_column, 1))
        elif sort_order.lower() == "desc":
            sort_criteria.append((sort_column, -1))
        else:
            raise HTTPException(status_code=400, detail="Invalid sort_order. Use 'asc' or 'desc'.")
    else:
        # Default sort order if not provided
        sort_criteria.append(('_id', 1))  # Default sort by _id ascending

    # Define filter criteria
    filter_criteria = {}
    if filter_column and filter_value:  # Check if both parameters are not None
        if filter_column=='age':
            filter_criteria[filter_column] = int(filter_value)
        else:
            filter_criteria[filter_column] = filter_value
    
    # Retrieve paginated documents from the collection
    users = list(collection.find(filter_criteria).skip(skip).limit(limit).sort(sort_criteria))

    # Convert ObjectId instances to strings
    for user in users:
        user['_id'] = str(user['_id'])

    # Return paginated users along with total number of pages
    return {'users': users, 'total_pages': total_pages}
