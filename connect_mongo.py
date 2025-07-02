from pymongo import MongoClient

def insert_data_mongo(conn_str, db_name, collection_name, data):

    # Connect to MongoDB
    client = MongoClient(conn_str)
    db = client[db_name]
    collection = db[collection_name]

    # Print list of databases in the current cluster
    for db_name in client.list_database_names():
        print(db_name)

    # Insert documents
    if isinstance(data, dict):
        result = collection.insert_one(data)
        print("Inserted 1 document.")

        # Fetch and print inserted document
        inserted_docs = collection.find_one({"_id": result.inserted_id})
        print(inserted_docs)
        return inserted_docs
        
    elif isinstance(data, list):
        result = collection.insert_many(data)
        print(f"Inserted {len(result.inserted_ids)} documents.")
            
        # Fetch and print inserted documents
        inserted_docs = list(collection.find({"_id": {"$in": result.inserted_ids}}))
        for doc in inserted_docs:
            print(doc)
        return inserted_docs
    
    else:
        print ("Data must be a dictionary or a list of dictionaries.")

doc = {
    "name": "Sonam",
    "age": 22,
    "email": "sonam@example.com"
}

# insert_data_mongo("mongodb://localhost:27017/", "student_db", "students", doc)

docs = [
    {"name": "Deepak", "age": 25, "email": "deepak@example.com", "ID_Cards": {"has_Adhar_card": True, "has_Pan_card": False }},
    {"name": "Sourav", "age": 28, "email": "sourav@example.com", "ID_Cards": {"has_Adhar_card": True, "has_Pan_card": True }}
]

# insert_data_mongo("mongodb://localhost:27017/", "student_db", "students", docs)


