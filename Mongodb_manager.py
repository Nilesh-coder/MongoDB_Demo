from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, PyMongoError
from dotenv import load_dotenv
from typing import Union, List, Dict, Any, Tuple
import os
import logging
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class MongoDBManager:
    """
    A robust class to manage MongoDB operations.
    The MongoClient instance is managed as a class-level singleton
    to ensure it's initialized once and reused across the application.
    """
    _client = None

    @classmethod
    def _validate_connection_string(cls, connection_string):
        """Validate connection string data type and format."""
        if not isinstance(connection_string, str):
            raise TypeError(f"Connection string must be a string, got {type(connection_string).__name__}")
        
        if not connection_string.strip():
            raise ValueError("Connection string cannot be empty or whitespace")
        
        # Basic MongoDB URI format validation
        if not (connection_string.startswith("mongodb://") or connection_string.startswith("mongodb+srv://")):
            raise ValueError("Connection string must start with 'mongodb://' or 'mongodb+srv://'")

    @classmethod
    def initialize_client(cls, connection_string: str = None):
        """
        Initializes the *singleton* MongoClient instance.
        This method should be called once at application startup.
        """
        if cls._client is not None:
            logging.info("MongoDB client already initialized.")
            return
        
        load_dotenv() # This line loads variables from .env into os.environ
        conn_str = connection_string or os.getenv("MONGO_DB_CONNECTION_STRING")

        # Validate connection string
        cls._validate_connection_string(conn_str)
        
        try:
            cls._client = MongoClient(conn_str)
            cls._client.admin.command('ping')  # Test connection
            logging.info("MongoDB client initialized successfully.")
        except (ConnectionFailure, PyMongoError) as e:
            logging.error(f"FATAL: MongoDB connection failed during initialization: {e}")
            cls._client = None
            raise
    
    @classmethod
    def _validate_name(cls, name, name_type):
        """Validate database or collection name."""
        if not isinstance(name, str):
            raise TypeError(f"{name_type} name must be a string, got {type(name).__name__}")
        
        if not name.strip():
            raise ValueError(f"{name_type} name cannot be empty or whitespace")
        
        if len(name) > 64:
            raise ValueError(f"{name_type} name cannot exceed 64 characters") 

        invalid_chars = ['~', '\\', '.', ' ', '"', "'",'$','#','%','+','()','*']
        for char in invalid_chars:
            if char in name:
                raise ValueError(f"{name_type} name cannot contain '{char}'")
           

    @classmethod
    def close_client(cls):
        """Close the singleton client."""
        if cls._client:
            cls._client.close()
            cls._client = None
            logging.info("MongoDB client closed.")

    def __init__(self, db_name, collection_name):
        """
        Initializes an instance of MongoDBManager for a specific database and collection.
        Relies on the class-level _client being already initialized.
        """
        if self._client is None:
            raise RuntimeError("MongoDB client not initialized. Call initialize_client() first.")
        
        # Validate inputs
        self._validate_name(db_name, "Database")
        self._validate_name(collection_name, "Collection")
        self.db_name = db_name
        self.collection_name = collection_name
        
        # Cache database and collection references
        self.db = self._client[db_name]              # Get database object once 
        self.collection = self.db[collection_name]    # Get collection object once
        # self.collection = self._client[db_name][collection_name]
        logging.info(f"MongoDBManager instance created DB: '{self.db_name}', Collection: '{self.collection_name}'")

    # Add your CRUD operations here
    def insert_document(self, data: Union[Dict, List[Dict]]) -> Tuple[bool, Any]:
        try:
        
            # Handle both single document and multiple documents
            if isinstance(data, dict):
                # Single document insertion
                result = self.collection.insert_one(data)
                
                print("=== SINGLE DOCUMENT INSERTED ===")
                print(f"Database: {self.db_name}")
                print(f"Collection: {self.collection_name}")
                print(f"Created Data:")
                print(json.dumps(data, indent=2, default=str))
                return result.acknowledged, result.inserted_id
            
            elif isinstance(data, list):
                # Multiple documents insertion
                result = self.collection.insert_many(data)

                print ("=== MULTIPLE DOCUMENTS INSERTION ===")
                print(f"Database: {self.db_name}")
                print(f"Collection: {self.collection_name}")
                print(f"Created Data:")
                print(json.dumps(data, indent=2, default=str))
                return result.acknowledged, result.inserted_ids
            
            else:
                print("Error: Data must be a dictionary or list of dictionaries")
                return False, None
            
        except Exception as e:
            logging.error(f"Error inserting data: {str(e)}")
            print(f"Error inserting data: {str(e)}")
            return False, None

    def find_one(self, filter_dict):
        return self.collection.find_one(filter_dict)
    
    def find_many(self, filter_dict=None):
        return self.collection.find(filter_dict or {})
    
    def update_one(self, filter_dict, update_dict):
        return self.collection.update_one(filter_dict, update_dict)
    
    def delete_one(self, filter_dict):
        return self.collection.delete_one(filter_dict)


# Usage example
if __name__ == "__main__":
    """
    Main function to demonstrate MongoDB operations using the singleton MongoClient.
    """
    logging.info("Starting application...")
    try:
        # Initialize once at app startup
        MongoDBManager.initialize_client()
         
        # Create managers for different collections
        # Both managers use the same underlying client
        
        user_manager = MongoDBManager("test_db", "test_collection")
        # product_manager = MongoDBManager("myapp", "products")
        
        # Use the managers
        # Single document insertion
        success, inserted_id = user_manager.insert_document({"name": "Jack", "email": "jack@example.com"})
        print(f"Success={success}, ID={inserted_id}")

        # Multiple Document insertion
        success, inserted_id = user_manager.insert_document([{"name": "Frank", "email": "frank@example.com"},{"name": "Daniel", "email": "daniel@example.com"}])
        print(f"Success={success}, ID={inserted_id}")
 
        # Test validation (these will raise errors)
        # MongoDBManager("", "users")  # Empty db name
        # MongoDBManager("my app", "users")  # Space in db name
        # MongoDBManager(123, " ")  # Wrong type
        
    except (TypeError, ValueError) as e:
        logging.error(f"Validation error: {e}")
    except Exception as e:
        logging.error(f"Application error: {e}")
    finally:
        MongoDBManager.close_client()