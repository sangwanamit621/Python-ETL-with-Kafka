import pymongo as pm

class Mongo():
    """
    Defined different functions related to mongodb which we will use to perform different type of operations. 
    """
    def __init__(self,username:str,password:str):
        """
        Pass username and password to connect to the mongoDB cluster. Error will be raised if incorrect username or password is passed
        Input:
        username: name of user
        password: password

        Error:
        Error will be raised if wrong username or password is passed
        """
        self.username = username
        self.password = password        
        self.client = pm.MongoClient("mongodb://{}:{}@0.0.0.0:27017".format(username,password))
        self.checkConnection()


    def checkConnection(self):
        # checking if username and password are correct by performing an operation
        try:
            if type(self.client.list_database_names())==list:
                print("Connection Established with MongoDB Cluster")
        except Exception as e:
            error_message = e.details.get('errmsg', 'Unknown error')
            if error_message == "Authentication failed.":
                raise  Exception(error_message+" Please provide correct username and password")
            else:
                raise Exception("Error occurred while connecting with MongoDB server: "+error_message)


    def getCollectionObject(self, db:str, collection:str):
        """
        Creates collection inside a database in a cluster
        """        
        try:
            self.db_obj = self.client.get_database(name=db)
            self.collection_obj = self.db_obj.get_collection(name=collection)
            print(f"Successfully Connected with Collection: {collection} of Database: {db} in mongoDB cluster")
        except Exception as e:
            error_message = e.details.get('errmsg', 'Unknown error')
            raise Exception("Error occurred while performing the operation: "+error_message)
        
    
    def addRecords(self, db, collection, records_list:list):
        """
        Takes list of records to add in the collection
        """
        self.db_obj = self.client.get_database(name=db)
        self.collection_obj = self.db_obj.get_collection(name=collection)
        
        if type(records_list)==dict:
            records_list = [records_list]
        output = self.collection_obj.insert_many(records_list)
        if output.acknowledged:
            ids = [str(id) for id in output.inserted_ids]
            print(f"Inserted {len(ids)} Records in {self.collection_obj.name}")
        else:
            print(f"Failed to Add records in the collection: {self.collection_obj.name}")


    def updateRecords(self, db, collection, condition:dict,updatedRecord:dict):
        """
        Takes condition and updates records which satisfies the condition and prints message to give count of updated records 
        """
        self.db_obj = self.client.get_database(name=db)
        self.collection_obj = self.db_obj.get_collection(name=collection)
        output = self.collection_obj.update_many(filter=condition,update=updatedRecord)
        
        if output.acknowledged:
            print(f"Updated {output.modified_count} Records based on given condition {condition}")
        else:
            print(f"Failed to update records in the collection: {self.collection_obj.name}")
        

    def fetchRecords(self, db, collection, condition:dict):
        """
        Takes condition as dict and returns records satisfying the condition in a list
        """
        self.db_obj = self.client.get_database(name=db)
        self.collection_obj = self.db_obj.get_collection(name=collection)
        return [item for item in self.collection_obj.find(condition)]
    
        
    def deleteRecords(self, db, collection, condition:dict):
        """
        Takes condition and deletes records which satisfies the condition and prints message to give count of deleted records 
        """
        self.db_obj = self.client.get_database(name=db)
        self.collection_obj = self.db_obj.get_collection(name=collection)
        output = self.collection_obj.delete_many(condition)
        if output.acknowledged:
            print(f"Deleted {output.deleted_count} Records based on given condition {condition}")
        else:
            print(f"Failed to delete records in the collection: {self.collection_obj.name}")

