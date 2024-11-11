GROUP 13 - ASSIGNMENT-3 - Record Manager - 11/1/2024 


## Description
---------------------------------------------------------------

The Record Manager handles table operations, including schema management, record insertion, retrieval, and deletion. 
It relies on the Storage Manager for low-level file operations and the Buffer Manager for efficient page caching and
memory management, enabling fast access to records.


## How to Run with Valgrind
---------------------------------------------------------------

type "make" valgrind_assign3


## How to Run without Valgrind
---------------------------------------------------------------

type "make"
type./test_assign3_1


1. Clone the BitBucket.
2. Open the terminal.
3. Navigate to "assign3_record_manager".
4. For executing we will execute the MakeFile, type "make" and press enter.



## Solution Description
----------------------------------------------------------------

## Implemented by Manasvi Lambore


    --------------------TABLE AND RECORD MANAGER FUNCTIONS---------------------

    # initRecordManager() 

    	-> This method is used to initialise a record manager.

	-> It sets the action to indicate that the record manager has been initialized.

	-> Its records the time of initialization.


    # shutdownRecordManager()

    	-> This method is used to shutdown a record manager.

	-> It attempts to shut down the buffer pool. If it fails, update the state to SHUTDOWN_RECORD_FAILED.

	-> It updates the time stamp to the current time to show when the failure occurred.

	-> Free the memory allocated to the record manager handler to prevent memory leaks.


    # createTable()

    	-> This function is used to create a table and It is used to store the information about the schema.

	-> It begins by allocating memory for the Record Manager and then proceeds to create and open a page file.

	-> Error handling is provided for file creation, opening, and memory allocation.

	-> After a successful operation, all allocated resources are cleaned up.


    # openTable()

    	-> This function opens a created table for operations.
	  
	-> It initializes the record manager and sets the table name while pinning the first page to access the table's metadata. 

	-> The function retrieves critical information such as the count of tuples, release page number, and attribute details from the page. 

	-> Memory is allocated for the schema structure, including attribute names, data types, and type lengths. 

	-> Function returns a status code indicating whether the table was opened successfully or if an error occurred.


     # closeTable() 

    	-> The closeTable function is responsible for closing an open table by accessing its associated record manager and attempting to shutdown the buffer pool.
	  
	-> Based on the result of the shutdown operation, it logs whether the table closure was successful or if an error occurred.

	-> The function updates the action log with the timestamp of the operation 

	-> Frees the memory allocated for the record manager.

	-> incrementTableCount function ensures that the provided count is a positive integer before updating the MAX_COUNT variable.


     # deleteTable()

    	-> This function is used to delete a created table.
	  
	-> On successful deletion, it logs a success message and returns an indication of the operation's success.

     
     # getNumTuples()

    	-> This method is used to get the total number of tuples in the table.
	  
	-> If the data is valid, it returns the count of tuples stored in the record manager.


## Implemented by Aditya Savaliya

    ----------------------HANDLING RECORDS IN A TABLE----------------------

     # insertRecord()
  
        -> The insertRecord() function initializes a new Record Id and pins the page in buffer pool.

 	-> Checks for availability of space in the page and then inserts the record and if space is not available, it moves to the next page.

 	-> Marks the page as dirty if the record is successfully inserted and unpins the page in buffer pool.


      # deleteRecord()

 	-> The deleteRecord() function checks if the record exists in the table and deletes it if it exists.

 	-> Then it marks the page as dirty and unpins the page.

 	-> If the record is not found, it returns an error


      # updateRecord()

 	-> The updateRecord() function checks if the record exists in the table and updates it if it exists.

 	-> Then it marks the page as dirty and unpins the page

 	-> Then unpins the page and return success if the record is not found.


     # getRecord()

 	-> The getRecord() function checks if the record exists in the table and returns it if it exists.

 	-> If pinning the page fails, it returns an error else it returns the record.

 	-> If the record is found (indicated by data not starting with #), it copis data into the output record and returns success.

	-> After copying the data, it unpins the page


     ----------------------SCANS----------------------

     # startscan()

    	-> The funtion initializes all the attributes of the RM_ScanHandle and RM_ScanInfo structs.

 
     # next()

    	-> The function uses the attributes initialized by the RM_ScanInfo structs.

    	-> Scans the entire table record wise.

    	-> Returns all the tuples if no condition is provided.

    	-> If condition is provided uses evalExpr to evaluate the expressions and return tuples satisfying the provided condition.


      # closeScan()

    	-> Does the job to de-allocate all the resources used.

    	-> Points all the used resource values to NULL and then deallocating them.


      # attrOffset()

    	-> Calculates the offset for a record.

    	-> Calculates the total size of all the attributes by using their datatypes.


## Implemented by Meet Saini

    ----------------------DEALING WITH SCHEMES----------------------

     # getRecordSize()

 	-> This function calculates the size of a record based on the schema.

 	-> For each attribute, it calculates the size based on the data type and accumulates total size.

 	-> After processing all attributes, it increments the total size by 1 for metadata and returns the total size.


     # createSchema()

 	-> The createSchema() function initializes a new Schema structure to define the attributes of a table, allocates memory for it and checks for errors.

 	-> If successful, it populates the schema with parameters provided: the number of attributes, their names, their data types, their lengths, and their keys


     # freeSchema()

 	-> The freeSchema() function is used to free the memory allocated for the schema.


  
   ----------------------DEALING WITH RECORDS AND ATTRIBUTES VALUES----------------------

      # createRecord()

    	-> Attributesllocates memory for a record.

    	-> Returns RC_MEMORY_ALLOCATION_FAIL if memory allocation fails.

 
      # freeRecord() 

    	-> Frees up all the memory allocated to a record.

    	-> Return RC_RE_NO_SUCH_RECORD on deleting non existing record.


      # getAttr()
	
    	-> Retrieves all the attributes and their values for a record.

   	-> Allocates memory to store all the attribute values.

    	-> Uses the calcAttrOffset to retieve all the attribute values for the specific record.


     # setAttr()

    	-> The function is used to set the attribute values for a record.

    	-> Uses calcAttrOffset.
    


## Group Members
---------------------------------------------------------------

- Aditya Savaliya
- Manasvi Lambore
- Meet Saini