#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "record_mgr.h"
#include "dberror.h"

enum SCAN_MGR_Response
{
    SCAN_SUCCESS,
    SCAN_FAIL
};

enum SCHEMA_MGR_Response
{
    SCHEMA_CREATED,
    SCHEMA_NOT_CREATED,
};

enum TABLE_MGR_Response
{
    TABLE_CREATED,
    TABLE_NOT_CREATED,
    TABLE_OPENED,
    OPEN_TABLE_FAILED,
    CLOSE_TABLE_FAILED,
    TABLE_CLOSED,
    TABLE_DELETEED,
    DELETE_TABLE_ERROR,
    SCAN_TABLE_SUCCESS,
    SCAN_TABLE_ERROR
};

enum RECORD_Response
{
    RECORD_INSERTED,
    RECORD_NOT_INSERTED,
    RECORD_DELETED,
    RECORD_NOT_DELETED,
    RECORD_NOT_UPDATED,
    FOUND_RECORD,
    FOUND_NOT_RECORD,
    INIT_RECORD_MANAGER,
    SHUTDOWN_RECORD_FAILED
};

typedef struct RecordMgr
{
    BM_PageHandle pageHandle;
    int scanCount;
    int record;
    Expr *condition;
    int countOfTuples;
    int deallocatePage;
    RID r_id;
    BM_BufferPool bp;
} RecordMgr;

typedef struct controller_state
{
    enum TABLE_MGR_Response TM_resp;
    enum RECORD_Response state;
    enum SCHEMA_MGR_Response schema_resp;
    enum SCAN_MGR_Response SCN_resp;
    int recordUpdatedAt;
} c_state;

struct Controller
{
    RecordMgr *recMgr;
    RecordMgr *sm;
    RecordMgr *tm;
    c_state currState;
} mgrHandler;

int indexCount = 1;
int MAX_COUNT = 1;
const int MAX_NO_OF_PAGES = 200;
const int SIZE_OF_ATTRIBUTE = 20;

void clearMemory(void *ptor)
{
    free(ptor);
}

/* Table and Record Manager Functions */

/*
    # This method is used to initialise a record manager
*/
RC initRecordManager(void *mgmtData)
{
    // Initialize storage manager
    initStorageManager();
    mgrHandler.currState.state = INIT_RECORD_MANAGER;
    mgrHandler.currState.recordUpdatedAt = time(NULL);
    return RC_OK;
}

/*
    # This method is used to shutdown a record Manger
*/
RC shutdownRecordManager()
{
    if (shutdownBufferPool(&mgrHandler.recMgr->bp) != RC_OK)
    {
        mgrHandler.currState.state = SHUTDOWN_RECORD_FAILED;
        mgrHandler.currState.recordUpdatedAt = time(NULL);
    }
    // Prevents memory leaks and ensures that we only free memory that has been allocated
    free(mgrHandler.recMgr);
    return RC_OK;
}

/*
    # This function is used to create a table
    # It is used to store the information about the schema
*/
RC createTable(char *name, Schema *schema)
{
    // Buffer for page data
    char data[PAGE_SIZE];
    // File handle for page file operations
    SM_FileHandle f_handle;
    // Variable to store function call status
    RC status;

    // Allocate memory for the Record Manager structure
    mgrHandler.recMgr = (RecordMgr *)malloc(sizeof(RecordMgr));
    if (mgrHandler.recMgr == NULL)
    {
        // Return error if memory allocation fails
        return RC_MEM_ALLOCATION_FAIL;
    }

    // Initialize the buffer pool
    if (initBufferPool(&mgrHandler.recMgr->bp, name, MAX_NO_OF_PAGES, RS_LRU, NULL) != RC_OK)
    {
        mgrHandler.currState.TM_resp = TABLE_NOT_CREATED;
        mgrHandler.currState.recordUpdatedAt = time(NULL);
        // Free allocated memory if buffer pool init fails
        free(mgrHandler.recMgr);
        return RC_BUFF_SHUTDOWN_FAILED;
    }

    char *pageHandle = data;

    int k = 0;
    while (k < 4)
    {
        if (k == 0)
        {
            *(int *)pageHandle = 0;
        }
        else if (k == 1)
        {
            *(int *)pageHandle = 1;
        }
        else if (k == 2)
        {
            *(int *)pageHandle = schema->numAttr;
        }
        else if (k == 3)
        {
            *(int *)pageHandle = schema->keySize;
        }

        pageHandle += sizeof(int);
        // Increment k to proceed to the next iteration
        k++;
    }

    int i = 0;
    char *handle = pageHandle;

    while (i < schema->numAttr)
    {
        // Copy attribute name from the schema to handle
        strncpy(handle, schema->attrNames[i], SIZE_OF_ATTRIBUTE);

        // Move handle forward by the size of attribute name
        handle += SIZE_OF_ATTRIBUTE;

        // Assign the data type of the attribute
        *(int *)handle = (int)schema->dataTypes[i];

        // Move handle forward by the size of an integer
        handle += sizeof(int);

        // Assign the length of the attribute type
        *(int *)handle = (int)schema->typeLength[i];

        // Move handle forward by the size of an integer
        handle += sizeof(int);

        // Increment index to the next attribute
        i++;
    }

    // Create the page file
    status = createPageFile(name);
    if (status != RC_OK)
    {
        // If the page file creation fails, log the error state
        mgrHandler.currState.TM_resp = TABLE_NOT_CREATED;
        mgrHandler.currState.recordUpdatedAt = time(NULL);
        // Perform cleanup by freeing the allocated memory for the record manager
        free(mgrHandler.recMgr);
        // Return the failure status
        return status;
    }

    // Open the newly created page file
    status = openPageFile(name, &f_handle);
    if (status != RC_OK)
    {
        // If the page file open fails, log the error state
        mgrHandler.currState.TM_resp = TABLE_NOT_CREATED;
        mgrHandler.currState.recordUpdatedAt = time(NULL);
        // Perform cleanup
        free(mgrHandler.recMgr);
        return status;
    }

    // Write metadata and schema information to the first block of the page file
    status = writeBlock(0, &f_handle, data);
    if (status != RC_OK)
    {
        // If the write operation fails, log the error state
        mgrHandler.currState.TM_resp = TABLE_NOT_CREATED;
        mgrHandler.currState.recordUpdatedAt = time(NULL);
        // Ensure that the page file is closed before exiting the function
        closePageFile(&f_handle);
        // Perform cleanup by freeing the allocated memory for the record manager
        free(mgrHandler.recMgr);
        return status;
    }

    // Close the page file after writing operation
    status = closePageFile(&f_handle);
    if (status != RC_OK)
    {
        // If the page file closure fails, log the error state
        mgrHandler.currState.TM_resp = TABLE_NOT_CREATED;
        mgrHandler.currState.recordUpdatedAt = time(NULL);
        // Perform cleanup by freeing the allocated memory for the record manager
        free(mgrHandler.recMgr);
        return status;
    }

    // Give record success in state log
    mgrHandler.currState.TM_resp = TABLE_CREATED;
    mgrHandler.currState.recordUpdatedAt = time(NULL);

    // Return success if all the steps completed without any kind of error
    return RC_OK;
}

int getIncrement(int r)
{

    return r++;
}

/*
    The function calculates the total number of slots that can fit in the page
    by dividing the PAGE_SIZE by recordSize.
*/
int availableSlot(char *data, int recordSize)
{
    float sizedata = 0;
    int index = -1, numberOfSlots;
    numberOfSlots = PAGE_SIZE / recordSize;

    if (index == -1)
    {
        index = 0;
        while (index < numberOfSlots)
        {
            if (*(data + index * recordSize) != '#')
            {
                return index;
            }
            index++;
        }
    }
    else
    {
        return -1;
    }
    sizedata++;
    return -1;
}

/*
    # This function opens a created table for operations
*/
RC openTable(RM_TableData *rel, char *name)
{
    int res = 0;
    SM_PageHandle pageHandle;
    bool status;

    // Initialize the record manager and assign table name to it
    res += getIncrement(res);
    rel->name = name;
    rel->mgmtData = mgrHandler.recMgr;
    res += getIncrement(res);

    // Pin the first page to access table metadata
    status = (pinPage(&mgrHandler.recMgr->bp, &mgrHandler.recMgr->pageHandle, 0) == RC_OK);
    if (!status)
    {
        mgrHandler.currState.TM_resp = OPEN_TABLE_FAILED;
        mgrHandler.currState.recordUpdatedAt = time(NULL);
        return RC_ERROR;
    }

    res++;

    int initialTableData = 2;

    // Get the current page's data as a char pointer
    pageHandle = (char *)mgrHandler.recMgr->pageHandle.data;

    // Read the count of tuples from the first part of the page
    mgrHandler.recMgr->countOfTuples = *(int *)pageHandle;

    // Move the page handle forward by the size of an integer
    pageHandle = pageHandle + sizeof(int);

    // Read the release page number
    mgrHandler.recMgr->deallocatePage = *(int *)pageHandle;

    // Move the page handle forward again
    pageHandle = pageHandle + sizeof(int);

    // Retrieve the attribute count from the page
    int attrCount = *(int *)pageHandle;

    pageHandle += sizeof(int);

    initialTableData += attrCount;

    // Allocate memory for the schema structure
    Schema *schema = (Schema *)calloc(1, sizeof(Schema));

    // Set the number of attributes in the schema
    schema->numAttr = attrCount;

    // Allocate memory for attribute names by initializing it to zero
    schema->attrNames = (char **)calloc(attrCount, sizeof(char *));

    // Allocate memory for data types by initializing it to zero
    schema->dataTypes = (DataType *)calloc(attrCount, sizeof(DataType));

    // Allocate memory for type lengths by initializing it to zero
    schema->typeLength = (int *)calloc(attrCount, sizeof(int));

    int i = 0;
    while (i < attrCount)
    {
        // Allocate the memory for attribute names
        schema->attrNames[i] = (char *)malloc(SIZE_OF_ATTRIBUTE);

        // Copy attribute name from pageHandle
        strncpy(schema->attrNames[i], pageHandle, SIZE_OF_ATTRIBUTE);

        // Move pageHandle by the size of the attribute
        pageHandle = pageHandle + SIZE_OF_ATTRIBUTE;

        // Assign the data type of the attribute
        schema->dataTypes[i] = *(DataType *)pageHandle;

        // Move pageHandle by the size of DataType
        pageHandle = pageHandle + sizeof(DataType);

        // Assign the type length of the attribute
        schema->typeLength[i] = *(int *)pageHandle;

        // Move pageHandle by the size of an integer
        pageHandle = pageHandle + sizeof(int);

        // Increment the index
        i++;
    }

    rel->schema = schema;

    status = forcePage(&mgrHandler.recMgr->bp, &mgrHandler.recMgr->pageHandle) == RC_OK;
    if (!status)
    {
        // Log the failure of opening the table
        mgrHandler.currState.TM_resp = OPEN_TABLE_FAILED;
        // Update the timestamp of the recorded state
        mgrHandler.currState.recordUpdatedAt = time(NULL);
    }
    else
    {
        // Log the successful opening of the table
        mgrHandler.currState.TM_resp = TABLE_OPENED;
        // Update the timestamp of the recorded state
        mgrHandler.currState.recordUpdatedAt = time(NULL);
    }
    // Return RC_ERROR if failed otherwise return RC_OK
    return !status ? RC_ERROR : RC_OK;
}

/*
    # This function closes the table after all operations are finished
    # It de-allocates all memory to prevent memory leaks
*/
RC closeTable(RM_TableData *rel)
{
    RecordMgr *rMgr = (*rel).mgmtData;

    int result = shutdownBufferPool(&rMgr->bp);
    if (result == (float)RC_ERROR)
    {
        mgrHandler.currState.TM_resp = CLOSE_TABLE_FAILED;
        mgrHandler.currState.recordUpdatedAt = time(NULL);
    }
    else
    {
        mgrHandler.currState.TM_resp = TABLE_CLOSED;
        mgrHandler.currState.recordUpdatedAt = time(NULL);
    }
    return (result == RC_ERROR) ? RC_ERROR : RC_OK;
}

void incrementTableCount(int count)
{
    // Ensure count is a attrPositive integer before updating
    if (count > 0)
    {
        MAX_COUNT = count;
    }
    else
    {
        printf("Error: count must be a attrPositive integer.\n");
    }
}

/*
    # This fucntion is used to delete a created table
    # It is done by using destroyPageFile function from the BufferManger implementation
*/
RC deleteTable(char *name)
{
    if (destroyPageFile(name) != RC_OK)
    {
        // Increment table count if needed
        incrementTableCount(1);
        mgrHandler.currState.TM_resp = DELETE_TABLE_ERROR;
        mgrHandler.currState.recordUpdatedAt = time(NULL);
        return RC_ERROR;
    }

    // Log the successful deletion of the table
    mgrHandler.currState.TM_resp = TABLE_DELETEED;
    mgrHandler.currState.recordUpdatedAt = time(NULL);

    return RC_OK;
}

/*
    # This method is used to get the total number of tuples in the table
*/
int getNumTuples(RM_TableData *rel)

{
    // Check if the table data or management data is NULL
    if (rel == NULL || rel->mgmtData == NULL)
    {
        printf("Error: Table data or management data is NULL.\n");
        // Return an error code if the input is invalid
        return -1;
    }

    RecordMgr *recMgr = rel->mgmtData;
    return recMgr->countOfTuples;
}

/*
    # This function is used to insert a new record into the table
    # It takes the table data pointer and the new record
    # It returns 0 on success and -1 on failure
*/
RC insertRecord(RM_TableData *rel, Record *record)
{
    char *data;
    RID *rec_ID = &(*record).id;
    RecordMgr *recordMgr = (*rel).mgmtData;
    int retryCount = 0;
    float varValue = 1.0;

    // Intialize the record ID
    if (!retryCount)
    {
        recordMgr = rel->mgmtData;
        rec_ID->page = recordMgr->deallocatePage;
    }

    // Check if the record exists in the table and return error if it does
    if (pinPage(&(*recordMgr).bp, &(*recordMgr).pageHandle, (*rec_ID).page) != RC_OK)
    {
        mgrHandler.currState.TM_resp = RECORD_NOT_INSERTED;
        mgrHandler.currState.recordUpdatedAt = time(NULL);
        return RC_ERROR;
    }

    // Find an available slot in the page
    data = (*recordMgr).pageHandle.data;
    rec_ID->slot = availableSlot(data, getRecordSize(rel->schema));

    // If the slot is not available, move to the next page and attempt pinning again
    while (rec_ID->slot == -1)
    {
        // Unpin the page before moving to the next page
        if (unpinPage(&(*recordMgr).bp, &(*recordMgr).pageHandle) == RC_ERROR)
        {
            mgrHandler.currState.TM_resp = RECORD_NOT_INSERTED;
            mgrHandler.currState.recordUpdatedAt = time(NULL);
            return RC_ERROR;
        }

        // Move to the next page and attempt pinning
        rec_ID->page++;
        if (pinPage(&(*recordMgr).bp, &(*recordMgr).pageHandle, rec_ID->page) != RC_OK)
        {
            mgrHandler.currState.TM_resp = RECORD_NOT_INSERTED;
            mgrHandler.currState.recordUpdatedAt = time(NULL);
            return RC_ERROR;
        }

        // Find an available slot in the page after pinning
        data = (*recordMgr).pageHandle.data;
        rec_ID->slot = availableSlot(data, getRecordSize(rel->schema));
    }

    // Mark the page as dirty
    if (markDirty(&(*recordMgr).bp, &(*recordMgr).pageHandle) == RC_ERROR)
    {
        mgrHandler.currState.TM_resp = RECORD_NOT_INSERTED;
        mgrHandler.currState.recordUpdatedAt = time(NULL);
        return RC_ERROR;
    }

    // Write the record data into the identified slot
    char *slotPointer = data + ((*rec_ID).slot * getRecordSize(rel->schema));
    *slotPointer = '#';
    memcpy(slotPointer + 1, (*record).data + 1, getRecordSize(rel->schema) - 1);

    // Unpin the page before updating global info
    if (unpinPage(&(*recordMgr).bp, &(*recordMgr).pageHandle) == RC_ERROR)
    {
        mgrHandler.currState.TM_resp = RECORD_NOT_INSERTED;
        mgrHandler.currState.recordUpdatedAt = time(NULL);
        return RC_ERROR;
    }

    // Update global info
    recordMgr->countOfTuples++;

    // Attempt to pin the page after updating global info
    if (pinPage(&(*recordMgr).bp, &(*recordMgr).pageHandle, 0) == RC_ERROR)
    {
        mgrHandler.currState.TM_resp = RECORD_NOT_INSERTED;
        mgrHandler.currState.recordUpdatedAt = time(NULL);
        return RC_ERROR;
    }

    // Log success and update the state record
    mgrHandler.currState.TM_resp = RECORD_INSERTED;
    mgrHandler.currState.recordUpdatedAt = time(NULL);
    return RC_OK;
}

/*
    # This function deletes the record based on the RID
    # If the record is not found, it returns NULL
    # Otherwise, it returns the pointer to the record
*/
RC deleteRecord(RM_TableData *rel, RID id)
{
    // Get the record manager
    RecordMgr *recordMgr = (RecordMgr *)(*rel).mgmtData;

    // Attempt to pin the page
    if (pinPage(&(*recordMgr).bp, &(*recordMgr).pageHandle, id.page) == RC_ERROR)
    {
        // Log failure and update the state record
        mgrHandler.currState.TM_resp = RECORD_NOT_DELETED;
        mgrHandler.currState.recordUpdatedAt = time(NULL);
        return RC_ERROR;
    }

    // Mark the record as deleted in the page data
    (*recordMgr).deallocatePage = id.page;

    // Write the record data into the identified slot
    char *data = (*recordMgr).pageHandle.data + (id.slot * getRecordSize(rel->schema));
    *data = '-';

    // Mark the page as dirty
    if (markDirty(&(*recordMgr).bp, &(*recordMgr).pageHandle) == RC_ERROR)
    {
        mgrHandler.currState.TM_resp = RECORD_NOT_DELETED;
        mgrHandler.currState.recordUpdatedAt = time(NULL);
        return RC_ERROR;
    }

    // Unpin the page after writing data
    if (unpinPage(&(*recordMgr).bp, &(*recordMgr).pageHandle) != RC_OK)
    {
        mgrHandler.currState.TM_resp = RECORD_NOT_DELETED;
        mgrHandler.currState.recordUpdatedAt = time(NULL);
        return RC_ERROR;
    }

    // Log success and update the state record
    mgrHandler.currState.TM_resp = RECORD_DELETED;
    mgrHandler.currState.recordUpdatedAt = time(NULL);
    return RC_OK;
}

/*
    # This function is used to update a record
    # It takes the table data pointer and the new record
*/
RC updateRecord(RM_TableData *table, Record *newRecord)
{
    RC returnValue;
    RecordMgr *recordManager = (RecordMgr *)(*table).mgmtData;
    bool shouldUpdate = true;

    // Check if the record exists in the table
    if (pinPage(&(*recordManager).bp, &(*recordManager).pageHandle, (*newRecord).id.page) == RC_ERROR)
    {
        mgrHandler.currState.TM_resp = RECORD_NOT_UPDATED;
        mgrHandler.currState.recordUpdatedAt = time(NULL);
        return RC_ERROR;
    }

    // Check if the record should be updated or not
    if (shouldUpdate)
    {
        // Get the current data
        char *currentDataattrPosition = (*recordManager).pageHandle.data;

        // Calculate the current data attrPosition
        currentDataattrPosition += (*newRecord).id.slot * getRecordSize(table->schema);

        // Mark the current data as deleted
        *currentDataattrPosition = '#';

        // Copy the new data
        memcpy(currentDataattrPosition + 1, (*newRecord).data + 1, getRecordSize(table->schema) - 1);
    }

    // Mark the page as dirty after making changes
    if (markDirty(&(*recordManager).bp, &(*recordManager).pageHandle) != RC_OK)
    {
        mgrHandler.currState.TM_resp = RECORD_NOT_UPDATED;
        mgrHandler.currState.recordUpdatedAt = time(NULL);
        return RC_ERROR;
    }

    // Unpin the page after writing data
    return unpinPage(&(*recordManager).bp, &(*recordManager).pageHandle) == RC_ERROR ? RC_ERROR : RC_OK;
}

/*
    # This function returns the record based on the RID
    # If the record is not found, it returns NULL
*/
RC getRecord(RM_TableData *table, RID id, Record *outputRecord)
{
    // Check if the record exists in the table
    RecordMgr *recordManager = (RecordMgr *)(*table).mgmtData;
    bool shouldFetchRecord = true;

    // Attempt to pin the page, return error if pinning fails or if the record is not found
    if (pinPage(&(*recordManager).bp, &(*recordManager).pageHandle, id.page) == RC_ERROR)
    {
        mgrHandler.currState.TM_resp = FOUND_NOT_RECORD;
        mgrHandler.currState.recordUpdatedAt = time(NULL);
        return RC_ERROR;
    }

    // Get the record data from the page
    char *dataPointer = (*recordManager).pageHandle.data;
    dataPointer += (id.slot * getRecordSize(table->schema));

    // Check if the record is found
    // # is used to indicate that the record is not found
    if (*dataPointer == '#')
    {
        // Check if the record should be fetched
        if (shouldFetchRecord)
        {
            char *recordData = outputRecord->data;
            outputRecord->id = id;
            // Copy the record data
            memcpy(++recordData, dataPointer + 1, getRecordSize(table->schema) - 1);
        }
    }
    else
    {
        if (shouldFetchRecord)
        {
            return RC_RM_NO_MORE_TUPLES;
        }
    }

    // Unpin the page after completing the operation
    if (unpinPage(&(*recordManager).bp, &(*recordManager).pageHandle) == RC_ERROR)
    {
        mgrHandler.currState.TM_resp = FOUND_NOT_RECORD;
        mgrHandler.currState.recordUpdatedAt = time(NULL);
        return RC_ERROR;
    }

    // Log successful retrieval of the record
    mgrHandler.currState.TM_resp = FOUND_RECORD;
    mgrHandler.currState.recordUpdatedAt = time(NULL);
    return RC_OK;
}

/*
    # This function returns the size of the record in bytes based on the schema
    # and the data types in the schema
*/
int getRecordSize(Schema *customSchema)
{
    // Check if the schema is valid
    if (customSchema == NULL || customSchema->numAttr <= 0)
    {
        printf("Invalid schema or schema has no attributes\n");
        return -1;
    }

    int totalSize = 0;

    // Calculate the total size of the record based on the schema
    for (int i = 0; i < customSchema->numAttr; i++)
    {
        switch (customSchema->dataTypes[i])
        {
        case DT_INT:
            totalSize += sizeof(int);
            break;
        case DT_STRING:
            totalSize += customSchema->typeLength[i];
            break;
        case DT_FLOAT:
            totalSize += sizeof(float);
            break;
        case DT_BOOL:
            totalSize += sizeof(bool);
            break;
        default:
            printf("Unrecognized data type\n");
            // Return error if an unidentified data type is found
            return -1;
        }
    }

    // Add 1 for the record's metadata
    return totalSize + 1;
}

/*
    # the function is used to initialize the scan manager
    # and all its attributes
*/
RC startScan(RM_TableData *r, RM_ScanHandle *s_handle, Expr *condition)
{
    if (condition == NULL)
    {
        mgrHandler.currState.TM_resp = SCAN_FAIL;
        mgrHandler.currState.recordUpdatedAt = time(NULL);
        return RC_SCAN_CONDITION_NOT_FOUND;
    }

    if (openTable(r, "ScanTable") == RC_ERROR)
    {
        mgrHandler.currState.TM_resp = SCAN_FAIL;
        mgrHandler.currState.recordUpdatedAt = time(NULL);
        return RC_ERROR;
    }

    mgrHandler.sm = (RecordMgr *)malloc(sizeof(RecordMgr));
    s_handle->mgmtData = mgrHandler.sm;
    mgrHandler.sm->r_id.page = 1;
    mgrHandler.sm->r_id.slot = 0;

    mgrHandler.sm->scanCount = 0;
    mgrHandler.sm->condition = condition;

    mgrHandler.tm = r->mgmtData;
    mgrHandler.tm->countOfTuples = SIZE_OF_ATTRIBUTE;

    s_handle->rel = r;

    mgrHandler.currState.SCN_resp = SCAN_FAIL;
    mgrHandler.currState.recordUpdatedAt = time(NULL);
    return RC_OK;
}

/*
    # this funtion evaluates the condition
    # return the tuples taht staisfy the condition given
    # if no condition is provided it returns all the tuples
*/
RC next(RM_ScanHandle *scan, Record *rec)

{
    RecordMgr *sm = scan->mgmtData, *tm = scan->rel->mgmtData;

    int slotCount;

    int count_scan_var_val = 1;

    int chkVal_flag = true;

    Schema *schema = scan->rel->schema;

    Value *outputExpr = (Value *)malloc(sizeof(Value));

    int tuple_Count = 0;

    slotCount = PAGE_SIZE / getRecordSize(schema);
    int dem = 1;
    while (sm->scanCount <= tm->countOfTuples && MAX_COUNT > 0)

    {
        count_scan_var_val--;

        if (sm->scanCount <= 0 && tuple_Count == 0)

        {
            if (chkVal_flag && tuple_Count == 0)

            {
                sm->r_id.page = 1;
                dem = 2;
            }

            sm->r_id.slot = 0 + 0;
        }

        else
        {
            if (dem)
            {
                sm->r_id.slot++;

                if (chkVal_flag)
                {

                    if (sm->r_id.slot >= slotCount)

                    {
                        dem = 1;
                        sm->r_id.slot = 0, sm->r_id.page++, tuple_Count--;
                    }
                }
            }
        }

        if (pinPage(&tm->bp, &sm->pageHandle, sm->r_id.page) == RC_ERROR)
        {
            mgrHandler.currState.SCN_resp = SCAN_FAIL;
            mgrHandler.currState.recordUpdatedAt = time(NULL);
            return RC_ERROR;
        }

        int val = 1;
        char *recordDta;
        char *valPtr;
        if (val)
        {
            int dta = 1;
            recordDta = sm->pageHandle.data;
            if (dta)
            {
                recordDta += (sm->r_id.slot * getRecordSize(schema));

                rec->id.page = sm->r_id.page;
                rec->id.slot = sm->r_id.slot;

                // initializing and updating valPtr with rrecord data
                valPtr = rec->data;
                *valPtr = '-';
            }

            if (val)
            {
                memcpy(++valPtr, recordDta + 1, getRecordSize(schema) - 1);
                val++;
            }

            sm->scanCount++;
        }

        if (evalExpr(rec, schema, sm->condition, &outputExpr) == RC_ERROR)
        {
            mgrHandler.currState.SCN_resp = SCAN_FAIL;
            mgrHandler.currState.recordUpdatedAt = time(NULL);
            return RC_ERROR;
        }
        tuple_Count = tuple_Count - 1;

        while (outputExpr->v.boolV == TRUE)
        {

            if (unpinPage(&tm->bp, &sm->pageHandle) == RC_ERROR)
            {
                mgrHandler.currState.SCN_resp = SCAN_FAIL;
                mgrHandler.currState.recordUpdatedAt = time(NULL);
                return RC_ERROR;
            }

            count_scan_var_val = count_scan_var_val + 1;

            mgrHandler.currState.SCN_resp = SCAN_SUCCESS;
            mgrHandler.currState.recordUpdatedAt = time(NULL);
            return RC_OK;
        }
    }

    if (unpinPage(&tm->bp, &sm->pageHandle) == RC_ERROR)
    {
        mgrHandler.currState.SCN_resp = SCAN_FAIL;
        mgrHandler.currState.recordUpdatedAt = time(NULL);
        return RC_ERROR;
    }

    sm->r_id.page = 1;

    tuple_Count--;
    sm->r_id.slot = 0;

    count_scan_var_val = tuple_Count + 1;
    sm->scanCount = 0;

    return RC_RM_NO_MORE_TUPLES;
}

/*
    # deallocates all the memory allocated to the scan manager
*/
RC closeScan(RM_ScanHandle *scan)
{

    RecordMgr *Record_Mgr = scan->rel->mgmtData;
    int counter = 0;
    mgrHandler.sm = scan->mgmtData;
    while (mgrHandler.sm->scanCount > 0)
    {
        if (unpinPage(&Record_Mgr->bp, &mgrHandler.sm->pageHandle) == RC_ERROR)
        {
            mgrHandler.currState.SCN_resp = SCAN_FAIL;
            mgrHandler.currState.recordUpdatedAt = time(NULL);
            counter = 1;
            return RC_ERROR;
        }
        mgrHandler.sm->scanCount = 0;
        mgrHandler.sm->r_id.slot = 0;
    }

    scan->mgmtData = NULL;
    free(scan->mgmtData);
    counter = 2;

    mgrHandler.currState.SCN_resp = SCAN_SUCCESS;
    mgrHandler.currState.recordUpdatedAt = time(NULL);
    return RC_OK;
}

/*
    # SCHEMA CREATION -
    # This function is used to Create a new Schema and initialize all the attributes to those schema
 */
Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
{
    // Check if parameters are valid for schema creation
    if (keySize <= 0)
    {
        mgrHandler.currState.schema_resp = SCHEMA_NOT_CREATED;
        mgrHandler.currState.recordUpdatedAt = time(NULL);
        return NULL;
    }

    // Allocate memory for the schema structure
    Schema *schema = (Schema *)malloc(sizeof(Schema));
    if (schema == NULL)
    {
        mgrHandler.currState.schema_resp = SCHEMA_NOT_CREATED;
        mgrHandler.currState.recordUpdatedAt = time(NULL);
        return NULL;
    }

    // Initialize the schema with the provided parameters
    schema->numAttr = numAttr;
    schema->attrNames = attrNames;
    schema->dataTypes = dataTypes;
    schema->typeLength = typeLength;
    schema->keySize = keySize;
    schema->keyAttrs = keys;

    return schema;
}

/*
    # This function is used to free the allocated Schema
*/
RC freeSchema(Schema *schema)
{
    // Check if schema is not NULL
    if (schema != NULL)
    {
        // Free the memory allocated for the schema
        free(schema);
        // Set the schema pointer to NULL
        schema = NULL;
    }
    // Return success
    return RC_OK;
}

/*
    # implementation of attrOffset
    # calculates the offset of the record
*/
RC attrOffset(Schema *schema, int attrNum, int *result)
{
    int offsetVal = 1;
    int dt_type = 0;
    if (offsetVal)
    {

        *result = 1;

        for (int k = 0; k < attrNum; ++k)
        {

            if (schema->dataTypes[k] == DT_STRING)
            {
                dt_type = 1;
                *result += schema->typeLength[k];
            }
            else if (schema->dataTypes[k] == DT_INT)
            {
                dt_type = 2;
                *result += sizeof(int);
            }
            else if (schema->dataTypes[k] == DT_BOOL)
            {
                dt_type = 3;
                *result += sizeof(bool);
            }
            else if (schema->dataTypes[k] == DT_FLOAT)
            {
                dt_type = 4;
                *result += sizeof(float);
            }
            else
            {
                dt_type = 1;
                return RC_RM_UNKOWN_DATATYPE;
            }
        }
    }

    return RC_OK;
}

/*
    # creates a new record
    # initializes the record attributres and then stores in provided address
*/
RC createRecord(Record **record, Schema *schema)
{
    if (schema == NULL)
        return RC_NULL_ARGUMENT;

    Record *rec = (Record *)malloc(sizeof(Record));
    if (rec == NULL)
        return RC_MEM_ALLOCATION_FAIL;

    int recordSize = getRecordSize(schema);
    rec->data = (char *)malloc(recordSize * sizeof(char));
    if (rec->data == NULL)
        return CREATE_RECORD_FAILED;

    // Initialize the record ID
    rec->id.page = -1;
    rec->id.slot = -1;

    // Initialize record data
    char *valPtr = rec->data;
    *valPtr = '-';
    valPtr++;
    *valPtr = '\0';

    // allocating rec to the record pointer
    *record = rec;
    return RC_OK;
}

/*
    # dealocates the memory for a record and its attributes
*/
RC freeRecord(Record *record)
{
    if (record == NULL)
        return RC_NULL_ARGUMENT;

    free(record->data);
    free(record);
    return RC_OK;
}

/*
    # retrieves the attribute values of a given record
    # uses the attrOffset function
*/
RC getAttr(Record *record, Schema *schema, int attrNum, Value **attrValue)
{
    int attrPos = 0;

    int ptr = 1;
    if (attrNum < 0)
        return RC_ERROR;

    else
    {
        char *recordDT = record->data;
        attrOffset(schema, attrNum, &attrPos);
        bool isOffset = true;

        Value *attrDT = (Value *)malloc(sizeof(Value));

        if (attrPos >= 0)
        {
            recordDT = recordDT + attrPos;
        }
        if (attrPos != 0 && ptr > 0)
        {
            schema->dataTypes[attrNum] = (attrNum != 1) ? schema->dataTypes[attrNum] : 1;
        }
        if (attrPos != 0 && ptr > 0)
        {
            switch (schema->dataTypes[attrNum])
            {
            case DT_INT:
            {
                if (isOffset)
                {
                    int value = 0;
                    memcpy(&value, recordDT, sizeof(int));
                    attrDT->dt = DT_INT;
                    attrDT->v.intV = value;
                    break;
                }
            }

            case DT_STRING:
            {
                int attrLength = schema->typeLength[attrNum];
                int valu = 1;
                if (valu)
                    attrDT->v.stringV = (char *)calloc(attrLength + 1, sizeof(char));
                if (isOffset)
                {
                    strncpy(attrDT->v.stringV, recordDT, attrLength);
                    attrDT->v.stringV[attrLength] = '\0';
                    attrDT->dt = DT_STRING;
                    attrPos++, ptr++;
                    valu = 1;
                }
                break;
            }

            case DT_BOOL:
            {
                attrPos++;
                bool value;
                memcpy(&value, recordDT, sizeof(bool));
                if (isOffset)
                {
                    attrDT->v.boolV = value;
                    attrDT->dt = DT_BOOL;
                    attrPos++, ptr++;
                }
                break;
            }

            case DT_FLOAT:
            {
                float value;
                memcpy(&value, recordDT, sizeof(float));
                if (isOffset)
                {
                    attrDT->dt = DT_FLOAT;
                    attrDT->v.floatV = value;
                }
                break;
            }

            default:
                printf("Invalid datatype encountered \n");
                break;
            }

            if (isOffset)
            {
                *attrValue = attrDT;
                mgrHandler.currState.SCN_resp = SCAN_FAIL;
                mgrHandler.currState.recordUpdatedAt = time(NULL);
            }
            return RC_OK;
        }
    }
    return RC_OK;
}
/*
    # the function sets the attribute values for a given record
    # performs edge case checks
*/
RC setAttr(Record *record, Schema *schema, int attrNum, Value *value)
{
    bool toSetAttribute = true;
    float attr = 0;
    int rattr = -1;
    int fop = 1;
    int attrPosition = 0;
    int attributeVal = 0;
    rattr += attributeVal;

    if (attrNum < 0)
    {

        attributeVal += 1;
        mgrHandler.currState.SCN_resp = SCAN_SUCCESS;
        if (fop)
        {
            mgrHandler.currState.recordUpdatedAt = time(NULL);
        }
        return RC_ERROR;
    }

    if (attrOffset(schema, attrNum, &attributeVal) != RC_OK)
    {
        mgrHandler.currState.SCN_resp = SCAN_FAIL;
        mgrHandler.currState.recordUpdatedAt = time(NULL);
        return RC_ERROR;
    }

    char *pointer_d = record->data + attributeVal;
    char *pointer_e = record->data;

    if (schema->dataTypes[attrNum] == DT_INT)
    {
        *(int *)pointer_d = value->v.intV;
    }
    else if (schema->dataTypes[attrNum] == DT_FLOAT)
    {
        *(float *)pointer_d = value->v.floatV;
    }
    else if (schema->dataTypes[attrNum] == DT_STRING)
    {
        strncpy(pointer_d, value->v.stringV, schema->typeLength[attrNum]);
        if (fop)
        {
            rattr++;
        }
    }
    else if (schema->dataTypes[attrNum] == DT_BOOL)
    {
        *(bool *)pointer_d = value->v.boolV;
    }
    else
    {
        printf("Datatype not available\n");
    }

    mgrHandler.currState.SCN_resp = SCAN_SUCCESS;
    mgrHandler.currState.recordUpdatedAt = time(NULL);
    return RC_OK;
}
