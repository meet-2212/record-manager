#include "storage_mgr.h"
#include "dberror.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

void initStorageManager(void)
{
}

/*
    # It creates a new page file with "fileName"
    # Uses fopen() function and writes data
    # Returns RC_FILE_NOT_FOUND if unsuccessfull
*/
RC createPageFile(char *fileName)
{
    // Open the file in write mode
    FILE *filePtr = fopen(fileName, "w");

    // Check if the file was opened successfully

    if (filePtr != NULL)
    {
        // Allocate a blank page to store data
        char *blankPage = (char *)calloc(PAGE_SIZE, sizeof(char));

        // Allocate headerPage to store file info like total number of pages
        char *headerPage = (char *)calloc(PAGE_SIZE, sizeof(char));

        // set initial page number to 1 and point the filePtr to headerPage
        fputs("1", filePtr);

        // Write the headerPage with total number of pages
        fwrite(headerPage, PAGE_SIZE, 1, filePtr);

        // Write the blankPage
        fwrite(blankPage, PAGE_SIZE, 1, filePtr);

        // Free the allocated memory
        free(headerPage);
        free(blankPage);

        // Close the file pointer
        fclose(filePtr);

        return RC_OK;
    }
    else
        return RC_FILE_NOT_FOUND;
}

/*
    # This method opens an existing page file
    # Updates and stores the file attributes in mgmtInfo
    # Returns RC_FILE_NOT_FOUND if the file does not exist
*/
RC openPageFile(char *fileName, SM_FileHandle *fHandle)
{
    FILE *filePtr; // file pointer

    filePtr = fopen(fileName, "r+"); // open the pageFile

    if (filePtr != NULL) // if file exists
    {
        /*update the fileHandle attributes*/

        (*fHandle).fileName = fileName; // store the file name

        /*read headerPage to get the Total Number of Pages*/
        char *headerPage;
        headerPage = (char *)calloc(PAGE_SIZE, sizeof(char));
        fgets(headerPage, PAGE_SIZE, filePtr);
        char *totalPage;
        totalPage = headerPage;

        (*fHandle).totalNumPages = atoi(totalPage); // convert to integer
        (*fHandle).curPagePos = 0;                  // store the current page position

        // store the File pointer information in the Management info of Page Handle
        (*fHandle).mgmtInfo = filePtr;

        free(headerPage); // free memory to avoid memory leaks

        return RC_OK;
    }
    else // if file does not exists
    {
        return RC_FILE_NOT_FOUND;
    }
}

/*
    # This method is used to close the pageFile
    # Returns the respective RC code.
*/
RC closePageFile(SM_FileHandle *fHandle)
{
    // if closing the file is success
    if (fclose((*fHandle).mgmtInfo) == 0)
    {
        return RC_OK;
    }
    else
    {
        return RC_FILE_NOT_FOUND;
    }
}

/*
    #This method deletes the pageFile
*/
RC destroyPageFile(char *fileName)
{
    // if remove pageFile is successful
    if (!remove(fileName))
    {
        return RC_OK;
    }
    else
    {
        return RC_FILE_NOT_FOUND;
    }
}

/*
    # This method checks if the page is valid or not and returns RC_READ_NON_EXISTING_PAGE if invalid
    # Reads a page specified by pageNum to memPage
    # Then it will return RC_READ_NON_EXISTING_PAGE
*/
RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    if (fHandle == NULL)
        return RC_FILE_HANDLE_NOT_INIT;

    // Validate the page number for suring that it is in valid range
    if (pageNum < 0 || pageNum > (*fHandle).totalNumPages - 1)
    {
        // Return an error if the page does not exist
        return RC_READ_NON_EXISTING_PAGE;
    }

    // Setting the file pointer at the correct position for reading a block
    else
    {
        fseek((*fHandle).mgmtInfo, (pageNum + 1) * PAGE_SIZE, SEEK_SET);
        fread(memPage, sizeof(char), PAGE_SIZE, (*fHandle).mgmtInfo);
        // Updates current page position
        (*fHandle).curPagePos = pageNum;
        return RC_OK;
    }
}

/*
    # The following method returns the page currently pointed.
*/
int getBlockPos(SM_FileHandle *fHandle)
{
    return (*fHandle).curPagePos;
}

/*
    # This method reads the first block using readBlock
    # Returns error RC_READ_NON_EXISTING_PAGE if no pages found
*/
RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    // Check if file handler is valid or not
    if (fHandle == NULL)
        return RC_FILE_HANDLE_NOT_INIT;

    // Reads first block
    if (RC_OK == readBlock(0, fHandle, memPage))
    {
        return RC_OK;
    }
    else
        // Return error if no pages found
        return RC_READ_NON_EXISTING_PAGE;
}

/*
    # The methods reads the previous block using readBlock
    # return error RC_READ_NON_EXISTING_PAGE if page not found
*/
RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    // Check if file handler is valid or not
    if (fHandle == NULL)
        return RC_FILE_HANDLE_NOT_INIT;

    // Reads previous block
    if (RC_OK == readBlock(getBlockPos(fHandle) - 1, fHandle, memPage))
        return RC_OK;
    else
        // Return error if no pages found
        return RC_READ_NON_EXISTING_PAGE;
}

/*
    # This method reads the first block using readBlock
    # Returns error RC_READ_NON_EXISTING_PAGE if no pages found
*/
RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    // Check if file handler is valid or not
    if (fHandle == NULL)
        return RC_FILE_HANDLE_NOT_INIT;

    // Reads current block
    if (RC_OK == readBlock(getBlockPos(fHandle), fHandle, memPage))
        return RC_OK;
    else
        // Return error if no pages found
        return RC_READ_NON_EXISTING_PAGE;
}

/*
    # This method reads a block next from the current block using readBlock
    # Returns error RC_READ_NON_EXISTING_PAGE if no pages found
*/
RC readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    // Check if file handler is valid or not
    if (fHandle == NULL)
        return RC_FILE_HANDLE_NOT_INIT;

    // Reads next block
    if (RC_OK == readBlock(getBlockPos(fHandle) + 1, fHandle, memPage))
        return RC_OK;
    else
        // Return error if no pages found
        return RC_READ_NON_EXISTING_PAGE;
}

/*
    # This method reads the last block using readBlock
    # Returns error RC_READ_NON_EXISTING_PAGE if no pages found
*/
RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    // Check if file handler is valid or not
    if (fHandle == NULL)
        return RC_FILE_HANDLE_NOT_INIT;

    // Read last block
    if (RC_OK == readBlock((*fHandle).totalNumPages - 1, fHandle, memPage))
        return RC_OK;
    else
        // Return error if no pages found
        return RC_READ_NON_EXISTING_PAGE;
}

/*
    # The following method writes from the block pointed by given pageNum to memPage
    # The method returns RC_WRITE_FAILED if trying to write in invalid page or any if any error occurs.
    # The methoad also updates the file handler to update the current page position.
*/
RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    // initially check if there is a pointer to the desired file.
    if (fHandle == NULL)
        return RC_FILE_HANDLE_NOT_INIT;

    // check if the provided page number exists in the file or no
    if (pageNum > (*fHandle).totalNumPages - 1 || pageNum < 0)
    {
        // page with pageNum doesn't exist
        return RC_WRITE_FAILED;
    }

    // moving the file cursor to the page number provided
    fseek((*fHandle).mgmtInfo, (pageNum + 1) * PAGE_SIZE, SEEK_SET);
    {
        // write the block to memPage
        fwrite(memPage, PAGE_SIZE, 1, (*fHandle).mgmtInfo);

        // update the curPagePos to pageNum;
        (*fHandle).curPagePos = pageNum;

        return RC_OK;
    }
}
/*
    # The method below writes the current page to memPage.
    # Returns RC_WRITE_FAILED in case of any error.
*/
RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    // check for the file handler
    if (fHandle == NULL)
        return RC_FILE_HANDLE_NOT_INIT;

    if (writeBlock(getBlockPos(fHandle), fHandle, memPage) == RC_OK)
        return RC_OK;
    else
        return RC_WRITE_FAILED;
}

/*
    # The method below creates a new block and fills it with zero bytes.
    # It also updates the required file attributes for the filepage.
    # Writes the newly created block to the file.
*/
RC appendEmptyBlock(SM_FileHandle *fHandle)
{
    // check for the pointer to the file
    if (fHandle == NULL)
        return RC_FILE_HANDLE_NOT_INIT;

    // create and allocate the new empty block.
    char *newBlock;

    newBlock = (char *)calloc(PAGE_SIZE, sizeof(char));

    // move the cursor to the recently added block
    fseek((*fHandle).mgmtInfo, ((*fHandle).totalNumPages + 1) * PAGE_SIZE, SEEK_SET);

    // append the block to the file if possible
    if (fwrite(newBlock, PAGE_SIZE, 1, (*fHandle).mgmtInfo))
    {
        // update the attributes of fhandle
        (*fHandle).curPagePos = (*fHandle).totalNumPages - 1;
        (*fHandle).totalNumPages += 1;

        // seek to the header page to update the total pages
        fseek((*fHandle).mgmtInfo, 0L, SEEK_SET);
        // write with formatting required as totalNumPages is of int type
        fprintf((*fHandle).mgmtInfo, "%d", (*fHandle).totalNumPages);

        // free up the allocated space
        free(newBlock);

        // append to file successful
        return RC_OK;
    }
    else
    {
        // free up the allocated space
        free(newBlock);

        // append to file unsuccessful
        return RC_WRITE_FAILED;
    }
}

/*
    # The following method ensures that the file has specified numberOfPages
    # If the file doesn't have specified numberOfPages.
    # then the required number of pages are added to achieve the specified capacity.
*/
RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle)
{
    // check if the pointer to file exists
    if (fHandle == NULL)
        return RC_FILE_HANDLE_NOT_INIT;

    // check if the toatal pages equals the specified numberOFPages
    if (numberOfPages <= (*fHandle).totalNumPages)
        return RC_OK;

    int i, requiredPages;

    // calculate the number of pages to add
    requiredPages = numberOfPages - (*fHandle).totalNumPages;

    // iterate through and add required number of pages
    for (int i = 0; i < requiredPages; i++)
    {
        // adds empty pages
        appendEmptyBlock(fHandle);
    }
    return RC_OK;
}
