#include <stdio.h> 
#include <stdlib.h> 
#include <fcntl.h> 
#include <sys/types.h> 
#include <sys/stat.h> 
#include <errno.h> 
#include <string.h> 
#include <limits.h> 
#include "storage_mgr.h" 

/************************************************************
 *                    handle data structures                *
 ************************************************************/

/***************************************************************
 * Function Name: initStorageManager
 *
 *
***************************************************************/


void initStorageManager(void){
}



/***************************************************************
 * Function Name: createPageFile
 *
***************************************************************/


RC createPageFile(char *fileName){
    FILE *fp;
    char fill[PAGE_SIZE];

    fp = fopen(fileName,"w+");

    memset(fill, '\0', sizeof(fill));
    
    fwrite(fill, 1, PAGE_SIZE, fp);


    fclose(fp);
    return RC_OK;
}

/***************************************************************
 * Function Name: openPageFile
 *
***************************************************************/


RC openPageFile (char *fileName, SM_FileHandle *fHandle){
    FILE *fp;
    int size;
    int seek_result;

    fp = fopen(fileName, "r");

    if(fp == NULL){
        printf("input error,-----openpageFile\n");
        return RC_FILE_NOT_FOUND;
    }
    
    fseek(fp, 0, SEEK_END);

    size = ftell(fp);
    
    fHandle->fileName = fileName;
    int pageSize = size%PAGE_SIZE;
    
//    if(size%PAGE_SIZE == 0){
    if(pageSize == 0){
    	fHandle->totalNumPages = (size / PAGE_SIZE);
    }
    
    else
    {
    	fHandle->totalNumPages = (size/PAGE_SIZE +1);
    }
    
    fHandle->curPagePos = 0;
    
    fclose(fp);
    return RC_OK;

}

/***************************************************************
 * Function Name: closePageFile

***************************************************************/


RC closePageFile (SM_FileHandle *fHandle){
    return RC_OK;
}

/***************************************************************
 * Function Name: destroyPageFile

***************************************************************/


RC destroyPageFile (char *fileName){
    remove(fileName);
    return RC_OK;
}

/* reading blocks from disc */


/***************************************************************
 * Function Name: readBlock
 *
***************************************************************/


RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    RC rc = - 99;
    if (fHandle == NULL){
        printf("input error,-----readblock\n");
        return rc;
    }
    
    FILE *fp;
    int offset;
    
    
    fp=fopen(fHandle->fileName,"r");
    
    offset=fHandle->curPagePos*PAGE_SIZE;
    
    rc = fseek(fp,offset,SEEK_SET);
    
    if (rc != 0){
        printf("seek error, ------readblock\n");
        return RC_OK;
    }
    
    fread(memPage,sizeof(char),PAGE_SIZE,fp);
    
    fHandle->curPagePos= pageNum;
    
    fclose(fp);
    return RC_OK;
    
    
}

/***************************************************************
 * Function Name: getBlockPos
 *
***************************************************************/


int getBlockPos (SM_FileHandle *fHandle)
{
    RC rc = -99;
    if (fHandle == NULL){
        printf("input error, ------getblockpos\n");
        return rc;
    }
    
    int curPageppos= fHandle->curPagePos;
	return curPageppos;
}

/***************************************************************
 * Function Name:  readFirstBlock
***************************************************************/


RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{

	return RC_OK;
}

/***************************************************************
 * Function Name:readPreviousBlock
***************************************************************/


RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{

		return RC_OK;
}

/***************************************************************
 * Function Name: readCurrentBlock
 *
***************************************************************/


RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    return RC_OK;
}

/***************************************************************
 * Function Name: readNextBlock
 *
***************************************************************/


RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    return RC_OK;
}

/***************************************************************
 * Function Name: readLastBlock
 *
***************************************************************/


RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    return  RC_OK;
}

/* writing blocks to a page file */

/***************************************************************
 * Function Name: writeBlock 
 *
***************************************************************/
RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){
	
    RC rc = -99;
    if (fHandle == NULL){
        printf("input error, -----write block -----\n");
        return rc;
    }
    
    
    ensureCapacity (pageNum, fHandle);		//Make sure the program have enough capacity to write block.
	
    
	FILE *fp;
	

	fp=fopen(fHandle->fileName,"rb+");
    
    
    fseek(fp,pageNum * PAGE_SIZE, SEEK_SET);
    fwrite(memPage, PAGE_SIZE, 1, fp);
    fHandle->curPagePos=pageNum;
    
	fclose(fp);
//	return rv;
    return  RC_OK;
}


/***************************************************************
 * Function Name: writeCurrentBlock 
 
***************************************************************/
RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){

    
    RC rc = -99;
    if (fHandle == NULL){
        printf("input error, -------writeCurrent BLock\n");
        return rc;
    }
    
	return writeBlock(fHandle->curPagePos, fHandle, memPage);
}

/***************************************************************
 * Function Name: appendEmptyBlock 

***************************************************************/
RC appendEmptyBlock (SM_FileHandle *fHandle){
    
    RC rc = -99;
    
    if (fHandle == NULL){
        printf("input error, -----appendEmptyBlock\n");
        return rc;
    }


	FILE *fp;
	char *allocData;

    allocData = (char *)malloc (PAGE_SIZE);
    
	fp=fopen(fHandle->fileName,"ab+");

    
    fwrite(allocData, PAGE_SIZE, 1, fp);
    fHandle -> totalNumPages += 1;
	free(allocData);
	fclose(fp);

    return RC_OK;
}

/***************************************************************
 * Function Name: ensureCapacity 
 *
***************************************************************/
RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle){
    
    RC rc = -99;
    if (fHandle == NULL){
        printf(" input error, ------ensureCapacity\n");
        return rc;
    }
    
	if(fHandle -> totalNumPages >= numberOfPages){
		return RC_OK;
	}
	
	FILE *fp;
	long allocCapacity;
	char *allocData;


	allocCapacity= (numberOfPages - fHandle -> totalNumPages) * PAGE_SIZE;

    allocData = (char *) malloc(allocCapacity);
	
	fp=fopen(fHandle->fileName,"ab+");
   

    
    fwrite(allocData, allocCapacity, 1, fp);
    fHandle -> totalNumPages = numberOfPages;

	free(allocData);
	fclose(fp);


    return  RC_OK;
}
