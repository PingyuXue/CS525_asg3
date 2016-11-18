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
/*
typedef struct SM_FileHandle {
  char *fileName;
  int totalNumPages;
  int curPagePos;
  void *mgmtInfo;
} SM_FileHandle;

typedef char* SM_PageHandle;
*/

/************************************************************
 *                    interface                             *
 ************************************************************/
/* manipulating page files */
/*
extern void initStorageManager (void);
extern RC createPageFile (char *fileName);
extern RC openPageFile (char *fileName, SM_FileHandle *fHandle);
extern RC closePageFile (SM_FileHandle *fHandle);
extern RC destroyPageFile (char *fileName);
*/

/* reading blocks from disc */
/*
extern RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage);
extern int getBlockPos (SM_FileHandle *fHandle);
extern RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);
extern RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);
extern RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);
extern RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);
extern RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);
*/

/* writing blocks to a page file */
/*
extern RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage);
extern RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);
extern RC appendEmptyBlock (SM_FileHandle *fHandle);
extern RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle);
*/

/* manipulating page files */


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
 * Description: Create a new page file fileName. The initial file size should be one page. This method should fill this single page with '\0' bytes.
 *
 * Parameters: char *fileName
 *
 * Return: RC
 *
 * Author: Xiaoliang Wu
 *
 * History:
 *      Date            Name                        Content
 *      --------------  --------------------------  ----------------
 *      2016/02/07      Xiaoliang Wu                implement function
 *
***************************************************************/


RC createPageFile(char *fileName){
    FILE *fp;
    char fill[PAGE_SIZE];
    int write_result;

    fp = fopen(fileName,"w+");

    if(fp == NULL){
        fclose(fp);
        return RC_CREATE_FILE_FAIL;
    }

    memset(fill, '\0', sizeof(fill));
    write_result = fwrite(fill, 1, PAGE_SIZE, fp);

    if(write_result != PAGE_SIZE){
        fclose(fp);
        destroyPageFile(fileName);
        return RC_CREATE_FILE_FAIL;
    }

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

//    seek_result = fseek(fp, 0, SEEK_END);

//    if(seek_result != 0){
//        fclose(fp);
//        return RC_GET_NUMBER_OF_BYTES_FAILED;
//    }
    
    fseek(fp, 0, SEEK_END);

    size = ftell(fp);
//    if(size == -1){
//        fclose(fp);
//        return RC_GET_NUMBER_OF_BYTES_FAILED;
//    }

    fHandle->fileName = fileName;
    if(size%PAGE_SIZE == 0){
    	fHandle->totalNumPages = (size / PAGE_SIZE);
    }else{
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
//    fHandle->fileName = "";
//    fHandle->curPagePos = 0;
//    fHandle->totalNumPages = 0;
    return RC_OK;
}

/***************************************************************
 * Function Name: destroyPageFile
 * 
 * Description: destroy (delete) a page file
 *
 * Parameters: SM_FileHandle *fHandle
 *
 * Return: RC
 *
 * Author: Xiaoliang Wu 
 *
 * History:
 *      Date            Name                        Content
 *      2016/02/07      Xiaoliang Wu                implement function
 *
***************************************************************/


RC destroyPageFile (char *fileName){
//    int remove_result;
//    remove_result = remove(fileName);
//    if(remove_result == 0){
//        return RC_OK;
//    }else{
//        return RC_FILE_NOT_FOUND;
//    }
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
//	if(pageNum>fHandle->totalNumPages-1||pageNum<0)
//		return RC_READ_NON_EXISTING_PAGE;
//	else
//	{
//		FILE *fp;
//		fp=fopen(fHandle->fileName,"r");
//		int offset;
//		offset=fHandle->curPagePos*PAGE_SIZE;
//		fseek(fp,offset,SEEK_SET);
//		fread(memPage,sizeof(char),PAGE_SIZE,fp);
//		fHandle->curPagePos=pageNum;
//		fclose(fp);
//		return RC_OK;
//	}
    
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
    
    fHandle->curPagePos=pageNum;
    
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
    RC rc = -99;
    if (fHandle == NULL){
        printf("input error, -----readFirstBlock\n");
        return rc;
    }
	FILE *fp;
    
	fp=fopen(fHandle->fileName,"r");
    
	rc = fseek(fp,0,SEEK_SET);
    if (rc != 0){
        printf("seek error, ------read1stblock\n");
        return rc;
    }
    
	fread(memPage,sizeof(char),PAGE_SIZE,fp);
	fHandle->curPagePos=0;
	fclose(fp);
	return RC_OK;
}

/***************************************************************
 * Function Name:readPreviousBlock
***************************************************************/


RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    RC rc = -99;
    if (fHandle == NULL){
        printf("input error, ------readPreviousBlock\n");
        return rc;
    }
    
//	if(fHandle->curPagePos<=0||fHandle->curPagePos>fHandle->totalNumPages-1)
//		return RC_READ_NON_EXISTING_PAGE;
//	else
//	{
		FILE *fp;
    
		fp=fopen(fHandle->fileName,"r");
    
		int offset=(fHandle->curPagePos-1)*PAGE_SIZE;
    
		rc = fseek(fp,offset,SEEK_SET);
    
    if (rc != 0){
        printf("seek error, -----readprevsblock\n");
        return rc;
    }
		fread(memPage,sizeof(char),PAGE_SIZE,fp);
		fHandle->curPagePos=fHandle->curPagePos-1;
		fclose(fp);
		return RC_OK;
//	}
}

/***************************************************************
 * Function Name: readCurrentBlock
 *
***************************************************************/


RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
//	if(fHandle->curPagePos<0||fHandle->curPagePos>fHandle->totalNumPages-1)
//		return RC_READ_NON_EXISTING_PAGE;
//	else
//	{
//		FILE *fp;
//		fp=fopen(fHandle->fileName,"r");
//		int offset=fHandle->curPagePos*PAGE_SIZE;
//		fseek(fp,offset,SEEK_SET);
//		fread(memPage,sizeof(char),PAGE_SIZE,fp);
//		fclose(fp);
//		return RC_OK;
//	}
    return RC_OK;
}

/***************************************************************
 * Function Name: readNextBlock
 *
***************************************************************/


RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
//	if(fHandle->curPagePos<0||fHandle->curPagePos>fHandle->totalNumPages-2)
//		return RC_READ_NON_EXISTING_PAGE;
//	else
//	{
//		FILE *fp;
//		fp=fopen(fHandle->fileName,"r");
//		int offset=(fHandle->curPagePos+1)*PAGE_SIZE;
//		fseek(fp,offset,SEEK_SET);
//		fread(memPage,sizeof(char),PAGE_SIZE,fp);
//		fHandle->curPagePos=fHandle->curPagePos+1;
//		fclose(fp);
//		return RC_OK;
//	}
    return RC_OK;
}

/***************************************************************
 * Function Name: readLastBlock
 *
***************************************************************/


RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
//	FILE *fp;
//	fp=fopen(fHandle->fileName,"r");
//	int offset=-PAGE_SIZE;
//	fseek(fp,offset,SEEK_END);
//	fread(memPage,sizeof(char),PAGE_SIZE,fp);
//	fHandle->curPagePos=fHandle->totalNumPages-1;
//	fclose(fp);
//	return RC_OK;
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
    
//    RC rv;
//    
//	if(fseek(fp,pageNum * PAGE_SIZE, SEEK_SET) != 0){
//		rv = RC_READ_NON_EXISTING_PAGE;	
//	} else if (fwrite(memPage, PAGE_SIZE, 1, fp) != 1){
//		rv = RC_WRITE_FAILED; 
//	} else {
//		fHandle->curPagePos=pageNum;		//Success write block, then curPagePos should be changed.
//		rv = RC_OK;
//	}
    
    fseek(fp,pageNum * PAGE_SIZE, SEEK_SET);
    fwrite(memPage, PAGE_SIZE, 1, fp);
    fHandle->curPagePos=pageNum;
    
	fclose(fp);
//	return rv;
    return  RC_OK;
}


/***************************************************************
 * Function Name: writeCurrentBlock 
 * 
 * Description: Write a page to disk using either the current position or an absolute position.
 *
 * Parameters: int numberOfPages, SM_FileHandle *fHandle
 *
 * Return: RC
 *
 * Author: Xincheng Yang
 *
 * History:
 *      Date            Name                        Content
 *   2016/2/2		Xincheng Yang             first time to implement the function
 *
***************************************************************/
RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
	if(fHandle == NULL){
		return RC_FILE_HANDLE_NOT_INIT;
	} 
	if(fHandle->curPagePos < 0){
		return RC_WRITE_FAILED;
	}

	return writeBlock(fHandle->curPagePos, fHandle, memPage);
}

/***************************************************************
 * Function Name: appendEmptyBlock 
 * 
 * Description: Increase the number of pages in the file by one. The new last page should be filled with zero bytes.
 *
 * Parameters: int numberOfPages, SM_FileHandle *fHandle
 *
 * Return: RC
 *
 * Author: Xincheng Yang
 *
 * History:
 *      Date            Name                        Content
 *   2016/2/1		Xincheng Yang             first time to implement the function
 *   2016/2/2		Xincheng Yang			  modified some codes
 *
***************************************************************/
RC appendEmptyBlock (SM_FileHandle *fHandle){
	if(fHandle == NULL){
		return RC_FILE_HANDLE_NOT_INIT;
	} 

	FILE *fp;
	char *allocData;
	RC rv;

	allocData = (char *)calloc(1, PAGE_SIZE);
	fp=fopen(fHandle->fileName,"ab+");

	if(fwrite(allocData, PAGE_SIZE, 1, fp) != 1)   
	{
		rv = RC_WRITE_FAILED;
	} else {
		fHandle -> totalNumPages += 1;
		rv = RC_OK;		
	}

	free(allocData);
	fclose(fp);

	return  rv;
}

/***************************************************************
 * Function Name: ensureCapacity 
 * 
 * Description: If the file has less than numberOfPages pages then increase the size to numberOfPages.
 *
 * Parameters: int numberOfPages, SM_FileHandle *fHandle
 *
 * Return: RC
 *
 * Author: Xincheng Yang
 *
 * History:
 *      Date            Name                        Content
 *   2016/2/1		Xincheng Yang             first time to implement the function
 *
***************************************************************/
RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle){
	if(fHandle == NULL){
		return RC_FILE_HANDLE_NOT_INIT;
	} 
	if(fHandle -> totalNumPages >= numberOfPages){
		return RC_OK;
	}
	
	FILE *fp;
	long allocCapacity;
	char *allocData;
	RC rv;

	allocCapacity= (numberOfPages - fHandle -> totalNumPages) * PAGE_SIZE;
	allocData = (char *)calloc(1,allocCapacity);
	
	fp=fopen(fHandle->fileName,"ab+");
   
	if(fwrite(allocData, allocCapacity, 1, fp) == 0)   
	{
		rv = RC_WRITE_FAILED;
	} else {
		fHandle -> totalNumPages = numberOfPages;		//When write success, totalNumPages should be changed to numberOfPages.	
		rv = RC_OK;
	}

	free(allocData);
	fclose(fp);

	return rv;
}
