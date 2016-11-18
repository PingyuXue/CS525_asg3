#include "buffer_mgr.h"
#include <stdio.h>
#include <stdlib.h>
#include "dberror.h"
#include "storage_mgr.h"

/*
 // Replacement Strategies
typedef enum ReplacementStrategy {
  RS_FIFO = 0,
  RS_LRU = 1,
  RS_CLOCK = 2,
  RS_LFU = 3,
  RS_LRU_K = 4
} ReplacementStrategy;

// Data Types and Structures
typedef int PageNumber;
#define NO_PAGE -1

typedef struct BM_BufferPool {
  char *pageFile;
  int numPages;
  ReplacementStrategy strategy;
  void *mgmtData; // use this one to store the bookkeeping info your buffer
                  // manager needs for a buffer pool
  int numReadIO;
  int numWriteIO;
  int timer;
} BM_BufferPool;

typedef struct BM_PageHandle {
  PageNumber pageNum;
  char *data;
  bool dirty;
  int fixCounts;
  void* strategyAttribute;
} BM_PageHandle;

 */


// Buffer Manager Interface Pool Handling

/***************************************************************
 * Function Name: initBufferPool
 *
 * Description: initBufferPool creates a new buffer pool with numPages page frames using the page replacement strategy strategy. The pool is used to cache pages from the page file with name pageFileName. Initially, all page frames should be empty. The page file should already exist, i.e., this method should not generate a new page file. stratData can be used to pass parameters for the page replacement strategy. For example, for LRU-k this could be the parameter k.
 *
 * Parameters: BM_BufferPool *const bm, const char *const pageFileName, const int numPages, ReplacementStrategy strategy, void *stratData.
 *
 * Return: RC
 *
 * Author: Xiaoliang Wu
 *
 * History:
 *      Date            Name                        Content
 *      16/02/24        Xiaoliang Wu                Not init pageHandle.
 *  02/27/16        Zhipeng Liu         add some init
***************************************************************/

RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,
                  const int numPages, ReplacementStrategy strategy,
                  void *stratData) {

    FILE *fp = fopen(pageFileName, "r");
    if (fp == NULL) {
        return RC_FILE_NOT_FOUND;
    }
    fclose(fp);
    
    bm->numPages = numPages;
    bm->strategy = strategy;
    bm->pageFile = (char *)pageFileName;
    bm->mgmtData = (BM_PageHandle *)calloc(numPages, sizeof(BM_PageHandle));
    
    initPages(bm, numPages);
    
    bm->numRead = 0;
    bm->numWrite = 0;
    bm->time = 0;
    
    return RC_OK;
}

RC initPages(BM_BufferPool *const bm, const int numPages) {
    int i;
    for (i = 0; i < numPages; i++)
    {
        (bm->mgmtData + i)->fixCount = 0;
        (bm->mgmtData + i)->dirty = 0;
        (bm->mgmtData + i)->pageNum = -1;
        (bm->mgmtData + i)->data = NULL;
    }
    return RC_OK;
}

/***************************************************************
 * Function Name: shutdownBufferPool
 *
 * Description: shutdownBufferPool destroys a buffer pool. This method should free up all resources associated with buffer pool. For example, it should free the memory allocated for page frames. If the buffer pool contains any dirty pages, then these pages should be written back to disk before destroying the pool. It is an error to shutdown a buffer pool that has pinned pages.
 *
 * Parameters: BM_BufferPool *const bm
 *
 * Return: RC
 *
 * Author: Xiaoliang Wu
 *
 * History:
 *      Date            Name                        Content
 *      16/02/24        Xiaoliang Wu                Complete.
 *      16/02/26        Xiaoliang Wu                Free buffer in pages.
 *      16/02/27        Xincheng Yang               Free fixCounts.
 *
***************************************************************/


RC shutdownBufferPool(BM_BufferPool *const bm) {
    int *counts = getFixCounts(bm);
    int i = 0; 
    
    while (i < bm->numPages) {
        if (*(counts + i)) {
            free(counts);
            return RC_SHUTDOWN_POOL_FAILED;
        }
        i++;
    }

    forceFlushPool(bm);
    
    //Free pageBuffer
    for (i = 0; i < bm->numPages; i++) {
        free((bm->mgmtData + i)->data); 
        free((bm->mgmtData + i)->strategyType);
    }
    
    free(counts);
    free(bm->mgmtData);
    
    return RC_OK;
}

/***************************************************************
 * Function Name: forceFlushPool
 *
 * Description: forceFlushPool causes all dirty pages (with fix count 0) from the buffer pool to be written to disk.
 *
 * Parameters: BM_BufferPool *const bm
 *
 * Return: RC
 *
 * Author: Xiaoliang Wu
 *
 * History:
 *      Date            Name                        Content
 *      16/02/25        Xiaoliang Wu                Complete, forcepage need set dirty to 0.
 *      16/02/27        Xincheng Yang               free fixCounts and dirtyFlags.
 *
***************************************************************/

RC forceFlushPool(BM_BufferPool *const bm) {
    
    bool *dirtys = getDirtyFlags(bm);
    int *fixs = getFixCounts(bm);

    int i = 0;
    while (i < bm->numPages) {
        if (*(dirtys + i) && *(fixs + i)) {
                continue;
        } else {
            RC RC_flag = forcePage(bm, ((bm->mgmtData) + i));
            if (RC_flag != RC_OK) {
                free(dirtys);
                free(fixs);
                return RC_flag;
            }
        }
        
        if (*(dirtys + i)) {
            ((bm->mgmtData) + i)->dirty = 0;
        }
        
        i++;
   } 

    free(dirtys);
    free(fixs);
    
    return RC_OK;
}

// Buffer Manager Interface Access Pages

/***************************************************************
 * Function Name: markDirty
 *
 * Description:mark a page as dirty
 *
 * Parameters: BM_BufferPool *const bm, BM_PageHandle *const page
 *
 * Return:RC
 *
 * Author:Zhipeng Liu
 *
 * History:
 *      Date            Name                        Content
 *      02/25/16        Zhipeng Liu                 complete
***************************************************************/

int seekPage (BM_BufferPool *const bm, BM_PageHandle *const page) {
    int index;
    for (index = 0; index < (bm->numPages); index++)
    {
        if ((bm->mgmtData + index)->pageNum == page->pageNum)
        {
            return index;
        }
    }
    return -1;
}

RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page)
{
    int index = seekPage(bm, page);
    page->dirty = 1;
    (bm->mgmtData + index)->dirty = 1;
    return RC_OK;
}

/***************************************************************
 * Function Name: unpinPage
 *
 * Description:unpin a page indicate by "page"
 *
 * Parameters:BM_BufferPool *const bm, BM_PageHandle *const page
 *
 * Return:RC
 *
 * Author:Zhipeng Liu
 *
 * History:
 *      Date            Name                        Content
 *      02/25/16        Zhipeng Liu                 complete
***************************************************************/

RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
    int index = seekPage(bm, page);
    (bm->mgmtData + index)->fixCount -= 1;
    return RC_OK;
}

/***************************************************************
 * Function Name: forcePage
 *
 * Description:write the current content of the page back to the page file on disk
 *
 * Parameters:BM_BufferPool *const bm, BM_PageHandle *const page
 *
 * Return:RC
 *
 * Author:Zhipeng Liu
 *
 * History:
 *      Date            Name                        Content
 *  02/16/2016  Zhipeng Liu        finish the function
***************************************************************/

RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
    FILE *fp = fopen(bm->pageFile, "rb+");
    fseek(fp, PAGE_SIZE*(page->pageNum), SEEK_SET);
    
    fwrite(page->data, PAGE_SIZE, 1, fp);
    
    (bm->numWrite) += 1;
    fclose(fp);
    
    int index = seekPage(bm, page);
    (bm->mgmtData + index)->dirty = 0;

    page->dirty = 0;

    return RC_OK;
}

/***************************************************************
 * Function Name: pinPage
 *
 * Description:pin a page, if the page does not exist in memory, read it from file
 *
 * Parameters:BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum
 *
 * Return:RC
 *
 * Author:Zhipeng Liu
 *
 * History:
 *      Date            Name                        Content
 *02/25/16       Zhipng Liu             imcomplete, need to implement the replace page part
***************************************************************/

RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page,
            const PageNumber pageNum)
{
    int pnum = 0;
    int flag = 0;
    
    int i = 0;
    while (i < bm->numPages)
    {
        if ((bm->mgmtData + i)->pageNum == -1)
        {
            (bm->mgmtData + i)->data = (char*)calloc(PAGE_SIZE, sizeof(char));
            pnum = i;
            flag = 1;
            break;
        }
        
        if ((bm->mgmtData + i)->pageNum == pageNum)
        {
            pnum = i;
            flag = 2;
            if (bm->strategy == RS_LRU)
                updataAttribute(bm, bm->mgmtData + pnum);
            break;
        }
        
        if (i == bm->numPages - 1)
        {

            flag = 1;
            if (bm->strategy == RS_FIFO)
            {
                pnum = strategyFIFOandLRU(bm);
                if ((bm->mgmtData + pnum)->dirty)
                    forcePage (bm, bm->mgmtData + pnum);
            }
            if (bm->strategy == RS_LRU)
            {
                pnum = strategyFIFOandLRU(bm);
                if ((bm->mgmtData + pnum)->dirty)
                    forcePage (bm, bm->mgmtData + pnum);
            }
        }
        i++;
    }
    
    if (flag == 1)
    {
        FILE* fp;
        fp = fopen(bm->pageFile, "r");
        fseek(fp, pageNum * PAGE_SIZE, SEEK_SET);
        fread((bm->mgmtData + pnum)->data, sizeof(char), PAGE_SIZE, fp);
        page->data = (bm->mgmtData + pnum)->data;
        bm->numRead++;
        ((bm->mgmtData + pnum)->fixCount)++;
        (bm->mgmtData + pnum)->pageNum = pageNum;
        page->fixCount = (bm->mgmtData + pnum)->fixCount;
        page->pageNum = pageNum;
        page->dirty = (bm->mgmtData + pnum)->dirty;
        page->strategyType = (bm->mgmtData + pnum)->strategyType;
        updataAttribute(bm, bm->mgmtData + pnum);
        fclose(fp);
    }
    if (flag == 2)
    {
        page->data = (bm->mgmtData + pnum)->data;
        ((bm->mgmtData + pnum)->fixCount)++;
        page->fixCount = (bm->mgmtData + pnum)->fixCount;
        page->pageNum = pageNum;
        page->dirty = (bm->mgmtData + pnum)->dirty;
        page->strategyType = (bm->mgmtData + pnum)->strategyType;
        //if(bm->strategy==RS_LRU)
        //  updataAttribute(bm, bm->mgmtData+pnum);
    }
    return RC_OK;
}

// Statistics Interface

/***************************************************************
 * Function Name: getFrameContents
 *
 * Description: Returns an array of PageNumbers where the ith element is the number of the page stored in the ith page frame
 *
 * Parameters: BM_BufferPool *const bm
 *
 * Return: PageNumber *
 *
 * Author: Xincheng Yang
 *
 * History:
 *      Date            Name                        Content
 *   2016/2/27      Xincheng Yang             first time to implement the function
 *
***************************************************************/
PageNumber *getFrameContents (BM_BufferPool *const bm) {
    PageNumber *arr = (PageNumber*)malloc(bm->numPages * sizeof(PageNumber));
    BM_PageHandle *handle = bm->mgmtData;

    int i;
    for (i = 0; i < bm->numPages; i++) {
        if ((handle + i)->data == NULL) {
            arr[i] = NO_PAGE;
        } else {
            arr[i] = (handle + i)->pageNum;
        }
    }
    return arr;
}

/***************************************************************
 * Function Name: getDirtyFlags
 *
 * Description: Returns an array of bools where the ith element is TRUE if the page stored in the ith page frame is dirty
 *
 * Parameters: BM_BufferPool *const bm
 *
 * Return: bool *
 *
 * Author: Xincheng Yang
 *
 * History:
 *      Date            Name                        Content
 *   2016/2/27      Xincheng Yang             first time to implement the function
 *
***************************************************************/
bool *getDirtyFlags (BM_BufferPool *const bm) {
    bool *arr = (bool*)malloc(bm->numPages * sizeof(bool));
    BM_PageHandle *handle = bm->mgmtData;

    int i;

    for (i = 0; i < bm->numPages; i++) {
        arr[i] = (handle + i)->dirty;
    }
    return arr;
}

/***************************************************************
 * Function Name: getFixCounts
 *
 * Description: Returns an array of ints where the ith element is the fix count of the page stored in the ith page frame
 *
 * Parameters: BM_BufferPool *const bm
 *
 * Return: int *
 *
 * Author: Xincheng Yang
 *
 * History:
 *      Date            Name                        Content
 *   2016/2/27      Xincheng Yang             first time to implement the function
 *
***************************************************************/
int *getFixCounts (BM_BufferPool *const bm) {
    int *arr = (int*)malloc(bm->numPages * sizeof(int));
    BM_PageHandle *handle = bm->mgmtData;

    int i;
    for (i = 0; i < bm->numPages; i++) {
        arr[i] = (handle + i)->fixCount;
    }
    return arr;
}

/***************************************************************
 * Function Name: getNumReadIO
 *
 * Description: Returns an array of ints where the ith element is the fix count of the page stored in the ith page frame
 *
 * Parameters: BM_BufferPool *const bm
 *
 * Return: int
 *
 * Author: Xincheng Yang
 *
 * History:
 *      Date            Name                        Content
 *   2016/2/27      Xincheng Yang             first time to implement the function
 *
***************************************************************/
int getNumReadIO (BM_BufferPool *const bm) {
    return bm->numRead;
}

/***************************************************************
 * Function Name: getNumWriteIO
 *
 * Description: Returns an array of ints where the ith element is the fix count of the page stored in the ith page frame
 *
 * Parameters: BM_BufferPool *const bm
 *
 * Return: int
 *
 * Author: Xincheng Yang
 *
 * History:
 *      Date            Name                        Content
 *   2016/2/27      Xincheng Yang             first time to implement the function
 *
***************************************************************/
int getNumWriteIO (BM_BufferPool *const bm) {
    return bm->numWrite;
}

/***************************************************************
 * Function Name: strategyFIFOandLRU
 *
 * Description: decide use which frame to save data using FIFO.
 *
 * Parameters: BM_BufferPool *bm
 *
 * Return: int
 *
 * Author: Xiaoliang Wu
 *
 * History:
 *      Date            Name                        Content
 *      16/02/27        Xiaoliang Wu                Complete
 *
***************************************************************/

int strategyFIFOandLRU(BM_BufferPool *bm) {
    int * attributes;
    int * fixCounts;
    int i;
    int min, abortPage;

    attributes = (int *)getAttributionArray(bm);
    fixCounts = getFixCounts(bm);

    min = bm->time;
    abortPage = -1;

    for (i = 0; i < bm->numPages; ++i) {
        if (*(fixCounts + i) != 0) continue;

        if (min >= (*(attributes + i))) {
            abortPage = i;
            min = (*(attributes + i));
        }
    }

    if ((bm->time) > 32000) {
        (bm->time) -= min;
        for (i = 0; i < bm->numPages; ++i) {
            *(bm->mgmtData->strategyType) -= min;
        }
    }
    return abortPage;
}

/***************************************************************
 * Function Name: getAttributionArray
 *
 * Description: return an array that includes all pages strategyAttribute.
 *
 * Parameters: BM_BufferPool *bm
 *
 * Return: int*
 *
 * Author: Xiaoliang Wu
 *
 * History:
 *      Date            Name                        Content
 *      16/02/27        Xiaoliang Wu                Complete.
 *
***************************************************************/

int *getAttributionArray(BM_BufferPool *bm) {
    int *attributes;
    int *flag;
    int i;

    attributes = (int *)calloc(bm->numPages, sizeof((bm->mgmtData)->strategyType));
    for (i = 0; i < bm->numPages; ++i) {
        flag = attributes + i;
        *flag = *((bm->mgmtData + i)->strategyType);
    }
    return attributes;
}

/***************************************************************
 * Function Name: updataAttribute
 *
 * Description: modify the attribute about strategy. FIFO only use this function when page initial. LRU use this function when pinPage occurs.
 *
 * Parameters: BM_BufferPool *bm, BM_PageHandle *pageHandle
 *
 * Return: RC
 *
 * Author: Xiaoliang Wu
 *
 * History:
 *      Date            Name                        Content
 *      16/02/26        Xiaoliang Wu                FIFO, LRU complete.
 *
***************************************************************/

RC updataAttribute(BM_BufferPool *bm, BM_PageHandle *pageHandle) {
    // initial page strategy attribute assign buffer
    if (pageHandle->strategyType == NULL) {

        if (bm->strategy == RS_FIFO || bm->strategy == RS_LRU) {
            pageHandle->strategyType = calloc(1, sizeof(int));
        }

    }

    // assign number
    if (bm->strategy == RS_FIFO || bm->strategy == RS_LRU) {
        int * attribute;
        attribute = (int *)pageHandle->strategyType;
        *attribute = (bm->time);
        (bm->time)++;
        return RC_OK;
    }

    return RC_STRATEGY_NOT_FOUND;
}
