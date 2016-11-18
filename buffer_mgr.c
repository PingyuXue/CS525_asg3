#include "buffer_mgr.h"
#include <stdio.h>
#include <stdlib.h>
#include "dberror.h"
#include "storage_mgr.h"


// Buffer Manager Interface Pool Handling

/***************************************************************
 * Function Name: initBufferPool
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

/***************************************************************
 * Function Name: initPages
 ***************************************************************/
RC initPages(BM_BufferPool *const bm, const int numPages) {
    int i = 0;
//    for (i = 0; i < numPages; i++)
    while (i < numPages)
    {
        (bm->mgmtData + i)->fixCount = 0;
        (bm->mgmtData + i)->dirty = 0;
        (bm->mgmtData + i)->pageNum = -1;
        (bm->mgmtData + i)->data = NULL;
        i++;
    }
    return RC_OK;
}

/***************************************************************
 * Function Name: shutdownBufferPool
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
***************************************************************/

RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
    int index = seekPage(bm, page);
    (bm->mgmtData + index)->fixCount -= 1;
    return RC_OK;
}

/***************************************************************
 * Function Name: forcePage
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
***************************************************************/

RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page,
            const PageNumber pageNum)
{
    
    RC rc = -99;
    
    if (bm == NULL){
        printf("input error, ------pinpage\n");
        return rc ;
    }
    
    
    int pnum = 0;
    int flag = 0;
    
    int i = 0;
    while (i < bm->numPages)
    {
        int transNum = (bm->mgmtData + i)->pageNum;
//        if ((bm->mgmtData + i)->pageNum == -1)
        if (transNum == -1)
        {
            (bm->mgmtData + i)->data = (char*)calloc(PAGE_SIZE, sizeof(char));
            pnum = i;
            flag = 1;
            break;
        }
        
//        if ((bm->mgmtData + i)->pageNum == pageNum)
        if (transNum == pageNum)
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
            // change to switch
            ReplacementStrategy transStrat = bm->strategy;
            switch ( transStrat) {
                case RS_FIFO:
                {
                    pnum = strategyFIFOandLRU(bm);
                    
                    if ((bm->mgmtData + pnum)->dirty)
                        forcePage (bm, bm->mgmtData + pnum);
                    break;
                }
                    
                case  RS_LRU:
                {
                    pnum = strategyFIFOandLRU(bm);
                    if ((bm->mgmtData + pnum)->dirty)
                        forcePage (bm, bm->mgmtData + pnum);
                    break;
                    
                }
                    
                default:
                    break;
            }
//            if (bm->strategy == RS_FIFO)
//            {
//                pnum = strategyFIFOandLRU(bm);
//                if ((bm->mgmtData + pnum)->dirty)
//                    forcePage (bm, bm->mgmtData + pnum);
//            }
//            if (bm->strategy == RS_LRU)
//            {
//                pnum = strategyFIFOandLRU(bm);
//                if ((bm->mgmtData + pnum)->dirty)
//                    forcePage (bm, bm->mgmtData + pnum);
//            }
        }
        i++;
    }
    
//     change to switch
        switch (flag){
            case 1:
            {
                FILE* fp;
                fp = fopen(bm->pageFile, "r");
                
                fseek(fp, pageNum * PAGE_SIZE, SEEK_SET);
                
                fread((bm->mgmtData + pnum)->data, sizeof(char), PAGE_SIZE, fp);
                
                page->data = (bm->mgmtData + pnum)->data;
                bm->numRead++;
                ((bm->mgmtData + pnum)->fixCount)++;
                (bm->mgmtData + pnum)->pageNum = pageNum;
//                page->fixCount = (bm->mgmtData + pnum)->fixCount;
//                page->pageNum = pageNum;
//                page->dirty = (bm->mgmtData + pnum)->dirty;
//                page->strategyType = (bm->mgmtData + pnum)->strategyType;
                getPageDetail(bm, page, pageNum, pnum);
                rc = updataAttribute(bm, bm->mgmtData + pnum);
                if (rc != RC_OK){
                    printf("set error, ------pinpage()\n");
                    return rc;
                }
                fclose(fp);
                break;
            }
                case 2:
            {
                page->data = (bm->mgmtData + pnum)->data;
                ((bm->mgmtData + pnum)->fixCount)++;
//                page->fixCount = (bm->mgmtData + pnum)->fixCount;
//                page->pageNum = pageNum;
//                page->dirty = (bm->mgmtData + pnum)->dirty;
//                page->strategyType = (bm->mgmtData + pnum)->strategyType;
                getPageDetail(bm, page, pageNum,pnum);
                break;
            }
        }
    
    return RC_OK;
}

RC getPageDetail (BM_BufferPool *const bm, BM_PageHandle *const page,
            const PageNumber pageNum,int pnum){
    
    page->fixCount = (bm->mgmtData + pnum)->fixCount;
    page->dirty = (bm->mgmtData + pnum)->dirty;
    page->pageNum = pageNum;
    page->strategyType = (bm->mgmtData + pnum)->strategyType;
    return RC_OK;
}

// Statistics Interface

/***************************************************************
 * Function Name: getFrameContents
 *
***************************************************************/
PageNumber *getFrameContents (BM_BufferPool *const bm) {
    
    
    PageNumber *trans = (PageNumber*)calloc(bm->numPages, sizeof(PageNumber));
    BM_PageHandle *handle = bm->mgmtData;

    int i =0;
    int numpage = bm->numPages;
//    for (i = 0; i < bm->numPages; i++) {
    while(i < numpage){
        if ((handle + i)->data == NULL) {
            trans[i] = NO_PAGE;
        } else {
            trans[i] = (handle + i)->pageNum;
        }
        i++;
    }
    return trans;
}

/***************************************************************
 * Function Name: getDirtyFlags
 *
 *
***************************************************************/
bool *getDirtyFlags (BM_BufferPool *const bm) {
    
    RC rc = -99;
    if (bm == NULL){
        printf("input error, ------getDirtyFlag\n");
        return rc;
    }
    
    bool *arr = (bool*)calloc(bm->numPages , sizeof(bool));
    
    BM_PageHandle *handle = bm->mgmtData;

    int i = 0 ;
    int page = bm->numPages;
//    for (i = 0; i < bm->numPages; i++) {
    while ( i< page ){
        arr[i] = (handle + i)->dirty;
        i++;
    }
    
    return arr;
}

/***************************************************************
 * Function Name: getFixCounts
 *
***************************************************************/
int *getFixCounts (BM_BufferPool *const bm) {
    
    RC rc = -99;
    
    if ( bm == NULL){
        printf("input error, -----getFixCount\n");
        return rc;
    }
    
    int *arr = (int*)malloc(bm->numPages * sizeof(int));
    BM_PageHandle *handle = bm->mgmtData;

    int i =0;
    int pageNum = bm->numPages;
//    for (i = 0; i < bm->numPages; i++) {
    while (i <pageNum){
        arr[i] = (handle + i)->fixCount;
        i++;
    }
    
    return arr;
}

/***************************************************************
 * Function Name: getNumReadIO
 *
***************************************************************/
int getNumReadIO (BM_BufferPool *const bm) {
    return bm->numRead;
}

/***************************************************************
 * Function Name: getNumWriteIO
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
 ****************************************************************/

int strategyFIFOandLRU(BM_BufferPool *bm) {
    
    RC rc = -99;
    
    if (bm == NULL){
        printf("input error, ------strategy\n");
        return rc;
    }
    
    int * attributes;
    int * fixCounts;
    int min = bm->time;
    int abortPage = -1;
    int pagenum = bm->numPages;

    attributes = (int *)getAttributionArray(bm);
    fixCounts = getFixCounts(bm);


    
    int i = 0;
    

//    for (i = 0; i < bm->numPages; ++i) {
    while (i < pagenum){
        if (*(fixCounts + i) != 0) continue;

        if (min >= (*(attributes + i))) {
            abortPage = i;
            min = (*(attributes + i));
        }
        i++;
    }

    if ((bm->time) > 35000) {
        (bm->time) -= min;
    
        int j =0;
//        for (i = 0; i < bm->numPages; ++i) {
        while ( j< pagenum){
            *(bm->mgmtData->strategyType) -= min;
            j++;
        }
    }
    return abortPage;
}

/***************************************************************
 * Function Name: getAttributionArray
 *
***************************************************************/

int *getAttributionArray(BM_BufferPool *bm) {
    
 
    RC rc = -99;
    if (bm == NULL){
        printf("input error, ------getAttributionArray\n");
        return rc;
    }
    
    int *attributes;
    int *flag;
    int i = 0;
    int pageNum = bm->numPages;
    attributes = (int *)calloc(bm->numPages, sizeof((bm->mgmtData)->strategyType));
    
//    for (i = 0; i < bm->numPages; ++i) {
    while (i < pageNum ){
        flag = attributes + i;
        *flag = *((bm->mgmtData + i)->strategyType);
        i++;
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
 * Author:
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
