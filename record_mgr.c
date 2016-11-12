#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "dberror.h"
#include "record_mgr.h"
#include "tables.h"
#include "expr.h"


size_t sizeint = sizeof(int );
size_t sizechar = sizeof(char );
size_t sizebool = sizeof(bool );
size_t sizefloat = sizeof(float );

// url: cs525/ former-code/ test / combina1/ conbin1/ compare-master
/***************************************************************
 * Function Name: initRecordManager
 *
 * Description: initial record manager
 *
 * Parameters: void *mgmtData
 *
 * Return: RC
 *
 * Author: Pingyu Xue
 *
 * History:
 *      Date            Name                        Content
 *   11/5/2016    Pingyu Xue <pxue2@hawk.iit.edu>   create
 *   11/8/2016    Pingyu Xue <pxue2@hawk.iit.edu>   rewrite
***************************************************************/

RC initRecordManager (void *mgmtData) {
    
    printf("-----start initial record manager -----\n");
//    RC flag  =-99;
//    
//    if (mgmtData == NULL){
//        printf("init erro, in put error, in initRecordManager\n");
//        return flag;
//    }
//    
//    BM_BufferPool *bm = MAKE_POOL();
//    
//    ReplacementStrategy strategy = RS_LRU;
//    
//    int numpage = 10;
//
//    flag = initBufferPool(bm, "", numpage, strategy, NULL);
//    
//    if (flag !=RC_OK){
//        printf("error init buffer pool , in ininRecordManager\n");
//        return flag;
//    }
    
    return RC_OK;
}

/***************************************************************
 * Function Name: shutdownRecordManager
 *
 * Description: shutdown record manager
 *
 * Parameters: NULL
 *
 * Return: RC
 *
 * Author: Pingyu Xue
 *
 * History:
 *      Date            Name                        Content
 *   11/5/2016    Pingyu Xue <pxue2@hawk.iit.edu>   create
 *   11/8/2016    Pingyu Xue <pxue2@hawk.iit.edu>   rewrite
***************************************************************/

RC shutdownRecordManager () {
    printf("-----shut down record manager -----\n");
    return RC_OK;
}

/***************************************************************
 * Function Name: createTable
 *
 * Description: Creating a table should create the underlying page file and store information about the schema, free-space, ... and so on in the Table Information pages
 *
 * Parameters: char *name, Schema *schema
 *
 * Return: RC
 *
 * Author: Pingyu Xue
 *
 * History:
 *      Date            Name                        Content
 *   11/5/2016    Pingyu Xue <pxue2@hawk.iit.edu>   create
 *   11/5/2016    Pingyu Xue <pxue2@hawk.iit.edu>   rewrite
 *   11/6/2016    pingyu Xue <pxue2@hawk.iit.edu>   rewrite
 *   11/8/2016    pingyu Xue <pxue2@hawk.iit.edu>   rewrite
 *   11/9/2016    pingyu Xue <pxue2@hawk.iit.edu>   modify
***************************************************************/

RC createTable (char *name, Schema *schema) {

    RC flag =- 99;
    
    if( name == NULL || schema == NULL){
        printf("null input, error\n");
        return flag;
    }
    
    SM_FileHandle filehandle;  //Q1: why not char *?
    
    // file_meta_data;
    int slot_size;
    int record_num;
    int record_size;
    int get_file_storage_size; // get the size of the schema and load the extra size
    int file_meta_data_size; //file meta data size
    
    
        // transfer :
    char * input_infor = (char *) malloc (sizechar * PAGE_SIZE);
    char * schema_data = serializeSchema(schema);
    
    int writeposition = 1;
    
    
    // create pagefile
    flag = createPageFile(name);
    if (flag != RC_OK){
        printf("create page file eror, in createTable ()\n");
        return flag;
    }
    
    flag = -99;
    flag = openPageFile(name, &filehandle);
    if (flag != RC_OK){
        printf("openpage file eror , in creteTable()\n");
        return RC_OK;
    }

    
        //     get informatino for the filemeta data
        //     file_meta_data has the lenth of the schema's attr
        //        and also have the record_num, record_size, slot_size in the contents
        //        its for the meta contrl

    // 4 size of the int number is to set the location for the attribute in the file meta data.
    int extra_size = (int) (4 * sizeint);
    record_num = 0;
    slot_size = SLOT_SIZE;
    record_size = getRecordSize(schema) / slot_size ;
    
    get_file_storage_size = (int) (strlen(serializeSchema(schema)) + extra_size);

    if (get_file_storage_size % PAGE_SIZE == 0){
        file_meta_data_size = (get_file_storage_size / PAGE_SIZE);
    }
    else{
        file_meta_data_size = (get_file_storage_size / PAGE_SIZE) + 1;
    }
    
    ensureCapacity(file_meta_data_size, &filehandle);
    
        // transfer data into input_infor and write into block
    memmove(input_infor, &file_meta_data_size, sizeint);
    memmove(input_infor + sizeint, &record_size, sizeint);
    memmove(input_infor + (2 * sizeint), &slot_size, sizeint);
    memmove(input_infor + (3 * sizeint), &record_num, sizeint);
    
        // get schema infor and input into the
        //** file_meta_data_tr and extra_size is make easy to read
    
    
    int file_meta_data_tr = PAGE_SIZE - extra_size;  // PAGE_SIZE - 4 * sizeint
    
    
        // scheam data is less than the page size or not
    if (strlen(schema_data) < file_meta_data_tr){
        memmove(input_infor + extra_size , schema_data, strlen(schema_data));
        writeBlock(0, &filehandle, input_infor);
        free(input_infor);
    }
    
    else {
        memmove(input_infor + extra_size, schema_data, file_meta_data_tr);
        writeBlock(0, &filehandle, input_infor);
        free(input_infor);
    
        // reload input:
        for (; writeposition < file_meta_data_size; writeposition++){
                // calloc 0 page of the
                input_infor = (char *) calloc (PAGE_SIZE, sizeint);
    
                if ( writeposition != (file_meta_data_size - 1)){
                    memmove(input_infor, schema_data + writeposition * PAGE_SIZE, PAGE_SIZE);
                }
                // last page input
                else {
                    memmove(input_infor, schema_data + writeposition * PAGE_SIZE, strlen(schema_data + writeposition * PAGE_SIZE));
                }
                writeBlock(writeposition, &filehandle, input_infor);
                free(input_infor);
            }
            
        }
//        Page_Meta_Data(&filehandle);
    Page_Meta_Data(&filehandle);
    
        // create table done
    closePageFile(&filehandle);
    
    return  RC_OK;

}

/***************************************************************
 * Function Name: openTable
 *
 * Description: open a table
 *
 * Parameters: RM_TableData *rel, char *name
 *
 * Return: RC
 *
 * Author: Pingyu Xue
 *
 * History:
 *      Date            Name                        Content
 *   11/5/2016    Pingyu Xue <pxue2@hawk.iit.edu>   create
 *   11/5/2016    Pingyu Xue <pxue2@hawk.iit.edu>   rewrite
 *   11/6/2016    pingyu Xue <pxue2@hawk.iit.edu>   rewrite
 *   11/8/2016    pingyu Xue <pxue2@hawk.iit.edu>   rewrite
 *   11/9/2016    pingyu Xue <pxue2@hawk.iit.edu>   modify
***************************************************************/

RC openTable (RM_TableData *rel, char *name) {
    RC rc = -99;
        if( rel == NULL || name == NULL){
            printf("input error \n");
            return rc;
        }
    
    // set dbm_cr schema struct for the schema
    Schema *scheam_cr;
    int numAttr_cr;
    char ** attrNames_cr;
    DataType *dataTypes_cr;
    int *typeLength_cr;
    int *keyAttrs_cr;
    int keysize_cr;
    
    char * flag;
    char * mark;
    mark = flag;
    // offset for the locate record:
    int offset = 9;
        // create bufferpool pagehandle
    BM_PageHandle *pagehandle = MAKE_PAGE_HANDLE();
    BM_BufferPool *bufferpool = MAKE_POOL();
    SM_FileHandle *filehandle = (SM_FileHandle *) malloc ( sizeof(SM_FileHandle));
    
        // filemetadata size & schema_data
    int file_meta_data_size;
    char * schema_data;
    
    
    // flag for input:
    char *flag2;
    
    char *temp_trans = (char *) calloc(20, sizechar); // temp transfor data; 20 free space
    
    
        // init buffer pool
    openPageFile(name, filehandle);
    
    initBufferPool(bufferpool, name, 20, RS_LRU, NULL);
    file_meta_data_size = File_Meta_Data_Size(bufferpool);
    
    // get char schema data:
    int i;
    for (i=0; i< file_meta_data_size; ++i){
        // get data
        pinPage(bufferpool, pagehandle, i);
        // free request
        unpinPage(bufferpool, pagehandle);
        }
    
    schema_data = pagehandle->data + (4 *sizeint);
    
    
    flag = strchr(schema_data, '<');
    flag++;
    
    
        // base on the test rm_serializer file
        // get the numAttr number:
    for(i=0;;++i){
    
        if ( *(flag + i) == '>'){
            break;
        }
    
        temp_trans[i] = flag[i];
    }
    
    numAttr_cr = atoi(temp_trans);
        free(temp_trans);
    
        //give the value of the other attribute in the schema:
    attrNames_cr = (char **) malloc( numAttr_cr * sizeof(char *));
    dataTypes_cr = (DataType *) malloc (numAttr_cr * sizeof(DataType));
    typeLength_cr = (int *) malloc (numAttr_cr * sizeint);
    
        // flag move on:
    flag = strchr(flag, '(');
    
    flag++;
    
    int j;  // change for the 1st loop
    int k;
 
        for(i=0; i< numAttr_cr; i++){
            for (j=0;;j++){
//            while (j++){
    
                if ( *(flag + j) == ':'){
                    attrNames_cr[i] = (char *) malloc (j * sizechar);
                    memmove(attrNames_cr[i], flag, j);
                    switch ( *(flag + j + 2)) {
                        case 'I':
                            dataTypes_cr[i] = DT_INT;
                            typeLength_cr[i] = 0;
                            break;
                        case 'B':
                            dataTypes_cr[i] = DT_BOOL;
                            typeLength_cr[i] = 0;
                            break;
                        case 'F':
                            dataTypes_cr[i] = DT_FLOAT;
                            typeLength_cr[i] = 0;
                            break;
                        case 'S':
                            dataTypes_cr[i]= DT_STRING;
    
                            // coun the value
                            temp_trans = (char *)malloc (40 * sizechar);
    
                            for (k=0;;k++){
                                // not consider;
                                if ( *(flag + k + offset) == ']'){ // ?
                                    break;
                                }
                                temp_trans[k] = flag [k +j + offset]; //?
                            }
    
                            typeLength_cr [i] = atoi(temp_trans);
                            free(temp_trans);
                            break;
                    }
                    if ( i == numAttr_cr -1){
                        break;
                    }
    
                    flag = strchr(flag, ',');
                    flag += 2;
                    break;
                }
            }
        }

//    int set (char **attrNames_cr, DataType *dataTypes_cr , int * typeLength_cr, char *temp_trans , char * flag, int numAttr_cr)

    //func2 done:
    
    flag = strchr(flag,'(');
    flag ++;

    flag2 = flag;
    
    // get key size and return valeu
    getkeysize(flag2, keyAttrs_cr);

    keyAttrs_cr = (int *) malloc(keysize_cr * sizeint);
    
    // get key attr and return value
    getKeyAttr(keysize_cr, flag, attrNames_cr, keyAttrs_cr);
    
    // create scheme
    scheam_cr = createSchema(numAttr_cr, attrNames_cr, dataTypes_cr, typeLength_cr, keysize_cr, keyAttrs_cr);
    
    // set file meta table infor.
    setTableMeta(rel, name, bufferpool, filehandle, scheam_cr);
        
    return RC_OK;

}

/***************************************************************
 * Function Name: closeTable
 *
 * Description: close a table
 *
 * Parameters: RM_TableData *rel
 *
 * Return: RC
 *
 * Author: Pingyu Xue
 *
 * History:
 *      Date            Name                        Content
 *   11/5/2016    Pingyu Xue <pxue2@hawk.iit.edu>   create
 *   11/8/2016    pingyu Xue <pxue2@hawk.iit.edu>   modify
 *   11/9/2016    pingyu Xue <pxue2@hawk.iit.edu>   add comment
***************************************************************/

RC closeTable (RM_TableData *rel) {

    RC flag = -99 ;
    
    if ( rel == NULL){
        printf("input table error, close table()\n");
        return flag;
    }
    
    rel->name = NULL;
    rel->schema = NULL;
    
    shutdownBufferPool(rel->bm);
    
    // no need to free those
//    free(rel->bm);
//    free(rel->fh);
//    freeSchema(rel->schema);
    
        
    return RC_OK;
    
}

/***************************************************************
 * Function Name: deleteTable
 *
 * Description: delete a table
 *
 * Parameters: char *name
 *
 * Return: RC
 *
 * Author: Pingyu Xue
 *
 * History:
 *      Date            Name                        Content
 *   11/5/2016    Pingyu Xue <pxue2@hawk.iit.edu>   create
***************************************************************/

RC deleteTable (char *name) {

    RC flag = -99;
    
    if ( name == NULL){
        printf("input error, deletetable\n");
        return flag;
    }
    
    flag = destroyPageFile(name);
    
    if (flag != RC_OK){
        printf("delete table error\n");
        return flag;
    }
    
    printf("table has delete\n");
    return RC_OK;
}

/***************************************************************
 * Function Name: getNumTuples
 *
 * Description: get the number of record
 *
 * Parameters: RM_TableData *rel
 *
 * Return: int
 *
 * Author: Pingyu Xue
 *
 * History:
 *      Date            Name                        Content
 *   11/5/2016    Pingyu Xue <pxue2@hawk.iit.edu>   create
 *   11/9/2016    pingyu Xue <pxue2@hawk.iit.edu>   rewrite
***************************************************************/

int getNumTuples (RM_TableData *rel) {
    
    int numTuples = getTotalRecords_slot(rel);
    
    return numTuples;
}

/***************************************************************
 * Function Name: insertRecord
 *
 * Description: insert a record.
 *
 * Parameters: RM_TableData *rel, Record *record
 *
 * Return: RC
 *
 * Author: Pingyu Xue
 *
 * History:
 *      Date            Name                        Content
 *   11/5/2016    Pingyu Xue <pxue2@hawk.iit.edu>   create
 *   11/7/2016    Pingyu Xue <pxue2@hawk.iit.edu>   rewrite
 *   11/9/2016    Pingyu Xue <pxue2@hawk.iit.edu>   rewrite
 *   11/11/2016    Pingyu Xue <pxue2@hawk.iit.edu>  fix  error
***************************************************************/
RC insertRecord (RM_TableData *rel, Record *record) {
    BM_PageHandle *pagehandle = MAKE_PAGE_HANDLE();
    int recordsize = getRecordSize(rel->schema);
    int pageMetadataIndx = File_Meta_Data_Size(rel->bm);
    int slotnum = (recordsize + sizeof(bool)) / 256 + 1;
    int offset = 0;
    int recordCurNum, numTuples;
    bool statement = true;
    bool check = true;
    
    // Find out the target page and slot at the end.
    //func1:
    findTargetPage(rel->bm, pagehandle, pageMetadataIndx);
    //func1 done:
    
    // Find out the target meta index and record number of the page.
    //func 2
    //
    do {
        memmove(&recordCurNum, pagehandle->data + offset + sizeint, sizeint);
        offset += 2*sizeint;
    } while (recordCurNum == PAGE_SIZE / SLOT_SIZE);
    
    // If no page exist, add new page.
    if(recordCurNum < 0){
//        // func 5: add new page
//        // If page mata is full, add new matadata block.
        if(offset == PAGE_SIZE){
            
            // Link into new meta data page.
            memmove(pagehandle->data + PAGE_SIZE - sizeint, &rel->fh->totalNumPages, sizeint);
            Page_Meta_Data(rel->fh);
            markDirty(rel->bm, pagehandle);
            unpinPage(rel->bm, pagehandle);      // Unpin the last meta page.
            pinPage(rel->bm, pagehandle, rel->fh->totalNumPages-1);  // Pin the new page.
            offset = (int) (2 * sizeint);
        }
        memmove(pagehandle->data + offset - 2*sizeint, &rel->fh->totalNumPages, sizeint);  // set page number.
        appendEmptyBlock(rel->fh);
//        addnewPage(pagehandle, rel, offset);
        recordCurNum = 0;
    }
     // func 5 done
    
//    findRecordIndex(rel, pagehandle, offset, recordCurNum);
    // func 2 done:
    
    // func4 read recordfrom the file
    readRecord(rel, pagehandle, record, slotnum, offset, recordCurNum, recordsize);
    // func4 done
    
    // Tuple number add 1.
    //func3: Tuple number add
    updateTumpleInfor(rel, pagehandle, numTuples);
    //func3 done:
    
    free(pagehandle);
    
    return RC_OK;

}

/***************************************************************
 * Function Name: deleteRecord
 *
 * Description: delete a record by id
 *
 * Parameters: RM_TableData *rel, RID id
 *
 * Return: RC
 *
 * Author: Pingyu Xue
 *
 * History:
 *      Date            Name                        Content
 *   11/5/2016    Pingyu Xue <pxue2@hawk.iit.edu>   create
 *   11/7/2016    Pingyu Xue <pxue2@hawk.iit.edu>   rewrite
 *   11/9/2016    pingyu Xue <pxue2@hawk.iit.edu>   modify and init
***************************************************************/
RC deleteRecord (RM_TableData *rel, RID id) {

    //xpy
    RC flag = -99;
    
    if (rel == NULL){
        printf( "input error \n");
        return flag;
    }
    

    BM_PageHandle *pagehandle = (BM_PageHandle *) malloc (sizeof(BM_PageHandle));
    
    int spaceoffset = SLOT_SIZE* id.slot;

    int record_size = getRecordSize(rel->schema);
    int numtup;
    char *nullstring = (char *)malloc((sizebool + record_size ) * sizechar);
    
    flag = pinPage(rel->bm, pagehandle, id.page);
    
    if( flag != RC_OK){
        printf ("check pinpage in deletepage() 1 \n");
        return flag;
    }
    // change to memmove
    memmove(pagehandle->data +spaceoffset , nullstring, sizebool+record_size);

    markDirty(rel->bm, pagehandle);
    
    flag = -99;
    flag = unpinPage(rel->bm, pagehandle);
    if (flag != RC_OK){
        printf("check unpinpage in DELETEpage() 1 \n");
    }
    
    flag = pinPage(rel->bm, pagehandle, 0);
    
    if(flag != RC_OK){
        printf(" pin page error, in deleteRecord\n");
        return flag;
    }
    
    memmove(&numtup, pagehandle->data + 3 *sizeint, sizeint);
    
    numtup -= 1;
    
    memmove(pagehandle->data + 3* sizeint, &numtup, sizeint);
    markDirty(rel->bm, pagehandle);
    
    flag = -99;
    flag = unpinPage(rel->bm, pagehandle);
    
    if (flag != RC_OK){
        printf("unpin error , in deleteRecord\n");
        return flag;
    }
        
    free(nullstring);
    free(pagehandle);
        
    return RC_OK;
}

/***************************************************************
 * Function Name: updateRecord
 *
 * Description: update a record by its id
 *
 * Parameters: RM_TableData *rel, Record *record
 *
 * Return: RC
 *
 * Author: Pingyu Xue
 *
 * History:
 *      Date            Name                        Content
 *   11/5/2016    Pingyu Xue <pxue2@hawk.iit.edu>   create
 *   11/5/2016    Pingyu Xue <pxue2@hawk.iit.edu>   init
 *   11/8/2016    Pingyu Xue <pxue2@hawk.iit.edu>   add comment
 *   11/9/2016    pingyu Xue <pxue2@hawk.iit.edu>   modify error from the testfile
***************************************************************/
RC updateRecord (RM_TableData *rel, Record *record) {
    
    // xpy:
    RC flag = -99;
    
    if ( rel == NULL || record == NULL){
        printf("input error, in updateRecord");
        return flag;
    }
    
    int spaceOffset = SLOT_SIZE * record->id.slot;
    
    BM_PageHandle *pagehandle = (BM_PageHandle *)malloc (sizeof(BM_PageHandle));
    
    
    int record_size = getRecordSize(rel->schema);
    
    flag = pinPage(rel->bm, pagehandle, record->id.page);
    if (flag != RC_OK){
        printf("check pinpage in updateRecord()\n");
        return flag;
    }
    
    markDirty(rel->bm, pagehandle);
    
    memmove(pagehandle->data + spaceOffset + sizebool, record->data, record_size);
    
    
    
    flag = -99;
    flag = unpinPage(rel->bm, pagehandle);
    
    if( flag != RC_OK){
        printf("check unpinpage in UpdateRecord\n");
        return flag;
    }
        
    free(pagehandle);
    
    return RC_OK;
        
    
}

/***************************************************************
 * Function Name: getRecord
 *
 * Description: get a record by id
 *
 * Parameters: RM_TableData *rel, RID id, Record *record
 *
 * Return: RC
 *
 * Author: Pingyu Xue
 *
 * History:
 *      Date            Name                        Content
 *   11/5/2016    Pingyu Xue <pxue2@hawk.iit.edu>   create
 *   11/8/2016    Pingyu Xue <pxue2@hawk.iit.edu>   fix logic error
***************************************************************/
RC getRecord (RM_TableData *rel, RID id, Record *record) {
    // xpy:
        RC flag = -99;

        BM_PageHandle *bmPageHandle = MAKE_PAGE_HANDLE();
    
        int spaceOffset = SLOT_SIZE * id.slot;
        int record_size = getRecordSize(rel->schema);
    
        bool record_statment;
    
        record->id = id;
    
        flag = pinPage(rel->bm, bmPageHandle, id.page);
    
        if (flag != RC_OK){
            printf("wrong pin, check pinpage in getRecord\n");
            return flag;
        }

        memmove(&record_statment, bmPageHandle->data + spaceOffset, sizebool);
    record->data = (char *)malloc (record_size);

        flag = -99;
    
        if (record_statment == true){
            
            memmove(record->data, bmPageHandle->data + spaceOffset + sizebool, record_size );
    
            flag = unpinPage(rel->bm, bmPageHandle);
    
            if (flag != RC_OK){
                printf("check unpingpage in getRecord() \n");
                return flag;
            }
            
            free(bmPageHandle);
            return RC_OK;
        }
        else{
            free(bmPageHandle);
            return flag;
        }

}

/***************************************************************
 * Function Name:startScan
 *
 * Description:initialize a scan by the parameters
 *
 * Parameters:RM_TableData *rel, RM_ScanHandle *scan, Expr *cond
 *
 * Return:RC
 *
 * Author: Pingyu Xue
 *
 * History:
 *      Date            Name                        Content
 *   11/5/2016    Pingyu Xue <pxue2@hawk.iit.edu>   create
***************************************************************/

RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{
    
    // xpy:
    RC flag = -99;
    
    if ( rel == NULL ){
        printf("input table error, in startScan\n");
        return flag;
    }
    
    scan->rel=rel;
    
    scan->curPage=0;
    
    scan->curSlot=0;
    
    scan->expr=cond;
    
    scan->mgmtData = NULL;
    
    return RC_OK;
    
}

/***************************************************************
 * Function Name:next
 *
 * Description:do the search in the scanhanlde and return the next tuple that fulfills the scan condition in parameter "record"
 *
 * Parameters:RM_ScanHandle *scan, Record *record
 *
 * Return:RC
 *
 * Author: Pingyu Xue
 *
 * History:
 *      Date            Name                        Content
 *   11/5/2016    Pingyu Xue <pxue2@hawk.iit.edu>   create
 *   11/8/2016    Pingyu Xue <pxue2@hawk.iit.edu>   rewrite
 *   11/8/2016    Pingyu Xue <pxue2@hawk.iit.edu>   fix eror
 *   11/9/2016    Pingyu Xue <pxue2@hawk.iit.edu>   rewrite
 *   11/10/2016    Pingyu Xue <pxue2@hawk.iit.edu>  rewrite and split into two function;
***************************************************************/

RC next (RM_ScanHandle *scan, Record *record) 
{
 
    RC flag = -99;
    
    if (scan == NULL || record == NULL){
        printf("input error, ------next()\n");
        return flag;
    }
    
    // set normal attribute
    int maxslot,page;
    BM_BufferPool *bufferpool = scan->rel->bm;
    
    // init
    RID rid;
    Record *record_replace=(Record *)malloc (sizeof(Record));
    Value *value=(Value *)malloc (sizeof(Value));
    BM_PageHandle *pagehandle= MAKE_PAGE_HANDLE();
    

    int recordSize= (int) (((getRecordSize (scan->rel->schema)+sizebool)/SLOT_SIZE )+1);
    int index = File_Meta_Data_Size(bufferpool);
    
    pinPage(bufferpool,pagehandle,index);


    // keep seaching the record which need be find
    while (1){
        if (scan->curPage != index)
        {
            memmove(&page,pagehandle->data+(scan->curPage)* 2 * sizeint, sizeint);
            memmove(&maxslot, pagehandle->data + ((scan->curPage) * 2+ 1) * sizeint, sizeint);
        
            if(maxslot!=-1)
            {
                int offset = scan->curSlot;
                while (offset < maxslot)
                {
                    rid.page=page;
                    rid.slot=offset*recordSize;
                
                    flag = getRecord(scan->rel,rid,record_replace);
                    if(flag == RC_OK)
                    {
                        evalExpr (record_replace, scan->rel->schema, scan->expr,&value);;
                        if(value->v.boolV)
                        {
                        // func1 :setrecord:
                            setRecord(scan, record, rid, record_replace, maxslot, offset); //
                        // func1  done
                            free(value);
                            free(record_replace);
                            unpinPage (bufferpool, pagehandle);
                            free(pagehandle);
                            return RC_OK;
                        }
                    }
                    offset++;
                }
            }
        }
        
        else
        {
            unpinPage(bufferpool,pagehandle);
            printf("cannot find right page\n");
            free(pagehandle);
            return RC_RM_NO_MORE_TUPLES;
        }
        scan->curPage++;
    }
}


/***************************************************************
 * Function Name: closeScan
 *
 * Description: free the memo space of this scan
 *
 * Parameters: RM_ScanHandle *scan
 *
 * Return: RC
 *
 * Author: Pingyu Xue
 *
 * History:
 *      Date            Name                        Content
 *   11/5/2016    Pingyu Xue <pxue2@hawk.iit.edu>   create
***************************************************************/

RC closeScan (RM_ScanHandle *scan)
{
//    scan->currentPage = -1;
//    scan->currentSlot = -1;
    
    scan->mgmtData = NULL;
    scan->rel = NULL;
    free(scan->rel);
    // test file : cannot free the data;
//    free(scan->expr);
    free(scan->mgmtData);
    
    // test file : cannot free the data
//    free(scan);
    
    
    return RC_OK;
}

/***************************************************************
 * Function Name: getRecordSize
 *
 * Description: get the size of record described by "schema"
 *
 * Parameters: Schema *schema
 *
 * Return: int
 *
 * Author: Pingyu Xue
 *
 * History:
 *      Date            Name                        Content
 *   11/5/2016    Pingyu Xue <pxue2@hawk.iit.edu>   create
 *   11/7/2016    Pingyu Xue <pxue2@hawk.iit.edu>   init , add comment
***************************************************************/

int getRecordSize (Schema *schema)
{
    RC flag = -99;
    
    if (schema == NULL){
        printf("input schema error , in getRecordSize");
        return flag;
    }

    int recordsize = 0;
    
    int i = 0;
    
    while(i< schema->numAttr){
        if( schema->dataTypes[i] == DT_INT){
            recordsize += sizeint;
        }
        
        else if (schema->dataTypes[i] == DT_FLOAT){
            recordsize += sizefloat;
            }
        
        else if (schema->dataTypes[i] == DT_BOOL){
            recordsize += sizebool;
        }
        
        else if (schema->dataTypes[i] == DT_STRING){
            recordsize += schema->typeLength[i];
        }
        i++;
    }
    return recordsize;
}

/***************************************************************
 * Function Name: createSchema
 *
 * Description: create a new schema described by the parameters
 *
 * Parameters: int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys
 *
 * Return: Schema
 *
 * Author: Pingyu Xue
 *
 * History:
 *      Date            Name                        Content
 *   11/5/2016    Pingyu Xue <pxue2@hawk.iit.edu>   create
***************************************************************/

Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
{
    RC rc = -99;
    
    if (numAttr < 0 || attrNames == NULL || dataTypes == NULL || typeLength == 0 || keySize <0 || keys == NULL)
    {
        printf(" input error, attribute not complete input, ------createSchema()\n");
        return rc;
    }
    
    Schema *scheme_create_by_dbm = (Schema*)malloc(sizeof(Schema));

    scheme_create_by_dbm->numAttr = numAttr;
    
    scheme_create_by_dbm->attrNames = attrNames;
    
    scheme_create_by_dbm->dataTypes = dataTypes;
    
    scheme_create_by_dbm->typeLength = typeLength;
    
    scheme_create_by_dbm->keyAttrs = keys;
    
    scheme_create_by_dbm->keySize = keySize;

    return scheme_create_by_dbm;
    
}

/***************************************************************
 * Function Name: freeSchema
 *
 * Description: free the memo space of this schema
 *
 * Parameters: Schema *schema
 *
 * Return: RC
 *
 * Author: Pingyu Xue
 *
 * History:
 *      Date            Name                        Content
 *   11/5/2016    Pingyu Xue <pxue2@hawk.iit.edu>   create
***************************************************************/

RC freeSchema (Schema *schema)
{
    RC flag = -99;
    
    if (schema == NULL){
        printf("input error ! freeSchema\n");
        return flag;
    }
    
    // free pointer
    free(schema->dataTypes);
    free(schema->keyAttrs);
    free(schema->typeLength);
    
    
    schema->keySize = -1;
    

    int i=0;
    
    // free all the schema->attrNames[]
    while (i< schema->numAttr){
        free(schema->attrNames[i]);
        i++;
    }

    // set numAttr will be a cannot use number
    schema->numAttr = -1;
    
    // free point and schema
    free(schema->attrNames);
    free(schema);
        
    return RC_OK;
}

/***************************************************************
 * Function Name: createRecord
 *
 * Description: Create a record by the schema
 *
 * Parameters: Record *record, Schema *schema
 *
 * Return: RC
 *
 * Author: Pingyu Xue
 *
 * History:
 *      Date            Name                        Content
 *   11/5/2016    Pingyu Xue <pxue2@hawk.iit.edu>   create
***************************************************************/
RC createRecord (Record **record, Schema *schema) {
     *record = (Record *)malloc( sizeof(Record));
    
    // init record:
    (*record)->data = (char*)malloc(getRecordSize(schema) *sizechar);
    
    (*record)->id.slot = -1;
    
    (*record)->id.page = -1;

    return RC_OK;
}

/***************************************************************
 * Function Name: freeRecord
 *
 * Description: Free the space of a record
 *
 * Parameters: Record *record
 *
 * Return: RC
 *
 * Author: Pingyu Xue
 *
 * History:
 *      Date            Name                        Content
 *   11/5/2016    Pingyu Xue <pxue2@hawk.iit.edu>   create
***************************************************************/
RC freeRecord (Record *record) {
    free(record->data);
    
    record->id.page = -1;
    
    record->id.slot = -1;
    
    free(record);

    return RC_OK;
}

/***************************************************************
 * Function Name: getAttr
 *
 * Description: Get the value of a record
 *
 * Parameters: Record *record, Schema *schema, int attrNum, Value **value
 *
 * Return: RC, value
 *
 * Author: Pingyu Xue
 *
 * History:
 *      Date            Name                        Content
 *   11/5/2016    Pingyu Xue <pxue2@hawk.iit.edu>   create
 *   11/8/2016    Pingyu Xue <pxue2@hawk.iit.edu>   rewrite
 *   11/9/2016    Pingyu Xue <pxue2@hawk.iit.edu>   modify, fix error about the space offset
***************************************************************/
RC getAttr (Record *record, Schema *schema, int attrNum, Value **value) {
    //xpy:
    
    RC flag = - 99;
    
    if ( record == NULL || schema == NULL || value == NULL){
        printf("error input \n");
        return flag;
    }
        int attrOffset = 0;
    
        *value = (Value *)malloc (sizeof(Value));
    
//    char * endf = '\0';
    
        int i = 0;
    
    // init the attr type and offset of the attribute
        while (i<attrNum){
            if ( schema->dataTypes[i] == DT_INT){
                attrOffset += sizeint;
            }
    
            else if( schema->dataTypes[i] == DT_BOOL){
                attrOffset += sizebool;
            }
    
            else if (schema->dataTypes[i] == DT_FLOAT){
                attrOffset += sizefloat;
            }
            else if (schema->dataTypes[i] == DT_STRING){
                attrOffset += schema->typeLength[i];
            }
            i++;
        }
    
        (*value)->dt = schema->dataTypes[attrNum];
    
    
    while (true){
        if (schema->dataTypes [attrNum] == DT_INT){
            memmove(&((*value)->v.intV), record->data + attrOffset, sizeof(int));
            break;
        }
    
        else if (schema->dataTypes[attrNum] == DT_BOOL){
            memmove(&((*value)->v.boolV), record->data + attrOffset, sizeof(int));
            break;
        }
    
        else if (schema->dataTypes[attrNum] == DT_FLOAT){
            memmove(&((*value)->v.floatV), record->data + attrOffset, sizeof(float));
            break;
        }
    
        else if (schema->dataTypes[attrNum] == DT_STRING){
            (*value)->v.stringV = (char *)malloc ( schema->typeLength[attrNum] +1);
            memmove((*value)->v.stringV, record->data + attrOffset, schema->typeLength[attrNum]);

            char  endf = '\0';
            memmove((*value)->v.stringV + schema->typeLength[attrNum], &endf, 1);
            
            break;
        }
    }
        return RC_OK;
    
}

/***************************************************************
 * Function Name: setAttr
 *
 * Description: Set the value of a record
 *
 * Parameters: Record *record, Schema *schema, int attrNum, Value *value
 *
 * Return: RC
 *
 * Author: Pingyu Xue
 *
 * History:
 *      Date            Name                        Content
 *   11/5/2016    Pingyu Xue <pxue2@hawk.iit.edu>   create
 *   11/5/2016    Pingyu Xue <pxue2@hawk.iit.edu>   rewrite
 *   11/8/2016    Pingyu Xue <pxue2@hawk.iit.edu>   rewrite
 *   11/8/2016    Pingyu Xue <pxue2@hawk.iit.edu>   add commentand init , fix the memmove error
 *   11/11/2016    Pingyu Xue <pxue2@hawk.iit.edu>   fix error
***************************************************************/
RC setAttr (Record *record, Schema *schema, int attrNum, Value *value) {

    RC flag = -99;
    
    //init basic data;
    if ( record == NULL || schema == NULL || value == NULL){
        printf("error input, none value\n");
        return flag;
    }
    
    int attrOffset = 0;
    int i = 0;
    
    //init basic data;
    while (i < attrNum){
            // add while(1)
        while (1){
            if (schema->dataTypes[i] == DT_INT){
                attrOffset += sizeint;
                break;
            }
            else if( schema->dataTypes[i] == DT_BOOL){
                attrOffset += sizebool;
                break;
            }
            
            else if( schema->dataTypes[i] == DT_FLOAT){
                attrOffset += sizefloat;
                break;
            }
                
            else if( schema->dataTypes[i] == DT_STRING){
                attrOffset += schema->typeLength[i];
                break;
            }
            break;
                
        }
        i++;
    }
    
//    detect type and copy info
    while (1){
        if (schema->dataTypes[attrNum] == DT_INT){
            memmove(record->data + attrOffset, &(value->v.intV), sizeint);
            break;
        }
        
        else if ( schema->dataTypes[attrNum] == DT_BOOL){
            memmove(record->data + attrOffset, &(value->v.boolV), sizeint);
            break;
        }
        
        else if( schema->dataTypes[attrNum] == DT_FLOAT){
            memmove(record->data + attrOffset, &(value->v.floatV), sizefloat);
            break;
        }
        
        else if (schema->dataTypes[attrNum] == DT_STRING){
            if (strlen(value->v.stringV) >= schema->typeLength[attrNum]){

                memmove(record->data + attrOffset, value->v.stringV, schema->typeLength[attrNum]);
                break;
            }
            else{
                strcpy(record->data + attrOffset, value->v.stringV);
                break;
            }
        }
        
    }
    
    return RC_OK;
}

/***************************************************************
 * Function Name: addPageMetadataBlock
 *
 * Description: add block that contain pages metadata
 *
 * Parameters: SM_FileHandle *fh
 *
 * Return: RC
 *
 * Author: Pingyu Xue
 *
 * History:
 *      Date            Name                        Content
 *   11/5/2016    Pingyu Xue <pxue2@hawk.iit.edu>   create
 *   11/5/2016    Pingyu Xue <pxue2@hawk.iit.edu>   rewrite
 *   11/10/2016    Pingyu Xue <pxue2@hawk.iit.edu>  fix error
***************************************************************/
RC Page_Meta_Data(SM_FileHandle *fh) {
    RC rc = -99;

    int pageNum = fh->totalNumPages;
    int pagecapacity  = -1;
    char * pageMetadataInput =  (char *)malloc(PAGE_SIZE *sizechar); ;
    int pageMetadataNum = (int)(PAGE_SIZE / ( 2 * sizeint));

    
    rc = appendEmptyBlock(fh);
    if (rc != RC_OK) {
        rc = closePageFile(fh);
        return rc;
    }

    int offset = 0;
    while (offset < pageMetadataNum){
        memmove(pageMetadataInput + offset * 2 * sizeint, &pageNum, sizeint);
        memmove(pageMetadataInput + offset * 2 * sizeint + sizeint, &pagecapacity, sizeint);
        pageNum++;
        
        if (offset == pageMetadataNum - 1) {
            pageNum = fh->totalNumPages - 1;
        }
        offset++;
    }
    rc = writeBlock(fh->totalNumPages - 1, fh, pageMetadataInput);
    
    if (rc != RC_OK) {
        printf("resutl error, ------pageMetaData()\n");
        return rc;
    }

    free(pageMetadataInput);
    return RC_OK;
}
//


// additional by xpy:
/***************************************************************
 * Function Name: File_Meta_Data_Size
 *
 * Description: get  basic information for the table
 *
 * Parameters: BM_BufferPool *bm
 *
 * Return: int
 *
***************************************************************/
int File_Meta_Data_Size(BM_BufferPool *bm) {
    
    // xpy:
    RC flag = -99;
    if ( bm == NULL){
        printf("input error , in file_meta_data_size\n");
        return flag;
    }
    
    int file_data_size;
    
    //    BM_PageHandle *pagehandle = (BM_PageHandle *)malloc (sizeof(BM_PageHandle));
    BM_PageHandle *pagehandle = MAKE_PAGE_HANDLE();
    
    
    flag = pinPage(bm, pagehandle, 0);
    
    if (flag != RC_OK){
        printf("pinpage error , int file_meta_data_size\n");
        return flag;
    }
    
    memmove(&file_data_size, pagehandle->data, sizeint);
    
    flag = -99;
    flag = unpinPage(bm, pagehandle);
    if (flag != RC_OK){
        printf("uppinpage error , int file_meta_data_size\n");
        return flag;
    }
    
    free(pagehandle);
    
    return file_data_size;
}


/***************************************************************
 * Function Name: getTotalRecords_slot
 *
 * Description: get tuples number in the table.
 *
 * Parameters: BM_BufferPool *bm
 *
 * Return: int
 *
 ***************************************************************/
int getTotalRecords_slot( RM_TableData *rel){

    RC flag = -99;
    int numTumples;

    BM_PageHandle *bmPageHandle = MAKE_PAGE_HANDLE();
//    bmPageHandle = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));


    flag = pinPage(rel->bm, bmPageHandle, 0);

    if ( flag != RC_OK){
        printf(" wrong, in getTotalPage() \n");
        return flag;
    }

    memcmp(&numTumples, bmPageHandle->data + ( 3 * sizeint), sizeint);
    
    flag = unpinPage(rel->bm, bmPageHandle);
    
    if ( flag != RC_OK){
        printf(" wrong, in getTotalPage() \n");
        return flag;
    }

    free(bmPageHandle);

    return numTumples;

}

/***************************************************************
 * Function Name: modifyFileMetaPage
 *
 * Description: get tuples number in the table.
 *
 * Parameters: BM_BufferPool *bm
 *
 * Return: int
 *
 ***************************************************************/

/************ Table create ******************/

//Schema *scheam_cr;
//int numAttr_cr;
//char ** attrNames_cr;
//DataType *dataTypes_cr;
//int *typeLength_cr;
//int *keyAttrs_cr;
//int keysize_cr;
//
//char * flag;
//BM_PageHandle *pagehandle = MAKE_PAGE_HANDLE();
//BM_BufferPool *bufferpool = MAKE_POOL();
//SM_FileHandle *filehandle = (SM_FileHandle *) malloc ( sizeof(SM_FileHandle));
//
//// filemetadata size & schema_data
//int file_meta_data_size;
//char * schema_data;
//char *flag2;
//char *temp_trans = (char *) calloc(20, sizechar);


// func 2
// para: attrName, flag dataType, typylength , temp-trans
int set(char **attrNames_cr, DataType *dataTypes_cr , int * typeLength_cr, char *temp_trans , char * flag, int numAttr_cr)
{
    
    int i,j,k;
    int offset = 9;
    for(i=0; i< numAttr_cr; i++){
        for (j=0;;j++){
        //            while (j++){
        
        if ( *(flag + j) == ':'){
            attrNames_cr[i] = (char *) malloc (j * sizechar);
            memmove(attrNames_cr[i], flag, j);
            switch ( *(flag + j + 2)) {
                case 'I':
                    dataTypes_cr[i] = DT_INT;
                    typeLength_cr[i] = 0;
                    break;
                case 'B':
                    dataTypes_cr[i] = DT_BOOL;
                    typeLength_cr[i] = 0;
                    break;
                case 'F':
                    dataTypes_cr[i] = DT_FLOAT;
                    typeLength_cr[i] = 0;
                    break;
                case 'S':
                    dataTypes_cr[i]= DT_STRING;
                    
                    // coun the value
                    temp_trans = (char *)malloc (40 * sizechar);
                    
                    for (k=0;;k++){
                        // not consider;
                        if ( *(flag + k + offset) == ']'){ // ?
                            break;
                        }
                        temp_trans[k] = flag [k +j + offset]; //?
                    }
                    
                    typeLength_cr [i] = atoi(temp_trans);
                    free(temp_trans);
                    break;
            }
            if ( i == numAttr_cr -1){
                break;
            }
            
            flag = strchr(flag, ',');
            flag += 2;
            break;
        }
    }
}
return RC_OK;
}
//func3:
int getkeysize(char *input, int keysize){
    
    int i =0;
    
    while(1){
        input = strchr(input, ',');
        if (input == NULL){
            break;
        }
        input++;
        i++;
    }
    i++;
    
    keysize = i;
    return RC_OK;
}

//func3 done

// func 4:
int getKeyAttr( int key_size , char * input, char **attrNames, char *keyAttrs)
{
    int i =0;
    int j =0;
    int k =0;
    char *trans = (char *)malloc(100 * sizechar);
    
    RC rc = -99;
    if (key_size <= 0){
        printf("in put key_size error ---- getKeyAttr()\n");
        return rc;
    }
    
    for  (; i<key_size ; i++){
        // while
        for (;;j++){
            if ((*(input + j) == ',') || (*(input + j) == ')')) {
                
                memmove(trans, input, j);
                for (k = 0; k < key_size; k++) {
                    
                    if (strcmp(trans,attrNames[k]) == 0) {
                        keyAttrs[i] = k; // assign keyAttrs
                        free(trans);
                        break;
                    }
                }
                if (*(input + j) == ',') {
                    input = input + j + 2;
                }
                else {
                    input = input + j;
                }
                break;
            }
        }
    }
    return RC_OK;
}
//func 4 done:

// func 2:
//   getTypeLength(numAttr_cr, dataTypes_cr, attrNames_cr, flag, typeLength_cr);
int getTypeLength (int numAttr, DataType *dataTypes, char **attrNames, char *input, int *typeLength)
{
    RC rc = -99;
    if (numAttr < 0 || dataTypes == NULL){
        printf("input error, -----getTypeLength\n");
        return rc;
    }
    
    int i = 0;
    int j = 0;
    int k;

    while (i< numAttr){
//        for(j=0;;j++){
        while (1){
//            if ( *(input + j) == ':'){
            if ( *(input + j) == ':'){
                attrNames[i] = (char *) malloc (j * sizechar);
                memmove(attrNames[i], input, j);  //change to memove
                switch ( *(input + j + 2)) {
                    case 'I':
                        dataTypes[i] = DT_INT;
                        typeLength[i] = 0;
                        break;
                    case 'B':
                        dataTypes[i] = DT_BOOL;
                        typeLength[i] = 0;
                        break;
                    case 'F':
                        dataTypes[i] = DT_FLOAT;
                        typeLength[i] = 0;
                        break;
                    case 'S':
                        dataTypes[i]= DT_STRING;

                        char *temp = (char *) malloc (40 * sizechar);
                        
                        for (k=0;;k++){
                        // not consider;
                            if ( *(input + k + 9) == ']'){ // ?
                                break;
                            }
                            temp[k] = input [k +j + 9]; //?
                        }
                    
                        typeLength[i] = atoi(temp);
                        free(temp);
                        break;
                }
                if ( i == numAttr -1){
                    break;
                }
            
                input = strchr(input, ',');
                input += 2;
                break;
            }
            j++;
        }
    }
    return RC_OK;
}

// func5 :
int setTableMeta( RM_TableData *rel, char *name, BM_BufferPool *bufferpool, SM_FileHandle * filehandle, Schema * scheam){
    
    // check the value input or not
    RC flag = -99;
    
    if ( name == NULL || bufferpool == NULL || filehandle == NULL || scheam == NULL){
        printf("input error , -----setTableMeta()\n");
        return flag;
    }
    
    rel->name = name;
    
    rel->bm = bufferpool;
    
    rel->fh = filehandle;
    
    rel->schema = scheam;
    
   
    // func5 done:
    
    return RC_OK;

}
/*************************opentable done:  *******************************/




/*********************** insert record func:*****************************/
// func1: find target page:
// para: rel->bm, h->data, pageMetadatIndx,
// return
int findTargetPage(BM_BufferPool *bufferpool, BM_PageHandle *pageHandle, int Index){
    RC rc  = -99;
    
    if (Index < 0 || bufferpool == NULL || pageHandle == NULL){
        printf("input error, ------findTargetPage\n");
        return rc;
    }
    
    while(1){
        pinPage(bufferpool, pageHandle, Index);

        memmove(&Index, pageHandle->data + PAGE_SIZE - sizeint, sizeint);

        if (Index != -1){
            unpinPage(bufferpool, pageHandle);
        }
        else {
            break;
        }
    }
    
    return RC_OK;
}

// Find out the target meta index and record number of the page.
// func2: find index
// para: pagehandle, offset(int), recordcurNum, rel->filehandle, rel->bm)
int findRecordIndex(RM_TableData *rel, BM_PageHandle *pagehandle, int offset, int curNum)
{
    RC rc = -99;
    
    if (rel == NULL || curNum < 0 ){
        printf("input error\n");
        return rc;
    }
    
    //replace:
      do {
            memmove(&curNum, pagehandle->data + offset + sizeof(int), sizeof(int));
            offset += 2*sizeof(int);
        } while (curNum == PAGE_SIZE / 256);
    
    //     If no page exist, add new page.
        if(curNum == -1){
            // If page mata is full, add new matadata block.
            if(offset == PAGE_SIZE){
                memmove(pagehandle->data + PAGE_SIZE - sizeof(int), &rel->fh->totalNumPages, sizeof(int));   // Link into new meta data page.
                Page_Meta_Data(rel->fh);
                markDirty(rel->bm, pagehandle);
                unpinPage(rel->bm, pagehandle);      // Unpin the last meta page.
                rc = pinPage(rel->bm, pagehandle, rel->fh->totalNumPages-1);  // Pin the new page.
                offset = 2*sizeof(int);
            }
    
//            if(check){
                memmove(pagehandle->data + offset - 2*sizeof(int), &rel->fh->totalNumPages, sizeof(int));  // set page number.
    
                appendEmptyBlock(rel->fh);
                curNum = 0;
//            }
            
        }
    
    return RC_OK;
}


// func: unpdata
int updateTumpleInfor( RM_TableData *rel, BM_PageHandle * pagehandle, int numTumples){
    
//    printf("here is going to update the tumple\n");
    
    RC flag = -99;
    
    // need to update withe 3 size of int
    int extra_size = (int) (3 * sizeint);
    
    if(rel->bm == NULL || pagehandle == NULL){
        return flag;
    }
    
    flag = pinPage(rel->bm, pagehandle, 0);
    
    if(flag == RC_OK){
        //    memmove(&numTumples, pagehandle->data + 3 * sizeof(int), sizeof(int));

        memmove(&numTumples, pagehandle->data + extra_size, sizeint);
        numTumples++;
        
        memmove(pagehandle->data + extra_size, &numTumples, sizeint);
        
        markDirty(rel->bm, pagehandle);
        
        unpinPage(rel->bm, pagehandle);
        
        return RC_OK;
    }
    
    return flag;
}

// func4:  // read record
int readRecord(RM_TableData *rel, BM_PageHandle *pagehandle ,Record *record, int slotnum, int offset, int recordCurNum , int recordsize)
{
    // do not put in statment;
    bool statement  = true;
    
    memmove(&record->id.page, pagehandle->data + offset - 2*sizeof(int), sizeof(int));   // Set record->id page number.
    record->id.slot = recordCurNum * slotnum;                                // Set record->id slot.
    recordCurNum++;
    memmove(pagehandle->data + offset - sizeof(int), &recordCurNum, sizeof(int));   // Set record number++ into meta data.
    markDirty(rel->bm, pagehandle);
    unpinPage(rel->bm, pagehandle);              // unpin meta page.
    
    // Insert record header and record data into page.
    pinPage(rel->bm, pagehandle, record->id.page);
    memmove(pagehandle->data + 256*record->id.slot, &statement, sizeof(bool));   // Record header is a del_flag
    memmove(pagehandle->data + 256*record->id.slot + sizeof(bool), record->data, recordsize); // Record body is values.
    markDirty(rel->bm, pagehandle);
    unpinPage(rel->bm, pagehandle);

    return RC_OK;
}

// func 5: add new page
// If page mata is full, add new matadata block.
int addnewPage( BM_PageHandle *pagehandle, RM_TableData *rel ,int offset )
{
    if(offset == PAGE_SIZE){
    // Link into new meta data page.

        memmove(pagehandle->data + PAGE_SIZE - sizeof(int), &rel->fh->totalNumPages, sizeof(int));
        Page_Meta_Data(rel->fh);
        markDirty(rel->bm, pagehandle);
        unpinPage(rel->bm, pagehandle);      // Unpin the last meta page.
        pinPage(rel->bm, pagehandle, rel->fh->totalNumPages-1);  // Pin the new page.
        offset = 2*sizeof(int);
    }
    memmove(pagehandle->data + offset - 2*sizeof(int), &rel->fh->totalNumPages, sizeof(int));  // set page number.
    appendEmptyBlock(rel->fh);

    return RC_OK;
}

/***************************** insert func *************************/

/****************************** next func************************/
// func1 :setrecord:
// paraater: find the right record, get the record.
// return rc_ok
int setRecord( RM_ScanHandle *scan,Record *record, RID rid , Record *tmp, int maxslot , int offset){
    
    RC rc = -99;
    
    if ( record == NULL || scan == NULL || tmp == NULL){
        printf(" input error, ------setRecord()\n");
        return rc;
    }
    
    //init the record,
    record->id.page=rid.page;
    record->id.slot=rid.slot;
    record->data=tmp->data;
    
    if(offset == maxslot-1)
    {
        scan->curPage++;
        scan->curSlot = 0;
    }
    else{
        scan->curSlot = offset + 1;
    }
    return RC_OK;
}

//***************************  next func done *******************/