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
 *   11/8/2016    Pingyu Xue <pxue2@hawk.iit.edu>   re write
***************************************************************/

RC initRecordManager (void *mgmtData) {
   
    // xpy
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

    
    // xpy
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

    record_num = 0;
    slot_size = SLOT_SIZE;
    record_size = getRecordSize(schema) / slot_size ;
    
    get_file_storage_size = strlen(serializeSchema(schema)) + 4* sizeint;

    if (get_file_storage_size % PAGE_SIZE == 0){
        file_meta_data_size = (get_file_storage_size / PAGE_SIZE);
    }
    else
        file_meta_data_size = (get_file_storage_size / PAGE_SIZE) + 1;
    
    ensureCapacity(file_meta_data_size, &filehandle);
    
        // transfer data into input_infor and write into block
    memmove(input_infor, &file_meta_data_size, sizeint);
    memmove(input_infor + sizeint, &record_size, sizeint);
    memmove(input_infor + (2 * sizeint), &slot_size, sizeint);
    memmove(input_infor + (3 * sizeint), &record_num, sizeint);
    
        // get schema infor and input into the
        //** file_meta_data_tr and extra_size is make easy to read
    
    int extra_size = (4 * sizeint);
    int file_meta_data_tr = PAGE_SIZE - extra_size;  // PAGE_SIZE - 4 * sizeint
    
    
        // scheam data is less than the page size or not
    if (strlen(schema_data) < file_meta_data_tr){
        memcpy(input_infor + extra_size , schema_data, strlen(schema_data));
        writeBlock(0, &filehandle, input_infor);
        free(input_infor);
    }
    
    else {
        memcpy(input_infor + extra_size, schema_data, file_meta_data_tr);
        writeBlock(0, &filehandle, input_infor);
        free(input_infor);
    
        // reload input:
        for (; writeposition < file_meta_data_size; writeposition++){
                // calloc 0 page of the
                input_infor = (char *) calloc (PAGE_SIZE, sizeint);
    
                if ( writeposition != (file_meta_data_size - 1)){
                    memcpy(input_infor, schema_data + writeposition * PAGE_SIZE, PAGE_SIZE);
                }
                // last page input
                else {
                    memcpy(input_infor, schema_data + writeposition * PAGE_SIZE, strlen(schema_data + writeposition * PAGE_SIZE));
                }
                writeBlock(writeposition, &filehandle, input_infor);
                free(input_infor);
            }
            
        }
//        Page_Meta_Data(&filehandle);
    addPageMetadataBlock(&filehandle);
    
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
    //xpy:
    RC rc = -99;
        if( rel == NULL || name == NULL){
            printf("input error \n");
            return rc;
        }
    
        Schema *scheam_cr;
        int numAttr_cr;
        char ** attrNames_cr;
        DataType *dataTypes_cr;
        int *typeLength_cr;
        int *keyAttrs_cr;
        int keysize_cr;
    
        // create bufferpool pagehandle
        BM_PageHandle *pagehandle = MAKE_PAGE_HANDLE();
        BM_BufferPool *bufferpool = MAKE_POOL();
        SM_FileHandle *filehandle = (SM_FileHandle *) malloc ( sizeof(SM_FileHandle));
    
        // filemetadata size & schema_data
        int file_meta_data_size;
        char * schema_data;
    
    
        // flag for input:
        char *flag;
    
    // trans: 40 sizeof char
        char *trans = (char *) calloc(20, sizechar); // temp transfor data;
    
    
        // init buffer pool
        openPageFile(name, filehandle);
    
    //  pagenum change to 20 rather than 10
        initBufferPool(bufferpool, name, 20, RS_LRU, NULL);
//        file_meta_data_size = File_Meta_Data_Size(bufferpool);
    file_meta_data_size = File_Meta_Data_Size(bufferpool);
    
        // get char schema data:
        int i;
        for (i=0; i< file_meta_data_size; ++i){
            pinPage(bufferpool, pagehandle, i);
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
    
            trans[i] = flag[i];
        }
    
        numAttr_cr = atoi(trans);
        free(trans);
    
        //give the value of the other attribute in the schema:
        attrNames_cr = (char **) malloc( numAttr_cr * sizeof(char *));
        dataTypes_cr = (DataType *) malloc (numAttr_cr * sizeof(DataType));
        typeLength_cr = (int *) malloc (numAttr_cr * sizeint);
    
        // flag move on:
        flag = strchr(flag, '(');
        flag++;
    
    int j;  // change for the 1st loop
    int k;
    
    
    //fucn2 : exange
        for(i=0; i< numAttr_cr; i++){
            for (j=0;;j++){
//            while (j++){
    
                if ( *(flag + j) == ':'){
                    attrNames_cr[i] = (char *) malloc (j * sizechar);
                    memcpy(attrNames_cr[i], flag, j);
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
                            trans = (char *)malloc (40 * sizechar);
    
                            for (k=0;;k++){
                                // not consider;
                                if ( *(flag + k + 9) == ']'){ // ?
                                    break;
                                }
                                trans[k] = flag [k +j + 9]; //?
                            }
    
                            typeLength_cr [i] = atoi(trans);
                            free(trans);
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

    
//    get_Type_Length(numAttr_cr, dataTypes_cr, attrNames_cr, flag, typeLength_cr);
    
        flag = strchr(flag,'(');
        flag ++;
    // func 2, done
    
    
    char * flag2;
    flag2 = flag;
    
    // func3: exange
    
//        for (i= 0;;i++){  // change to while
//    i=0;
//    while (1){
//        flag2 = strchr(flag2, ',');
//        if (flag2 == NULL){
//            break;
//        }
//    
//        flag2++;
//        i++;
//    }
    // func3:
    getkeysize(flag2, keyAttrs_cr);

    keyAttrs_cr = (int *) malloc(keysize_cr * sizeint);
    
    // func 4: exchagnge
    // delete 4: begin:
//    j = 0; //  while(1)  , but cannot pass the testfile
//    for (i =0; i< keysize_cr;i++){
////          for (j=0;;j++){
////         fuck , why?  why ? why my inin j= 0 cannot run the testfile
//        while(1){
//            if ((*(flag + j) == ',') || (*(flag + j) == ')')) {
//                trans = (char *)malloc(100 * sizechar);
//                memmove(trans, flag, j);
//                for (k = 0; k < numAttr_cr; k++) {
//    
//                    if (strcmp(trans,attrNames_cr[k]) == 0) {
//                        keyAttrs_cr[i] = k; // assign keyAttrs
//                        free(trans);
//                        break;
//                    }
//                }
//                if (*(flag + j) == ',') {
//                    flag = flag + j + 2;
//                }
//                else {
//                    flag = flag + j;
//                }
//                break;
//            }
////            j++;  // add to while
//        }
////        j++; // change to while
//        
//    }
    // delete4 done
    
    // func4:
    getKeyAttr(keysize_cr, flag, attrNames_cr, keyAttrs_cr);
    
    scheam_cr = createSchema(numAttr_cr, attrNames_cr, dataTypes_cr, typeLength_cr, keysize_cr, keyAttrs_cr);
    
    rel->name = name;
    
    rel->bm = bufferpool;
    
    rel->fh = filehandle;
    
    rel->schema = scheam_cr;
        
    //    free(pagehandle);
        
        
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
   
// xpy:
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

// xpy:
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
    // xpy:
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
 * Author: Xincheng Yang
 *
 * History:
 *      Date            Name                        Content
 *   2016/3/27      Xincheng Yang             first time to implement the function
 *
***************************************************************/
RC insertRecord (RM_TableData *rel, Record *record) {
//    BM_PageHandle *pagehandle = MAKE_PAGE_HANDLE();
//    int recordSize = getRecordSize(rel->schema);   //r_size
//    int pageMetadataIndx = File_Meta_Data_Size(rel->bm);   // pageMetadataIndx
//    int slotnum = ((recordSize + sizebool) / 256) + 1;  // r_slotnum
//    int offset = 0;
//    int recordCurNum, numTuples; // recordCurNum
//    bool statement = true;  //r_stat
//    bool check = true;
//
//    // Find out the target page and slot at the end.
////    do {｝ while (1)
//    // func1: find target page:
//    // para: rel->bm, h->data, pageMetadatIndx,
//    findTargetPage(rel->bm, pagehandle, pageMetadataIndx);
//
//    // Find out the target meta index and record number of the page.
//    // func2: find record index
//    // para: pagehandle, offset(int), recordcurNum, rel->filehandle, rel->bm)
//    do {
//        memmove(&recordCurNum, pagehandle->data + offset + sizeof(int), sizeof(int));
//        offset += 2*sizeof(int);
//    } while (recordCurNum == PAGE_SIZE / 256);
//
////     If no page exist, add new page.
//    if(recordCurNum == -1){
//        // If page mata is full, add new matadata block.
//        if(offset == PAGE_SIZE){       
//            memcpy(pagehandle->data + PAGE_SIZE - sizeof(int), &rel->fh->totalNumPages, sizeof(int));   // Link into new meta data page. 
//            addPageMetadataBlock(rel->fh);
//            markDirty(rel->bm, pagehandle);
//            unpinPage(rel->bm, pagehandle);      // Unpin the last meta page.
//            pinPage(rel->bm, pagehandle, rel->fh->totalNumPages-1);  // Pin the new page.
//            offset = 2*sizeof(int);
//        }
//        
//        if(check){
//            memmove(pagehandle->data + offset - 2*sizeof(int), &rel->fh->totalNumPages, sizeof(int));  // set page number.
//            
//            appendEmptyBlock(rel->fh);
//            recordCurNum = 0;
//        }
//        
//    }
////     func 2: done
////    findRecordIndex(rel, pagehandle, offset, recordCurNum);
////    findRecordIndex(rel->bm, rel->fh, pagehandle, offset, recordCurNum);
//
//    // Read record->id and set record number add 1 in meta data.
//    memmove(&record->id.page, pagehandle->data + offset - 2*sizeint, sizeint);   // Set record->id page number.
//    record->id.slot = recordCurNum * slotnum;                                // Set record->id slot.
//    recordCurNum++;                                
//    memmove(pagehandle->data + offset - sizeof(int), &recordCurNum, sizeof(int));   // Set record number++ into meta data.
//    
//    if(markDirty(rel->bm, pagehandle)){
//        unpinPage(rel->bm, pagehandle);
//    }              // unpin meta page.
//
//    // Insert record header and record data into page.
//    pinPage(rel->bm, pagehandle, record->id.page);
//    memmove(pagehandle->data + 256*record->id.slot, &statement, sizeof(bool));   // Record header is a del_flag
//    memmove(pagehandle->data + 256*record->id.slot + sizeof(bool), record->data, recordSize); // Record body is values.
//    markDirty(rel->bm, pagehandle);
//    unpinPage(rel->bm, pagehandle);
//    
//    // Tuple number add 1.
//    // func 3: updata tuple infor
//    pinPage(rel->bm, pagehandle, 0);
//    memcpy(&numTuples, pagehandle->data + 3 * sizeof(int), sizeof(int));
//    numTuples++;
//    memcpy(pagehandle->data + 3 * sizeof(int), &numTuples, sizeof(int));
//    markDirty(rel->bm, pagehandle);
//    unpinPage(rel->bm, pagehandle);
//    
////
//    
////    updateTumpleInfor(rel->bm, pagehandle, numTuples);
//     // func3 done
//    
//    free(pagehandle);
//    return RC_OK;
    
    //    BM_PageHandle *pagehandle = MAKE_PAGE_HANDLE();
    //    int recordSize = getRecordSize(rel->schema);   //r_size
    //    int pageMetadataIndx = File_Meta_Data_Size(rel->bm);   // pageMetadataIndx
    //    int slotnum = ((recordSize + sizebool) / 256) + 1;  // r_slotnum
    //    int offset = 0;
    //    int recordCurNum, numTuples; // recordCurNum
    //    bool statement = true;  //r_stat
    //    bool check = true;
    
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
//    do {
//        pinPage(rel->bm, pagehandle, pageMetadataIndx);
//        memcpy(&pageMetadataIndx, pagehandle->data + PAGE_SIZE - sizeof(int), sizeof(int));
//        if(pageMetadataIndx != -1){
//            unpinPage(rel->bm, pagehandle);
//        } else {
//            break;
//        }
//    } while(true);
    
    findTargetPage(rel->bm, pagehandle, pageMetadataIndx);
    //func1 done:
    
    
    
    // Find out the target meta index and record number of the page.
    //func 2
    //
    do {
        memcpy(&recordCurNum, pagehandle->data + offset + sizeof(int), sizeof(int));
        offset += 2*sizeof(int);
    } while (recordCurNum == PAGE_SIZE / 256);
    
    // If no page exist, add new page.
    if(recordCurNum == -1){
        // If page mata is full, add new matadata block.
        if(offset == PAGE_SIZE){
            memcpy(pagehandle->data + PAGE_SIZE - sizeof(int), &rel->fh->totalNumPages, sizeof(int));   // Link into new meta data page.
            addPageMetadataBlock(rel->fh);
            markDirty(rel->bm, pagehandle);
            unpinPage(rel->bm, pagehandle);      // Unpin the last meta page.
            pinPage(rel->bm, pagehandle, rel->fh->totalNumPages-1);  // Pin the new page.
            offset = 2*sizeof(int);
        }
        memcpy(pagehandle->data + offset - 2*sizeof(int), &rel->fh->totalNumPages, sizeof(int));  // set page number.
        appendEmptyBlock(rel->fh);
        recordCurNum = 0;
    }
    
    
//    findRecordIndex(rel, pagehandle, offset, recordCurNum);
    // func 2 done:
    
    // func4
    // Read record->id and set record number add 1 in meta data.
    
//    memcpy(&record->id.page, pagehandle->data + offset - 2*sizeof(int), sizeof(int));   // Set record->id page number.
//    record->id.slot = recordCurNum * slotnum;                                // Set record->id slot.
//    recordCurNum++;
//    memcpy(pagehandle->data + offset - sizeof(int), &recordCurNum, sizeof(int));   // Set record number++ into meta data.
//    markDirty(rel->bm, pagehandle);
//    unpinPage(rel->bm, pagehandle);              // unpin meta page.
//    
//    // Insert record header and record data into page.
//    pinPage(rel->bm, pagehandle, record->id.page);
//    memcpy(pagehandle->data + 256*record->id.slot, &statement, sizeof(bool));   // Record header is a del_flag
//    memcpy(pagehandle->data + 256*record->id.slot + sizeof(bool), record->data, recordsize); // Record body is values.
//    markDirty(rel->bm, pagehandle);
//    unpinPage(rel->bm, pagehandle);
    
//    readRecord(RM_TableData *rel, BM_PageHandle *pagehandle ,Record *record, int slotnum, int offset, int recordCurNum , int recordsize)
    readRecord(rel, pagehandle, record, slotnum, offset, recordCurNum, recordsize);
    
    // func4 done
    
    
    // Tuple number add 1.
    //func3
//    pinPage(rel->bm, pagehandle, 0);
//    memcpy(&numTuples, pagehandle->data + 3 * sizeof(int), sizeof(int));
//    numTuples++;
//    memcpy(pagehandle->data + 3 * sizeof(int), &numTuples, sizeof(int));
//    markDirty(rel->bm, pagehandle);
//    unpinPage(rel->bm, pagehandle);
//
    
//    updateTumpleInfor(rel, pagehandle, numTuples);
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

        memcpy(&record_statment, bmPageHandle->data + spaceOffset, sizebool);
    record->data = (char *)malloc (record_size);

        flag = -99;
    
        if (record_statment == true){
            
            memcpy(record->data, bmPageHandle->data + spaceOffset + sizebool, record_size );
    
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
 * Author:liu zhipeng
 *
 * History:
 *      Date            Name                        Content
 *03/26/2016    liu zhipeng             design the outline of the function
***************************************************************/

RC next (RM_ScanHandle *scan, Record *record) 
{
    int index,maxslot,rpage,trs,rc;
    BM_BufferPool *tmpbm;
    tmpbm=scan->rel->bm;
    BM_PageHandle *ph=(BM_PageHandle*)calloc(1,sizeof(BM_PageHandle));
    RID rid;

    Value *result=(Value *)calloc(1,sizeof(Value));
    Record *tmp=(Record *)calloc(1,sizeof(Record));


//printf("ceshi : %s\n",scan->rel->name);
    trs=(getRecordSize (scan->rel->schema)+sizeof(bool))/256+1;
    index=File_Meta_Data_Size(tmpbm);
    
    pinPage(tmpbm,ph,index);

    while(scan->curPage!=index)
    {
        memcpy(&rpage,ph->data+(scan->curPage)*2*sizeof(int),sizeof(int));
        memcpy(&maxslot, ph->data + ((scan->curPage) *2+1)* sizeof(int), sizeof(int));
        int i;
        if(maxslot!=-1)
        {
            for(i=scan->curSlot;i<maxslot;i++)
            {
                rid.page=rpage;
                rid.slot=i*trs;
                if((rc=getRecord(scan->rel,rid,tmp))==RC_OK)
                {   
                    evalExpr (tmp, scan->rel->schema, scan->expr,&result);;
                    if(result->v.boolV)
                    {
                        record->id.page=rid.page;
                        record->id.slot=rid.slot;
                        record->data=tmp->data;
                        if(i==maxslot-1)
                        {
                            scan->curPage++;
                            scan->curSlot=0;
                        }
                        else{
                            scan->curSlot=i+1;
                        }
                        free(result);
                        free(tmp);
                        unpinPage (tmpbm, ph);
                        free(ph);
                        return RC_OK;
                    }
                }
            }
        }
        else
        {
            unpinPage(tmpbm,ph);
            free(ph);
            return RC_RM_NO_MORE_TUPLES;
        }
        scan->curPage++;
    }
    unpinPage (tmpbm, ph);
    free(ph);
    return RC_RM_NO_MORE_TUPLES;
}

    //xpy:
//    RC flag = -99;
//    
//    BM_BufferPool *bufferpool; // tmpbm;
//    bufferpool = scan->rel->bm;
//    
//    int index,maxslot;
//    int rpage;// rapge = ridpage
//    int recordsize; //trs
//    
//    BM_PageHandle * bmPageHandle;
//    Record *recordp;  //tmp
//    Value *value;  //result
//    
//    bmPageHandle = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
//    recordp = (Record*)malloc (sizeof(Record));
//    value= (Value *)malloc (sizeof(Value));
//    
//    RID rid;
//    
//    
//    recordsize = ((getRecordSize(scan->rel->schema) + sizeof(bool))/ SLOT_SIZE) + 1;
//    index = File_Meta_Data_Size(bufferpool);
//
//    pinPage(bufferpool, bmPageHandle, index);
//    
//    if (scan->curPage < 0){
//        printf("wrong scan curpage, in next()\n");
//    }
//    
////        for (; scan->curPage<=index; scan->curPage++){
//    while (scan->curPage != index){
//        memmove(&rpage, bmPageHandle->data + (scan->curPage) * 2 * sizeof(int), sizeof(int));
//        memmove(&maxslot, bmPageHandle->data + ((scan->curPage) * 2 + 1) * sizeof(int), sizeof(int));
//    
//        int i;
//        if ( maxslot != 1){
//            i = scan->curSlot;
//            while (i < maxslot){
////            for( ;i<maxslot;i++){
//                rid.page = rpage;
//                rid.slot = i * recordsize;
//                flag = getRecord(scan->rel, rid, recordp);
//                if (flag == RC_OK){
//                    evalExpr(recordp, scan->rel->schema, scan->expr, &value);
//    
//                    if (value->v.boolV){
//                        record->id.page = rid.page;
//                        record->id.slot = rid.slot;
//                        record->data = recordp->data;
//    
//                            // if near the end;
//                        if (i == (maxslot -1)){
//                            scan->curPage +=1;
//                            scan->curSlot = 0;
//                        }
//                        else {
//                            scan->curSlot +=1;
//                        }
//
//                        free(value);
//                        free(recordp);
//                        unpinPage(bufferpool, bmPageHandle);
//                        free(bmPageHandle);
//                        return RC_OK;
//                        }
//                    }
//
//                else{
//                    return RC_FILE_NOT_FOUND;
//                }
//                i++;
//            }
//        }
//        else{
//            printf("here is wrong 1 , in next()\n");
//            unpinPage(bufferpool, bmPageHandle);
//            free(bmPageHandle);
//            return RC_FILE_NOT_FOUND;
//        }
//        scan->curPage++;
//    }
//
//
//    printf("unknow error, in next()");
//    unpinPage(bufferpool, bmPageHandle);
//    free(bmPageHandle);
//    return RC_FILE_NOT_FOUND;
//}

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

    // xpy:
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
    
    // xpy:
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
    // xpy:
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
    
    //xpy:
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
    // xpy:
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
            memcpy(&((*value)->v.intV), record->data + attrOffset, sizeof(int));
            break;
        }
    
        else if (schema->dataTypes[attrNum] == DT_BOOL){
            memcpy(&((*value)->v.boolV), record->data + attrOffset, sizeof(int));
            break;
        }
    
        else if (schema->dataTypes[attrNum] == DT_FLOAT){
            memcpy(&((*value)->v.floatV), record->data + attrOffset, sizeof(float));
            break;
        }
    
        else if (schema->dataTypes[attrNum] == DT_STRING){
            (*value)->v.stringV = (char *)malloc ( schema->typeLength[attrNum] +1);
            memcpy((*value)->v.stringV, record->data + attrOffset, schema->typeLength[attrNum]);

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
 *   11/8/2016    Pingyu Xue <pxue2@hawk.iit.edu>   add commentand init , fix the memcpy error
***************************************************************/
RC setAttr (Record *record, Schema *schema, int attrNum, Value *value) {
    //xpy:
    
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
    while (true){
        if (schema->dataTypes[attrNum] == DT_INT){
//            memcpy(record->data + attrOffset,  &(value->v.intV), sizeint);
            memmove(record->data + attrOffset, &(value->v.intV), sizeint);
            break;
        }
        
        else if ( schema->dataTypes[attrNum] == DT_BOOL){
//            memcpy(record->data + attrOffset, &(value->v.boolV), sizeint);
            memmove(record->data + attrOffset, &(value->v.boolV), sizeint);
            break;
        }
        
        else if( schema->dataTypes[attrNum] == DT_FLOAT){
//            memcpy(record->data + attrOffset, &(value->v.floatV), sizefloat);
            memmove(record->data + attrOffset, &(value->v.floatV), sizefloat);
            break;
        }
        
        else if (schema->dataTypes[attrNum] == DT_STRING){
            if (strlen(value->v.stringV) >= schema->typeLength[attrNum]){
//                memcpy(record->data + attrOffset, &(value->v.stringV), schema->typeLength[attrNum]);
                memmove(record->data + attrOffset, value->v.stringV, schema->typeLength[attrNum]);
                break;
            }
            else{
//                strcpy(record->data + attrOffset, value->v.stringV);
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
***************************************************************/
RC addPageMetadataBlock(SM_FileHandle *fh) {
    RC RC_flag;
    int pageNum, capacity;
    char * pageMetadataInput;
    int pageMetadataNum;

    int i;
    RC_flag = appendEmptyBlock(fh);
    if (RC_flag != RC_OK) {
        RC_flag = closePageFile(fh);
        return RC_flag;
    }


    pageMetadataNum = PAGE_SIZE / (2*sizeof(int));

    pageMetadataInput = (char *)calloc(PAGE_SIZE, sizeof(char));
    pageNum = fh->totalNumPages;
    capacity = -1;

    for (i = 0; i < pageMetadataNum; ++i){ 
        memcpy(pageMetadataInput+i*2*sizeof(int), &pageNum, sizeof(int));
        memcpy(pageMetadataInput+i*2*sizeof(int)+sizeof(int), &capacity, sizeof(int));
        pageNum++;
        if (i == pageMetadataNum - 1) {
            pageNum = fh->totalNumPages - 1;
        }
    }
    RC_flag = writeBlock(fh->totalNumPages - 1, fh, pageMetadataInput);
    if (RC_flag != RC_OK) {
        return RC_flag;
    }

    free(pageMetadataInput);
    return RC_OK;
    
    

    
    // xpy:
//        RC flag = -99;
//    
//    //    RC pagenum = -1;
//    //    RC pagedataNum = -1;
//    //    RC pagedataCapacity = -1;
//    
//        int page_num;
//        int page_data_num;
//        int page_capacity;
//    
//    
//        char * input;
//    
//    //    int i = 0 ;
//        int i ;
//    
//        flag = appendEmptyBlock(fh);
//        if( flag != RC_OK){
//            printf(" flag not rc_ok, check addPageData, appendEmptyBlock(), in addOnePageData() \n");
//            closePageFile(fh);
//            return flag;
//        }
//    
//        size_t init_size = 2 * sizeof(int);
//    
//        page_data_num = PAGE_SIZE/ init_size;
//        input = (char *) malloc (PAGE_SIZE * sizeof(char));
//        page_num = fh->totalNumPages;
//    
//        //    pagedataCapacity = -1;
//    
//    //    while ( ++i < page_data_num){
//        for(i = 0; i< page_data_num; i++){
//            memcmp(input + (i * init_size), &page_num, sizeof(int));
//            memcmp(input + (i * init_size) + sizeof(int) , &page_capacity, sizeof(int));
//    
//            page_num++;
//    
//            if ( i == page_data_num -1){
//                page_num = fh->totalNumPages -1;
//            }
//        }
//    
//        flag = writeBlock(fh->totalNumPages -1, fh, input);
//        if( flag != RC_OK){
//            printf("the writeBlock is wrong, in addOnePageData()\n");
//            return flag;
//        }
//        
//        
//        free( input);
//        return  RC_OK;
}


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

//Schema *scheam_cr;
//int numAttr_cr;
//char ** attrNames_cr;
//DataType *dataTypes_cr;
//int *typeLength_cr;
//int *keyAttrs_cr;
//int keysize_cr;

// func 2:
int get_Type_Length (int numAttr, DataType *dataTypes, char **attrNames, char *input, int *typeLength)
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


// insert record func:
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
                memcpy(pagehandle->data + PAGE_SIZE - sizeof(int), &rel->fh->totalNumPages, sizeof(int));   // Link into new meta data page.
                addPageMetadataBlock(rel->fh);
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
    int extra_size = 3 * sizeint;
    
    if(rel->bm == NULL || pagehandle == NULL){
        return flag;
    }
    
    flag = pinPage(rel->bm, pagehandle, 0);
    
    if(flag == RC_OK){
        //    memmove(&numTumples, pagehandle->data + 3 * sizeof(int), sizeof(int));

        memmove(&numTumples, pagehandle->data + extra_size, sizeint);
        numTumples++;
        
        memcpy(pagehandle->data + extra_size, &numTumples, sizeint);
        
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
    
    memcpy(&record->id.page, pagehandle->data + offset - 2*sizeof(int), sizeof(int));   // Set record->id page number.
    record->id.slot = recordCurNum * slotnum;                                // Set record->id slot.
    recordCurNum++;
    memcpy(pagehandle->data + offset - sizeof(int), &recordCurNum, sizeof(int));   // Set record number++ into meta data.
    markDirty(rel->bm, pagehandle);
    unpinPage(rel->bm, pagehandle);              // unpin meta page.
    
    // Insert record header and record data into page.
    pinPage(rel->bm, pagehandle, record->id.page);
    memcpy(pagehandle->data + 256*record->id.slot, &statement, sizeof(bool));   // Record header is a del_flag
    memcpy(pagehandle->data + 256*record->id.slot + sizeof(bool), record->data, recordsize); // Record body is values.
    markDirty(rel->bm, pagehandle);
    unpinPage(rel->bm, pagehandle);

    return RC_OK;
}