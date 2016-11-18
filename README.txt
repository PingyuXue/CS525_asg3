=====================================================================================
=                   525 Programming Assignment 2 README file                        = 
=====================================================================================
                    1. Personnel information

     Pingyu Xue                 A20358086               pxue2@hawk.iit.edu
     Hanqiao Lu                 A20324072               hlu22@hawk.iit.edu
     Uday Tak                   A20381053               utak@hawk.iit.edu
     Vaibhav Uday Hongal	  A20378220               vhongal@hawk.iit.edu
  

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
                    2. File list 

  - buffer_mgr.c
  - buffer_mgr.h
  - buffer_mgr_stat.c
  - buffer_mgr_stat.h
  - dberror.c
  - dberror.h
  - dt.h
  - expr.c
  - expr.h
  - Makefile
  - README
  - record_mgr.c
  - record_mgr.h
  - rm_serializer.c
  - storage_mgr.c
  - storage_mgr.h
  - tables.h
  - test_assign3_1.c
  - test_expr.c
  - test_helper.h
  
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
                    3. Milestone

  2016/11/16 Complete project.

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
                    4. Installation instruction  

  using test_assign3_1.c test:
    $ make test
    $ ./test

  after test, use clean to delete files except source code.
    $ make clean

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
                    5. Function descriptions: of all additional functions  

extern RC initRecordManager (void *mgmtData);
extern RC shutdownRecordManager ();
extern RC createTable (char *name, Schema *schema);
extern RC openTable (RM_TableData *rel, char *name);
extern RC closeTable (RM_TableData *rel);
extern RC deleteTable (char *name);
extern int getNumTuples (RM_TableData *rel);

// handling records in a table
extern RC insertRecord (RM_TableData *rel, Record *record);
extern RC deleteRecord (RM_TableData *rel, RID id);
extern RC updateRecord (RM_TableData *rel, Record *record);
extern RC getRecord (RM_TableData *rel, RID id, Record *record);

// scans
extern RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond);
extern RC next (RM_ScanHandle *scan, Record *record);
extern RC closeScan (RM_ScanHandle *scan);

// dealing with schemas
extern int getRecordSize (Schema *schema);
extern Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys);
extern RC freeSchema (Schema *schema);

// dealing with records and attribute values
extern RC createRecord (Record **record, Schema *schema);
extern RC freeRecord (Record *record);
extern RC getAttr (Record *record, Schema *schema, int attrNum, Value **value);
extern RC setAttr (Record *record, Schema *schema, int attrNum, Value *value);

// additional function
extern RC addPageMetadataBlock(SM_FileHandle *fh);
extern int getFileMetaDataSize(BM_BufferPool *bm);
extern int recordCostSlot(BM_BufferPool *bm);
extern int getSlotSize(BM_BufferPool *bm);
#endif // RECORD_MGR_H

// addtional by xpy:
extern int getTotalRecords_slot( RM_TableData *rel);
extern int modifyFileMetaPage (RM_TableData *rel, char *name);
extern int File_Meta_Data_Size(BM_BufferPool *bm);
***************************************************************/

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
                    6. Additional error codes: of all additional error codes 

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
                    7. Data structure: main data structure used

typedef enum DataType {
  DT_INT = 0,
  DT_STRING = 1,
  DT_FLOAT = 2,
  DT_BOOL = 3
} DataType;

typedef struct Value {
  DataType dt;
  union v {
    int intV;
    char *stringV;
    float floatV;
    bool boolV;
  } v;
} Value;

typedef struct RID {
  int page;
  int slot;
} RID;

typedef struct Record
{
  RID id;
  char *data;
} Record;

// information of a table schema: its attributes, datatypes, 
typedef struct Schema
{
  int numAttr;
  char **attrNames;
  DataType *dataTypes;
  int *typeLength;
  int *keyAttrs;
  int keySize;
} Schema;

// TableData: Management Structure for a Record Manager to handle one relation
typedef struct RM_TableData
{
  char *name;
  Schema *schema;
  BM_BufferPool *bm;
  SM_FileHandle *fh;
} RM_TableData;

typedef struct RM_ScanHandle
{
  RM_TableData *rel;
  int curPage;
  int curSlot;
  Expr *expr;
  void *mgmtData;
} RM_ScanHandle;

// datatype for arguments of expressions used in conditions
typedef enum ExprType {
  EXPR_OP,
  EXPR_CONST,
  EXPR_ATTRREF
} ExprType;

typedef struct Expr {
  ExprType type;
  union expr {
    Value *cons;
    int attrRef;
    struct Operator *op;
  } expr;
} Expr;

// comparison operators
typedef enum OpType {
  OP_BOOL_AND,
  OP_BOOL_OR,
  OP_BOOL_NOT,
  OP_COMP_EQUAL,
  OP_COMP_SMALLER
} OpType;

typedef struct Operator {
  OpType type;
  Expr **args;
} Operator;

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
                    8. Extra credit: of all extra credits 

  No extra credit.

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
                    9. Additional files: of all additional files 

  No additioinal files.

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
                    10. Test cases: of all additional test cases added 

  Test case:
  testInsertManyRecords()
  testRecords()
  testCreateTableAndInsert()
  testUpdateTable()
  testScans()
  testScansTwo()
  testMultipleScans()
         
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
                    11. Problems solved  

  Implement all required functions and additional test case.

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
                    12. Problems to be solved

  This program could be optimized.
