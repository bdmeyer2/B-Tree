/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * btree.cpp modified by Brett Meyer
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */
#include <stack>
#include <stdlib.h>
#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"





//#define DEBUG

namespace badgerdb
{
    int compareInt (const void * a, const void * b)
    {
        return ( *(int*)a - *(int*)b );
    }
    
    int compareRecordId (const void * a, const void * b)
    {
        RecordId c = *(RecordId *)a;
        RecordId d = *(RecordId *)b;
        int x = c.page_number;
        int y = d.page_number;
        return (x - y);
    }
    
    // -----------------------------------------------------------------------------
    // BTreeIndex::BTreeIndex -- Constructor
    // -----------------------------------------------------------------------------
    
    BTreeIndex::BTreeIndex(const std::string & relationName,
                           std::string & outIndexName,
                           BufMgr *bufMgrIn,
                           const int attrByteOffset,
                           const Datatype attrType)
    {
        this->bufMgr = bufMgrIn;
        this->attributeType = attrType;
        this->attrByteOffset = attrByteOffset;
        
        std::ostringstream idxStr;
        idxStr << relationName << '.' << attrByteOffset;
        std::string indexName = idxStr.str( ); // indexName is the name of the index file
        outIndexName = indexName;
        
        //INTEGER
        if (this->attributeType == 0)
        {
            this->leafOccupancy = INTARRAYLEAFSIZE;
            this->nodeOccupancy = INTARRAYNONLEAFSIZE;
        }
        //DOUBLE
        else if (this->attributeType == 1)
        {
            this->leafOccupancy = DOUBLEARRAYLEAFSIZE;
            this->nodeOccupancy = DOUBLEARRAYNONLEAFSIZE;
        }
        //STRING
        else if (this->attributeType == 2)
        {
            this->leafOccupancy = STRINGARRAYLEAFSIZE;
            this->nodeOccupancy = STRINGARRAYNONLEAFSIZE;
        }
        //ERROR
        else
        {
            std::cout << "ERROR in BTreeIndex constructor: Datatype must equal INTEGER(0) DOUBLE(1) STRING(2)\n";
        }
        
        try
        {
            //Create blob file constructor will throw if file does not exist
            this->file = new BlobFile(outIndexName, false);
            
            //File does exist
            //Page 1 is meta data page
            //Get page 1 for its indexMetaInfo
            Page * page;
            bufMgr->readPage(file, 1, page);
            IndexMetaInfo * meta;
            meta = (IndexMetaInfo *)page;
            //Set rootPageNum
            this->rootPageNum = meta->rootPageNo;
            //Set IndexMetaInfo byte offset
            meta->attrByteOffset = attrByteOffset;
            //Set IndexMetaInfo relation name
            strcpy(meta->relationName, relationName.c_str());
            //Set IndexMetaInfo DataType (DO WE NEED THIS?)
            meta->attrType = attrType;
            //Unpin because if we construct to many pages in a row we would run out of buffer space
            bufMgr->unPinPage(file, 1, false);
            //No scan is executing
            this->scanExecuting = false;
        }
        //File does not exist
        catch (FileNotFoundException e)
        {
            //Create new file
            this->file = new BlobFile(outIndexName, true);
            PageId rootId;
            Page * rootPage;
            bufMgr->allocPage(file, rootId, rootPage);
            
            //SET UP THE ROOT PAGE
            //INTEGER
            if (this->attributeType == 0)
            {
                NonLeafNodeInt * root = (NonLeafNodeInt *) rootPage;
                root->level = 1;
                for (int i = 0; i < INTARRAYNONLEAFSIZE; i++)
                {
                    root->keyArray[i] = 0;
                }
                Page* page;
                PageId pageID;
                LeafNodeInt* leaf;
                bufMgr->allocPage(file, pageID, page);
                root->pageNoArray[0] = pageID;
                leaf = (LeafNodeInt*) page;
                for(unsigned int i = 0; i < INTARRAYLEAFSIZE; i++)
                {
                    leaf->keyArray[i] = 0;
                }
                leaf->rightSibPageNo = 0;
            }
            //DOUBLE
            else if (this->attributeType == 1)
            {
                NonLeafNodeDouble * root = (NonLeafNodeDouble *) rootPage;
                root->level = 1;
                for (int i = 0; i < INTARRAYNONLEAFSIZE; i++)
                {
                    root->keyArray[i] = 0;
                }
                Page* temp;
                PageId toAdd;
                LeafNodeDouble* leaf;
                bufMgr->allocPage(file, toAdd, temp);
                root->pageNoArray[0] = toAdd;
                leaf = (LeafNodeDouble*) temp;
                for(unsigned int i = 0; i < INTARRAYLEAFSIZE; i++)
                {
                    leaf->keyArray[i] = 0;
                }
                leaf->rightSibPageNo = 0;
            }
            //STRING
            else if (this->attributeType == 2)
            {
                
            }
            //ERROR
            else
            {
                
            }
            bufMgr->unPinPage(file, rootId, false);
            
            //SET UP THE META INFO PAGE
            Page * metaPage;
            PageId metaPageId = 1;
            bufMgr->allocPage(file, metaPageId, metaPage);
            IndexMetaInfo * metaInfo;
            
            metaInfo = (IndexMetaInfo *) metaPage;
            strcpy(metaInfo->relationName, relationName.c_str());
            metaInfo->attrByteOffset = attrByteOffset;
            metaInfo->attrType = attrType;
            metaInfo->rootPageNo = metaPageId;
            
            bufMgr->unPinPage(file, metaPageId, false);
            
            FileScan scan(relationName, bufMgr);
            
            RecordId scanID;
            scanID.page_number = 0;
            try {
                while (true)
                {
                    scan.scanNext(scanID);
                    std::string recordStr = scan.getRecord();
                    const char *record = recordStr.c_str();
                    void* key = (void*)(record + attrByteOffset);
                    insertEntry(key, scanID);
                }
            }
            catch (EndOfFileException e)
            {
                // Index has completed
            }
        }
    }
    
    
    // -----------------------------------------------------------------------------
    // BTreeIndex::~BTreeIndex -- destructor
    // -----------------------------------------------------------------------------
    
    BTreeIndex::~BTreeIndex()
    {
        if(this->scanExecuting == true)
        {
            this->endScan();
        }
        bufMgr->flushFile(this->file);
        try
        {
            delete this->file;
        }
        catch (FileNotFoundException e)
        {
            
        }
    }
    
    // -----------------------------------------------------------------------------
    // BTreeIndex::insertEntry
    // -----------------------------------------------------------------------------
    
    const void BTreeIndex::insertEntry(const void *key, const RecordId rid)
    {
        //INTEGER
        if (this->attributeType == 0)
        {
            //FIND THE FUCKING PAGEID
            std::stack<PageId>stack;
            int keyInt = *(int*)key;
            PageId currentId = this->rootPageNum;
            Page * page;
            int saveIndex = 0;
            bool done = false;
            while (!done)
            {
                bufMgr->readPage(file, currentId, page);
                bufMgr->unPinPage(file, currentId, false);
                NonLeafNodeInt * node = (NonLeafNodeInt *) page;
                bool found = false;
                //Note the second else if may not be needed or could be made cleaner
                for (int i = 0; i < INTARRAYNONLEAFSIZE && !found; i++)
                {
                    int currentKey = node->keyArray[i];
                    if (currentKey == 0 || keyInt <= currentKey)
                    {
                        stack.push(currentId);

                        found = true;
                        currentId = node->pageNoArray[i];
                        saveIndex = i;
                    }
                    else if (i == INTARRAYNONLEAFSIZE - 1)
                    {
                        stack.push(currentId);

                        currentId = node->pageNoArray[INTARRAYNONLEAFSIZE];
                        saveIndex = INTARRAYNONLEAFSIZE;
                    }
                }
                if (node->level == 1)
                {
                    done = true;
                }
            }
            
            //find out if node is full
            int freeIndex = -1;
            bool full = true;
            Page* temp;
            bufMgr->readPage(file, this->rootPageNum, temp);
            LeafNodeInt * node;
            node = (LeafNodeInt *) temp;
            for(int i = 0; i < leafOccupancy || !full; i++)
            {
                
                if(node->ridArray[i].page_number == 0)
                {
                    full = false;
                    freeIndex = i;
                }
            }
            if(full)
            {
                freeIndex = -1;
            }
            
            //not full
            if (freeIndex != -1)
            {
                Page * page;
                bufMgr->readPage(file, currentId, page);
                LeafNodeInt * newNode;
                newNode = (LeafNodeInt*) page;
                for (int i = freeIndex-1; i >= 0 && !done; i++)
                {
                    if (newNode->keyArray[i] > keyInt)
                    {
                        newNode->keyArray[i+1] = newNode->keyArray[i];
                        newNode->ridArray[i+1] = newNode->ridArray[i];
                    }
                    else
                    {
                        newNode->keyArray[i+1] = keyInt;
                        newNode->ridArray[i+1] = rid;
                    }
                }
                bufMgr->unPinPage(file, currentId, true);//page dirtied
            }
            //FUCK ITS FULL
            else
            {
                //create new node and split values, pushKeyUp is the key to insert into parent
                
                //current node is called node
                Page * newPage;
                LeafNodeInt * newLeafNode;
                PageId newPageId;
                bufMgr->allocPage(file, newPageId, newPage);
                newLeafNode->rightSibPageNo = node->rightSibPageNo;
                node->rightSibPageNo = newPageId;
                RecordId blankRID;
                blankRID.page_number = 0;
                for(int i = 0; i < leafOccupancy; i++)
                {
                    newLeafNode->keyArray[i] = 0;
                    newLeafNode->ridArray[i] = blankRID;
                }
                RecordId newRidArr [INTARRAYLEAFSIZE + 1];
                int newKeyArr [INTARRAYLEAFSIZE + 1];
                for (int i = 0; i < INTARRAYLEAFSIZE + 1; i++)
                {
                    if (i < INTARRAYLEAFSIZE) {
                        newRidArr[i] = node->ridArray[i];
                        newKeyArr[i] = node->keyArray[i];
                    }
                    else if (i == INTARRAYLEAFSIZE)
                    {
                        newRidArr[i] = rid;
                        newKeyArr[i] = keyInt;
                    }
                }
                
                qsort(newKeyArr, INTARRAYLEAFSIZE + 1, sizeof(int), compareInt);
                qsort(newRidArr, INTARRAYLEAFSIZE + 1, sizeof(RecordId), compareRecordId);
                
                int midIndex = (INTARRAYLEAFSIZE + 1) % 2;
                if(midIndex == 0)
                {
                    midIndex = (INTARRAYLEAFSIZE + 1) / 2;
                }
                else
                {
                    midIndex = ((INTARRAYLEAFSIZE + 1) / 2) + 1;
                }
                
                
                int k = 0;
                for (int i = midIndex; i < INTARRAYLEAFSIZE + 1; i++)
                {
                    if (i < INTARRAYLEAFSIZE)
                    {
                        node->keyArray[i] = 0;
                        node->ridArray[i] = blankRID;
                        newLeafNode->keyArray[k] = newKeyArr[i];
                        newLeafNode->ridArray[k] = newRidArr[i];
                    }
                    else if (i == INTARRAYLEAFSIZE)
                    {
                        newLeafNode->keyArray[k] = newKeyArr[i];
                        newLeafNode->ridArray[k] = newRidArr[i];
                    }
                    k++;
                }
                
                int pushUpKey = newKeyArr[midIndex];
                
                bufMgr->unPinPage(file, newPageId, false);
                bool done = false;
                while (!done)
                {
                    PageId currentId = stack.top();
                    //find out if node is full
                    int freeIndex = -1;
                    bool full = true;
                    Page * temp;
                    bufMgr->readPage(file, currentId, temp);
                    LeafNodeInt * node;
                    node = (LeafNodeInt *) temp;
                    for(int i = 0; i < leafOccupancy || !full; i++)
                    {
                        
                        if(node->ridArray[i].page_number == 0)
                        {
                            full = false;
                            freeIndex = i;
                        }
                    }
                    if(full)
                    {
                        freeIndex = -1;
                    }
                    //if not full
                    if (!full)
                    {
                        Page* page;
                        NonLeafNodeInt* parent;
                        bufMgr->readPage(file, currentId, page);
                        parent = (NonLeafNodeInt*) page;
                        for (int i = freeIndex-1; i >= 0 && !done; i++)
                        {
                            if (parent->keyArray[i] > pushUpKey)
                            {
                                parent->keyArray[i+1] = parent->keyArray[i];
                                parent->pageNoArray[i+1] = parent->pageNoArray[i];
                            }
                            else
                            {
                                parent->keyArray[i+1] = pushUpKey;
                                parent->pageNoArray[i+1] = newPageId;
                            }
                        }
                        bufMgr->unPinPage(file, currentId, true);
                        done = true;
                    }
                    //else full
                    else
                    {
                        Page * oldPage;
                        bufMgr->readPage(file, currentId, oldPage);
                        NonLeafNodeInt * node = (NonLeafNodeInt *)oldPage;
                        Page * newPage;
                        NonLeafNodeInt * newNonLeafNode;
                        PageId newPageId;
                        bufMgr->allocPage(file, newPageId, newPage);
                        if (node->level == 1)
                        {
                            newNonLeafNode->level = 1;
                        }
                        else
                        {
                            newNonLeafNode->level = 0;
                        }
                        
                        for(int i = 0; i < INTARRAYNONLEAFSIZE; i++)
                        {
                            newNonLeafNode->keyArray[i] = 0;
                            newNonLeafNode->pageNoArray[i] = 0;
                        }
                        int newKeyArr [INTARRAYNONLEAFSIZE + 1];
                        int newPageNoArray [INTARRAYNONLEAFSIZE + 2];
                        for (int i = 0; i < INTARRAYNONLEAFSIZE + 1; i++)
                        {
                            if (i < INTARRAYNONLEAFSIZE) {
                                newPageNoArray[i] = node->pageNoArray[i];
                                newKeyArr[i] = node->keyArray[i];
                            }
                            else if (i == INTARRAYNONLEAFSIZE)
                            {
                                newPageNoArray[i] = newPageId;
                                newKeyArr[i] = pushUpKey;
                            }
                        }
                        
                        qsort(newKeyArr, INTARRAYNONLEAFSIZE + 1, sizeof(int), compareInt);
                        qsort(newPageNoArray, INTARRAYNONLEAFSIZE + 1, sizeof(PageId), compareInt);
                        
                        int midIndex = (INTARRAYNONLEAFSIZE + 1) % 2;
                        if(midIndex == 0)
                        {
                            midIndex = (INTARRAYNONLEAFSIZE + 1) / 2;
                        }
                        else
                        {
                            midIndex = ((INTARRAYNONLEAFSIZE + 1) / 2) + 1;
                        }
                        
                        
                        int k = 0;
                        for (int i = midIndex; i < INTARRAYNONLEAFSIZE + 1; i++)
                        {
                            if (i < INTARRAYNONLEAFSIZE)
                            {
                                node->keyArray[i] = 0;
                                node->pageNoArray[i] = 0;
                                newNonLeafNode->keyArray[k] = newKeyArr[i];
                                newNonLeafNode->pageNoArray[k] = newPageNoArray[i];
                            }
                            else if (i == INTARRAYNONLEAFSIZE)
                            {
                                newNonLeafNode->keyArray[k] = newKeyArr[i];
                                newNonLeafNode->pageNoArray[k] = newPageNoArray[i];
                            }
                            k++;
                        }
                        
                        int pushUpKey = newKeyArr[midIndex];
                        bufMgr->unPinPage(file, currentId, true);
                        bufMgr->unPinPage(file, newPageId, true);
                        
                        if(stack.size() == 1)
                        {
                            Page * page;
                            PageId pageId;
                            NonLeafNodeInt* newNode;
                            bufMgr->allocPage(file, pageId, page);
                            newNode = (NonLeafNodeInt*) page;
                            newNode->level = 0;
                            newNode->keyArray[0] = pushUpKey;
                            for(int i = 1; i < INTARRAYNONLEAFSIZE; i++)
                            {
                                newNode->keyArray[i] = 0;
                            }
                            newNode->pageNoArray[0] = currentId;
                            newNode->pageNoArray[1] = newPageId;
                            
                            IndexMetaInfo* metaIndex;
                            Page* meta;
                            bufMgr->readPage(file, 1, meta);
                            metaIndex = (IndexMetaInfo*) meta;
                            metaIndex->rootPageNo = pageId;
                            this->rootPageNum = pageId;
                            bufMgr->unPinPage(file, 1, true);
                        }
                    }
                    stack.pop();
                }
                
            }
            
        }
        //DOUBLE
        else if (this->attributeType == 1)
        {
            
        }
        //STRING
        else if (this->attributeType == 2)
        {
            
        }
        //ERROR
        else
        {
            
        }
    }
    
    // -----------------------------------------------------------------------------
    // BTreeIndex::startScan
    // -----------------------------------------------------------------------------
    
    const void BTreeIndex::startScan(const void* lowValParm,
                                     const Operator lowOpParm,
                                     const void* highValParm,
                                     const Operator highOpParm)
    {
        
    }
    
    // -----------------------------------------------------------------------------
    // BTreeIndex::scanNext
    // -----------------------------------------------------------------------------
    
    const void BTreeIndex::scanNext(RecordId& outRid)
    {
        
    }
    
    // -----------------------------------------------------------------------------
    // BTreeIndex::endScan
    // -----------------------------------------------------------------------------
    //
    const void BTreeIndex::endScan()
    {
        
    }
    
    //SPLIT LEAF
    
    //SPLIT NONLEAF
    //create new node and split values, pushKeyUp is the key to insert into parent
    

}
