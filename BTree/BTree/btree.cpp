/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * btree.cpp modified by Brett Meyer
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */
#include <vector>
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
                Page* temp;
                PageId toAdd;
                LeafNodeInt* leaf;
                bufMgr->allocPage(file, toAdd, temp);
                root->pageNoArray[0] = toAdd;
                leaf = (LeafNodeInt*) temp;
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
                for (int i = 0; i < INTARRAYNONLEAFSIZE && !found; i++)
                {
                    int currentKey = node->keyArray[i];
                    if (currentKey == 0 || keyInt <= currentKey)
                    {
                        found = true;
                        currentId = node->pageNoArray[i];
                        saveIndex = i;
                    }
                    else if (i == INTARRAYNONLEAFSIZE - 1)
                    {
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
    
}
