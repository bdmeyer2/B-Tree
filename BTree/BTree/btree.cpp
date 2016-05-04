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
#include "exceptions/file_exists_exception.h"





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
        //get name of index
        std::ostringstream idxStr;
        idxStr << relationName << "." << attrByteOffset;
        outIndexName = idxStr.str(); // indexName is the name of the index file
        
        //setting private vars
        bufMgr = bufMgrIn;
        BTreeIndex::attrByteOffset = attrByteOffset;
        attributeType = attrType;
        scanExecuting = false; //NOTHING SHOULD BE SCANNING THE INDEX @ THIS POINT.
        switch (attributeType) {
            case INTEGER:
                leafOccupancy = INTARRAYLEAFSIZE;
                nodeOccupancy = INTARRAYNONLEAFSIZE;
                break;
            case DOUBLE:
                leafOccupancy = DOUBLEARRAYLEAFSIZE;
                nodeOccupancy = DOUBLEARRAYNONLEAFSIZE;
                break;
            case STRING:
                leafOccupancy = STRINGARRAYLEAFSIZE;
                nodeOccupancy = STRINGARRAYNONLEAFSIZE;
                break;
                
        }
        
        try
        {
            File::remove(outIndexName);
        }
        catch (FileNotFoundException e)
        {
            std::cout << "FILE NOT FOUND EXCEPTION CAUGHT\n";
        }
        //indexFile exists?
        std::cout << "indexFile " << outIndexName << " exists? " << std::endl;
        try {
            //NUP
            file = new BlobFile(outIndexName, true); //create new
            std::cout << "NUP" << std::endl;
            Page *empty_header_page, *empty_root_page;
            
            bufMgr->allocPage(file, headerPageNum,
                              empty_header_page); //alloc an empty page for metadata, set headerPageNum
            bufMgr->unPinPage(file, headerPageNum, true); //not needed anymore => unpin
            bufMgr->allocPage(file, rootPageNum,
                              empty_root_page); //alloc initial empty leaf node root, set rootPageNum
            bufMgr->unPinPage(file, rootPageNum, true); //not needed anymore => unpin
            
            //initialize root page
            switch(attributeType) {
                    
                case INTEGER: {
                    LeafNodeInt *root = (LeafNodeInt *) empty_root_page;
                    RecordId blankRID;
                    blankRID.page_number = -1;
                    for (int i = 0; i < INTARRAYLEAFSIZE; i++) {
                        root->keyArray[i] = -1;
                        root->ridArray[i] = blankRID;
                    }
                    root->rightSibPageNo = -1;
                    break;
                }
                case DOUBLE:
                {
                    LeafNodeDouble *root = (LeafNodeDouble *) empty_root_page;
                    RecordId blankRID;
                    blankRID.page_number = 0;
                    for (int i = 0; i < DOUBLEARRAYLEAFSIZE; i++) {
                        root->ridArray[i] = blankRID;
                    }
                    root->rightSibPageNo = 0;
                    break;
                }
                case STRING:
                {
                    LeafNodeString *root = (LeafNodeString *) empty_root_page;
                    RecordId blankRID;
                    blankRID.page_number = 0;
                    for (int i = 0; i < STRINGARRAYLEAFSIZE; i++) {
                        root->ridArray[i] = blankRID;
                    }
                    root->rightSibPageNo = 0;
                    break;
                }
                default:
                    break;
            }
            
            
            //initialize meta_info
            IndexMetaInfo *meta_info = (IndexMetaInfo *) empty_header_page;
            std::copy(relationName.begin(), relationName.end(), meta_info->relationName);
            meta_info->attrByteOffset = attrByteOffset;
            meta_info->attrType = attrType;
            meta_info->rootPageNo = rootPageNum;
            
            //init tree
            //treeInit(relationName, bufMgr);
            //fill the tree using filescan
            std::cout << "treeInit\n";
            
            //scanning the record 1 by 1
            FileScan fscan(relationName, bufMgr);
            
            try
            {
                RecordId scanRid;
                while(1)
                {
                    
                    fscan.scanNext(scanRid);
                    //Assuming RECORD.i is our key, lets extract the key, which we know is INTEGER and whose byte offset is also know inside the record.
                    std::string recordStr = fscan.getRecord();
                    const char *record = recordStr.c_str();
                    
                    //extracting the key
                    switch(BTreeIndex::attributeType){
                        case INTEGER:
                        {
                            int key = *((int*)(record + attrByteOffset));
                            std::cout << "INSERATING key: " << key << "\n";
                            BTreeIndex::insertEntry(&key, scanRid);
                        }
                            break;
                        case DOUBLE:
                        {
                            double key = *((double*)(record + attrByteOffset));
                            std::cout << "INSERATING key: " << key << "\n";
                            BTreeIndex::insertEntry(&key, scanRid);
                        }
                            break;
                        case STRING:
                        {
                            
                            char uniqkey[10];
                            std::string key = std::string((char*)(record + attrByteOffset));
                            key.copy(uniqkey, sizeof(uniqkey));
                            std::cout << "INSERATING key: " << key << "\n";
                            BTreeIndex::insertEntry(&key, scanRid);
                            
                        }
                            break;
                        default:
                            break;
                            
                    }
                }
            }
            catch(EndOfFileException e)
            {
                std::cout << "Read all records" << std::endl;
            }
            
            
            bufMgr->flushFile(file); //flush persistent
            
        } catch (FileExistsException e) {
            std::cout << "YUP" << std::endl;
            //check metadata
            file = new BlobFile(outIndexName, false); //no create new
            headerPageNum = 1; //header is always at begin of file
            
            Page *header_page;
            
            bufMgr->readPage(file, headerPageNum, header_page); //get header_page
            bufMgr->unPinPage(file, headerPageNum, false); //not needed anymore => unpin
            
            IndexMetaInfo *meta_info = (IndexMetaInfo *) header_page;
            
            if (relationName.compare(meta_info->relationName) == 0 && meta_info->attrByteOffset == attrByteOffset &&
                meta_info->attrType == attrType) {
                //valid header
                //set rootpage
                rootPageNum = meta_info->rootPageNo;
            }
            else {
                throw BadIndexInfoException("Invalid Header");
                
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
            int keyInt = *(int *) key;
            //START:IF THE ROOT PAGE IS A LEAF
            if (this->rootPageNum == 2)
            {
                //figure out if root is full
                int freeIndex = -1;
                bool full = true;
                Page* temp;
                bufMgr->readPage(file, this->rootPageNum, temp);
                LeafNodeInt * leafNode;
                leafNode = (LeafNodeInt *) temp;
                for(int i = 0; i < nodeOccupancy || !full; i++)
                {
                    if(leafNode->keyArray[i] == -1)
                    {
                        full = false;
                        freeIndex = i;
                        break;
                    }
                }
                if(full)
                {
                    freeIndex = -1;
                }
                //Start: If root is not full and is a leaf
                if (!full)
                {
                    bool done = false;
                    for (int i = freeIndex-1; i >= 0 && !done; i--)
                    {
                        if (leafNode->keyArray[i] > keyInt)
                        {
                            leafNode->keyArray[i+1] = leafNode->keyArray[i];
                            leafNode->ridArray[i+1] = leafNode->ridArray[i];
                        }
                        else
                        {
                            leafNode->keyArray[i+1] = keyInt;
                            leafNode->ridArray[i+1] = rid;
                            done = true;
                        }
                    }
                    bufMgr->unPinPage(file, this->rootPageNum, true);//page changed and thus is dirty
                }//end: if root is not full and is a leaf
                //start: root is full and is a leaf
                else if (full)
                {
                    //BEGIN: SPLIT LEAF
                    //current node is called leafNode
                    //create new leaf node
                    Page * newLeafNodePage;
                    LeafNodeInt * newLeafNode;
                    PageId newLeafNodePageId;
                    bufMgr->allocPage(file, newLeafNodePageId, newLeafNodePage);
                    newLeafNode->rightSibPageNo = leafNode->rightSibPageNo;
                    leafNode->rightSibPageNo = newLeafNodePageId;
                    RecordId blankrid;
                    blankrid.page_number = -1;
                    //initialize new leaf node
                    for(int i = 0; i < leafOccupancy; i++)
                    {
                        newLeafNode->keyArray[i] = -1;
                        newLeafNode->ridArray[i] = blankrid;
                    }
                    //create an array that holds all values to sort
                    RecordId newRidArr [INTARRAYLEAFSIZE + 1];
                    int newKeyArr [INTARRAYLEAFSIZE + 1];
                    for (int i = 0; i < INTARRAYLEAFSIZE + 1; i++)
                    {
                        if (i < INTARRAYLEAFSIZE) {
                            newRidArr[i] = leafNode->ridArray[i];
                            newKeyArr[i] = leafNode->keyArray[i];
                        }
                        else if (i == INTARRAYLEAFSIZE)
                        {
                            newRidArr[i] = rid;
                            newKeyArr[i] = keyInt;
                        }
                    }
                    //sort the array that holds all values
                    bool done = false;
                    for (int i = INTARRAYLEAFSIZE; i >= 0 && !done; i++)
                    {
                        if (newKeyArr[i] > keyInt)
                        {
                            newKeyArr[i+1] = newKeyArr[i];
                            newRidArr[i+1] = newRidArr[i];
                        }
                        else
                        {
                            newKeyArr[i+1] = keyInt;
                            newRidArr[i+1] = rid;
                            done = true;
                        }
                    }
                    
                    //get mid index of that array
                    int midIndex = (INTARRAYLEAFSIZE + 1) % 2;
                    if(midIndex == 0)
                    {
                        midIndex = (INTARRAYLEAFSIZE + 1) / 2;
                    }
                    else
                    {
                        midIndex = ((INTARRAYLEAFSIZE + 1) / 2) + 1;
                    }
                    
                    //split that array among 2 leaf nodes
                    int k = 0;
                    for (int i = midIndex; i < INTARRAYLEAFSIZE + 1; i++)
                    {
                        if (i < INTARRAYLEAFSIZE)
                        {
                            leafNode->keyArray[i] = -1;
                            leafNode->ridArray[i] = blankrid;
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
                    
                    bufMgr->unPinPage(file, newLeafNodePageId, true); //page was dirtied
                    //END; SPLIT LEAF NODE
                    
                    //create non leaf node and insert pushUpKey
                    Page * nonLeafNodePage;
                    PageId nonLeafNodePageId;
                    bufMgr->allocPage(file, nonLeafNodePageId, nonLeafNodePage);
                    NonLeafNodeInt * nonLeafNode = (NonLeafNodeInt *) nonLeafNodePage;
                    //initialize the nonLeafNode
                    nonLeafNode->level = 1;
                    for (int i = 0; i < INTARRAYNONLEAFSIZE; i++)
                    {
                        nonLeafNode->keyArray[i] = -1;
                        nonLeafNode->pageNoArray[i] = -1;
                    }
                    //insert pushUpKey into newNonLeafNode
                    nonLeafNode->keyArray[0] = pushUpKey;
                    //hard coded because old root leaf node was 2
                    nonLeafNode->pageNoArray[0] = 2;
                    nonLeafNode->pageNoArray[1] = newLeafNodePageId;
                    //Set rootpagenum to this new node's pageid
                    this->rootPageNum = nonLeafNodePageId;
                    
                }//end:root is full and is a leaf
            }//END: IF ROOT IS A LEAFNODE
            else //START: ROOT IS A NONLEAFNODE
            {
                 //traverse the tree down to level 1
                 std::stack<PageId>stack;
                 int keyInt = *(int*)key;
                 PageId currentId = this->rootPageNum;
                 PageId leafNodeId = -1;
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
                            if (currentKey == -1 || keyInt <= currentKey)
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
                         leafNodeId = currentId;
                         std::cout << "LeafNodeId: " << leafNodeId << std::endl;
                     }
                }
                
                //see if leaf node is full
                Page * leafNodePage;
                bufMgr->readPage(file, leafNodeId, leafNodePage);
                int freeIndex = -1;
                bool full = true;
                LeafNodeInt * leafNode;
                leafNode = (LeafNodeInt *) leafNodePage;
                for(int i = 0; i < nodeOccupancy || !full; i++)
                {
                    if(leafNode->keyArray[i] == -1)
                    {
                        full = false;
                        freeIndex = i;
                        break;
                    }
                }
                //this is here because it pertains to the above code block
                if(full)
                {
                    freeIndex = -1;
                }
                //START: ROOT IS A NONLEAF NODE AND LEAF NODE IS NOT FULL
                if (!full)
                {
                    bool done = false;
                    for (int i = freeIndex-1; i >= 0 && !done; i--)
                    {
                        if (leafNode->keyArray[i] > keyInt)
                        {
                            leafNode->keyArray[i+1] = leafNode->keyArray[i];
                            leafNode->ridArray[i+1] = leafNode->ridArray[i];
                        }
                        else
                        {
                            leafNode->keyArray[i+1] = keyInt;
                            leafNode->ridArray[i+1] = rid;
                            done = true;
                        }
                    }
                    bufMgr->unPinPage(file, this->rootPageNum, true);//page changed and thus is dirty
                }//END: ROOT IS A NONLEAF NODE AND LEAF NODE IS NOT FULL
                //START: ROOT IS A NONLEAF NODE AND LEAF NODE IS FULL
                else if (full)
                {
                    //BEGIN: SPLIT LEAF
                    //current node is called leafNode
                    //create new leaf node
                    Page * newLeafNodePage;
                    LeafNodeInt * newLeafNode;
                    PageId newLeafNodePageId;
                    bufMgr->allocPage(file, newLeafNodePageId, newLeafNodePage);
                    newLeafNode->rightSibPageNo = leafNode->rightSibPageNo;
                    leafNode->rightSibPageNo = newLeafNodePageId;
                    RecordId blankrid;
                    blankrid.page_number = -1;
                    //initialize new leaf node
                    for(int i = 0; i < leafOccupancy; i++)
                    {
                        newLeafNode->keyArray[i] = -1;
                        newLeafNode->ridArray[i] = blankrid;
                    }
                    //create an array that holds all values to sort
                    RecordId newRidArr [INTARRAYLEAFSIZE + 1];
                    int newKeyArr [INTARRAYLEAFSIZE + 1];
                    for (int i = 0; i < INTARRAYLEAFSIZE + 1; i++)
                    {
                        if (i < INTARRAYLEAFSIZE) {
                            newRidArr[i] = leafNode->ridArray[i];
                            newKeyArr[i] = leafNode->keyArray[i];
                        }
                        else if (i == INTARRAYLEAFSIZE)
                        {
                            newRidArr[i] = rid;
                            newKeyArr[i] = keyInt;
                        }
                    }
                    //sort the array that holds all values
                    bool done = false;
                    for (int i = INTARRAYLEAFSIZE; i >= 0 && !done; i++)
                    {
                        if (leafNode->keyArray[i] > keyInt)
                        {
                            leafNode->keyArray[i+1] = leafNode->keyArray[i];
                            leafNode->ridArray[i+1] = leafNode->ridArray[i];
                        }
                        else
                        {
                            leafNode->keyArray[i+1] = keyInt;
                            leafNode->ridArray[i+1] = rid;
                            done = true;
                        }
                    }
                    
                    //get mid index of that array
                    int midIndex = (INTARRAYLEAFSIZE + 1) % 2;
                    if(midIndex == 0)
                    {
                        midIndex = (INTARRAYLEAFSIZE + 1) / 2;
                    }
                    else
                    {
                        midIndex = ((INTARRAYLEAFSIZE + 1) / 2) + 1;
                    }
                    
                    //split that array among 2 leaf nodes
                    int k = 0;
                    for (int i = midIndex; i < INTARRAYLEAFSIZE + 1; i++)
                    {
                        if (i < INTARRAYLEAFSIZE)
                        {
                            leafNode->keyArray[i] = -1;
                            leafNode->ridArray[i] = blankrid;
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
                    
                    bufMgr->unPinPage(file, newLeafNodePageId, true); //page was dirtied
                    //END; SPLIT LEAF NODE
                    
                    //THERE IS ROOM IN THE PARENT
                    //THERE ISNT ROOM IN THE PARENT
                    done = false;
                    while (!done)
                    {
                        PageId nonLeafNodeCurrentId = stack.top();
                        //find out if nonLeafNode is full
                        int freeIndex = -1;
                        bool full = true;
                        Page * nonLeafNodePage;
                        bufMgr->readPage(file, nonLeafNodeCurrentId, nonLeafNodePage);
                        NonLeafNodeInt * nonLeafNode;
                        nonLeafNode = (NonLeafNodeInt *) nonLeafNodePage;
                        for(int i = 0; i < nodeOccupancy || !full; i++)
                        {
                            if(nonLeafNode->keyArray[i] == -1)
                            {
                                full = false;
                                freeIndex = i;
                            }
                        }
                        if(full)
                        {
                            freeIndex = -1;
                        }
                        //if nonLeafNode has room insert
                        if (!full)
                        {
                            for (int i = freeIndex-1; i >= 0 && !done; i++)
                            {
                                if (nonLeafNode->keyArray[i] > pushUpKey)
                                {
                                    nonLeafNode->keyArray[i+1] = nonLeafNode->keyArray[i];
                                    nonLeafNode->pageNoArray[i+1] = nonLeafNode->pageNoArray[i];
                                }
                                else
                                {
                                    nonLeafNode->keyArray[i+1] = pushUpKey;
                                    nonLeafNode->pageNoArray[i+1] = newLeafNodePageId;
                                }
                            }
                            bufMgr->unPinPage(file, currentId, true);
                            done = true;
                        }
                        //else nonLeafNode does not have room
                        else
                        {
                            //START: SPLIT NONLEAFNODE
                            Page * newNonLeafNodePage;
                            PageId newNonLeafNodePageId;
                            bufMgr->allocPage(file, newNonLeafNodePageId, newNonLeafNodePage);
                            NonLeafNodeInt * newNonLeafNode = (NonLeafNodeInt *) newNonLeafNodePage;
                            
                            //If we are splitting a level 1 nonleafnode the newnonleafnode level is 1
                            if (nonLeafNode->level == 1)
                            {
                                newNonLeafNode->level = 1;
                            }
                            else
                            {
                                newNonLeafNode->level = 0;
                            }
                            
                            //initialize new nonleafnode
                            for(int i = 0; i < INTARRAYNONLEAFSIZE; i++)
                            {
                                newNonLeafNode->keyArray[i] = -1;
                                newNonLeafNode->pageNoArray[i] = -1;
                            }
                            
                            //create and initialize larger array to hold new key and pageid
                            int newKeyArr [INTARRAYNONLEAFSIZE + 1];
                            int newPageNoArray [INTARRAYNONLEAFSIZE + 2];
                            for (int i = 0; i < INTARRAYNONLEAFSIZE + 1; i++)
                            {
                                if (i < INTARRAYNONLEAFSIZE) {
                                    newPageNoArray[i] = nonLeafNode->pageNoArray[i];
                                    newKeyArr[i] = nonLeafNode->keyArray[i];
                                }
                            }
                            
                            //sort the array that holds all values
                            bool done = false;
                            for (int i = INTARRAYNONLEAFSIZE; i >= 0 && !done; i++)
                            {
                                if (newKeyArr[i] > keyInt)
                                {
                                    newKeyArr[i+1] = newKeyArr[i];
                                    newPageNoArray[i+1] = newPageNoArray[i];
                                }
                                else
                                {
                                    newKeyArr[i+1] = pushUpKey;
                                    newPageNoArray[i+1] = newNonLeafNodePageId;
                                    done = true;
                                }
                            }
                            //ENDED HERE
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
                                    node->keyArray[i] = NULL;
                                    node->pageNoArray[i] = NULL;
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
                                    newNode->keyArray[i] = NULL;
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


                    
                    
    
                    //create non leaf node and insert pushUpKey
                    Page * nonLeafNodePage;
                    PageId nonLeafNodePageId;
                    bufMgr->allocPage(file, nonLeafNodePageId, nonLeafNodePage);
                    NonLeafNodeInt * nonLeafNode = (NonLeafNodeInt *) nonLeafNodePage;
                    //initialize the nonLeafNode
                    nonLeafNode->level = 1;
                    for (int i = 0; i < INTARRAYNONLEAFSIZE; i++)
                    {
                        nonLeafNode->keyArray[i] = -1;
                        nonLeafNode->pageNoArray[i] = -1;
                    }
                    //insert pushUpKey into newNonLeafNode
                    nonLeafNode->keyArray[0] = pushUpKey;
                    //hard coded because old root leaf node was 2
                    nonLeafNode->pageNoArray[0] = 2;
                    nonLeafNode->pageNoArray[1] = newLeafNodePageId;
                    //Set rootpagenum to this new node's pageid
                    this->rootPageNum = nonLeafNodePageId;
                    

                }
            }
            
            /*//FIND THE PAGEID
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
            NonLeafNodeInt * nonLeafNode;
            nonLeafNode = (NonLeafNodeInt *) temp;
            for(int i = 0; i < nodeOccupancy || !full; i++)
            {
                if (nonLeafNode->level == 1)
                {
                    bufMgr->readPage(file, nonLeafNode->pageNoArray[i], temp);
                    LeafNodeInt * leafNode;
                    leafNode = (LeafNodeInt *) temp;
                    if(leafNode->ridArray[i].page_number == 0)
                    {
                        full = false;
                        freeIndex = i;
                        break;
                    }
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
                bufMgr->readPage(file, nonLeafNode->pageNoArray[currentId], page);
                LeafNodeInt * leafNode;
                leafNode = (LeafNodeInt*) page;
                for (int i = freeIndex-1; i >= 0 && !done; i++)
                {
                    if (leafNode->keyArray[i] > keyInt)
                    {
                        leafNode->keyArray[i+1] = leafNode->keyArray[i];
                        leafNode->ridArray[i+1] = leafNode->ridArray[i];
                    }
                    else
                    {
                        leafNode->keyArray[i+1] = keyInt;
                        leafNode->ridArray[i+1] = rid;
                    }
                }
                bufMgr->unPinPage(file, currentId, true);//page dirtied
            }
            /*
            // ITS FULL
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
                blankRID.page_number = NULL;
                for(int i = 0; i < leafOccupancy; i++)
                {
                    newLeafNode->keyArray[i] = NULL;
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
                        node->keyArray[i] = NULL;
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
                        if(node->ridArray[i].page_number == NULL)
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
                            newNonLeafNode->keyArray[i] = NULL;
                            newNonLeafNode->pageNoArray[i] = NULL;
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
                                node->keyArray[i] = NULL;
                                node->pageNoArray[i] = NULL;
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
                                newNode->keyArray[i] = NULL;
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
                
            }*/
            
        }//END OF INTEGER
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
        if(scanExecuting)
        {
            endScan();
        }
        lowOp = lowOpParm;
        highOp = highOpParm;
        
        if((highOp != LT && highOp != LTE)|| (lowOp != GT && lowOp != GTE) )
        {
            throw BadOpcodesException();
        }
        
        scanExecuting = true;
        nextEntry = 0;
        currentPageNum = rootPageNum;
        //INTEGER
        
        if (this->attributeType == 0)
        {
            lowValInt = *(int *)lowValParm;
            highValInt = *(int *)highValParm;
            if (lowValInt > highValInt)
            {
                throw BadScanrangeException();
            }
            NonLeafNodeInt * node;
            currentPageNum = rootPageNum;
            bufMgr->readPage(file, currentPageNum, currentPageData);
            bufMgr->unPinPage(file, currentPageNum, currentPageData);
            node = (NonLeafNodeInt *) currentPageData;
            while (node->level == 0)
            {
                bufMgr->readPage(file, currentPageNum, currentPageData);
                bufMgr->unPinPage(file, currentPageNum, false);
                NonLeafNodeInt * node = (NonLeafNodeInt *) currentPageData;
                bool found = false;
                std::cout << node->level << "\n";
                //Note the second else if may not be needed or could be made cleaner
                for (int i = 0; i < INTARRAYNONLEAFSIZE && !found; i++)
                {
                    std::cout << node->keyArray[i] << "\n";
                    if (lowValInt <= node->keyArray[i])
                    {
                        currentPageNum = node->pageNoArray[i];
                        found = true;
                    }
                    if(node->keyArray[i] == NULL)
                    {
                        currentPageNum = node->pageNoArray[i + 1];
                        found = true;
                    }
                    if (i == INTARRAYNONLEAFSIZE - 1)
                    {
                        currentPageNum = node->pageNoArray[INTARRAYNONLEAFSIZE];
                        found = true;
                    }
                }
            }
            //REPIN IT
            bufMgr->readPage(file, currentPageNum, currentPageData);
            LeafNodeInt * leafNode = (LeafNodeInt *) currentPageData;
            bool found = false;
            while (!found)
            {
                for(int i = 0; i < INTARRAYLEAFSIZE && !found; i++)
                {
                    if(leafNode->keyArray[i] == highValInt)
                    {
                        if(highOp != LTE)
                        {
                            throw NoSuchKeyFoundException();
                        }
                        else
                        {
                            found = true;
                        }
                    }
                    if(leafNode->keyArray[i] == lowValInt)
                    {
                        if(lowOp != GTE)
                        {
                            throw NoSuchKeyFoundException();
                        }
                        else
                        {
                            found = true;
                        }
                    }
                    if(leafNode->keyArray[i] < highValInt && leafNode->keyArray[i] > lowValInt)
                    {
                        found = true;
                    }
                    if(leafNode->keyArray[i] > highValInt)
                    {
                        throw NoSuchKeyFoundException();
                    }
                }
                if (!found)
                {
                    bufMgr->unPinPage(file, currentPageNum, false);
                    if (leafNode->rightSibPageNo == NULL)
                    {
                        throw NoSuchKeyFoundException();
                    }
                    currentPageNum = leafNode->rightSibPageNo;
                    bufMgr->readPage(file, currentPageNum, currentPageData);
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
