/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

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
            //create blob file constructor will throw if file does not exist
            this->file = new BlobFile(outIndexName, false);
            
            //File does exist
        }
        catch (FileNotFoundException e)
        {
            //file does not exist we must create a new file
        }
        
        
        
    }
    
    
    // -----------------------------------------------------------------------------
    // BTreeIndex::~BTreeIndex -- destructor
    // -----------------------------------------------------------------------------
    
    BTreeIndex::~BTreeIndex()
    {
    }
    
    // -----------------------------------------------------------------------------
    // BTreeIndex::insertEntry
    // -----------------------------------------------------------------------------
    
    const void BTreeIndex::insertEntry(const void *key, const RecordId rid)
    {
        
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
