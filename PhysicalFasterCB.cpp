/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2008-2016 SciDB, Inc.
* All Rights Reserved.
*
* This file is part of the Paradigm4 Enterprise SciDB distribution kit
* and may only be used with a valid Paradigm4 contract and in accord
* with the terms and conditions specified by that contract.
*
* END_COPYRIGHT
*/

/*
 * PhysicalCrossBetween.cpp
 *
 *  Created on: August 15, 2014
 *  Author: Donghui Zhang
 */

#include <query/Operator.h>
#include <array/Metadata.h>
#include <array/Array.h>
#include "BetweenArray.h"
#include <util/SchemaUtils.h>
#include "LLSG.h"

namespace scidb {

class LLSG_PhysicalCrossBetween: public  PhysicalOperator
{
public:
    LLSG_PhysicalCrossBetween(const std::string& logicalName, const std::string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
         PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    virtual PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                                  const std::vector< ArrayDesc> & inputSchemas) const
    {
       return inputBoundaries[0];
    }

    /***
     * CrossBetween is a pipelined operator, hence it executes by returning an iterator-based array to the consumer
     * that overrides the chunkIterator method.
     */
    std::shared_ptr< Array> execute(std::vector< std::shared_ptr< Array> >& inputArrays,
                                      std::shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 2);
        checkOrUpdateIntervals(_schema, inputArrays[0]);

        // Ensure inputArray supports random access, and rangesArray is replicated.
        std::shared_ptr<Array> inputArray = ensureRandomAccess(inputArrays[0], query);

        SCIDB_ASSERT(inputArrays[0]->getArrayDesc().getResidency()->isEqual(_schema.getResidency()));
        SCIDB_ASSERT(inputArray->getArrayDesc().getResidency()->isEqual(_schema.getResidency()));

        std::shared_ptr<Array> rangesArray = llsg::adaptiveReplicate(inputArrays[1], query, inputArray->getArrayDesc().getResidency());

        // Some variables.
        SchemaUtils schemaUtilsInputArray(inputArray);
        SchemaUtils schemaUtilsRangesArray(rangesArray);
        size_t nDims = schemaUtilsInputArray._dims.size();
        assert(nDims*2 == schemaUtilsRangesArray._attrsWithoutET.size());

        // Scan all attributes of the rangesArray simultaneously, and fill in spatialRanges.
        // Set up a MultiConstIterators to process the array iterators simultaneously.
        SpatialRangesPtr spatialRangesPtr = make_shared<SpatialRanges>(nDims);

        vector<std::shared_ptr<ConstIterator> > rangesArrayIters(nDims*2);
        for (size_t i=0; i<nDims*2; ++i) {
            rangesArrayIters[i] = rangesArray->getConstIterator(safe_static_cast<AttributeID>(i));
        }
        MultiConstIterators multiItersRangesArray(rangesArrayIters);
        while (!multiItersRangesArray.end()) {
            // Set up a MultiConstIterators to process the chunk iterators simultaneously.
            vector<std::shared_ptr<ConstIterator> > rangesChunkIters(nDims*2);
            for (size_t i=0; i<nDims*2; ++i) {
                rangesChunkIters[i] = dynamic_pointer_cast<ConstArrayIterator>(rangesArrayIters[i])->getChunk().getConstIterator();
            }
            MultiConstIterators multiItersRangesChunk(rangesChunkIters);
            while (!multiItersRangesChunk.end()) {
                SpatialRange spatialRange(nDims);
                for (size_t i=0; i<nDims; ++i) {
                    const Value& v = dynamic_pointer_cast<ConstChunkIterator>(rangesChunkIters[i])->getItem();
                    spatialRange._low[i] = v.getInt64();
                }
                for (size_t i=nDims; i<nDims*2; ++i) {
                    const Value& v = dynamic_pointer_cast<ConstChunkIterator>(rangesChunkIters[i])->getItem();
                    spatialRange._high[i-nDims] = v.getInt64();
                }
                if (spatialRange.valid()) {
                    spatialRangesPtr->insert(std::move(spatialRange));
                }
                ++ multiItersRangesChunk;
            }
            ++ multiItersRangesArray;
        }

        // Return a CrossBetweenArray.
        spatialRangesPtr->buildIndex();
        return std::shared_ptr< Array>(make_shared<BetweenArray>(_schema, spatialRangesPtr, inputArray));
   }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(LLSG_PhysicalCrossBetween, "cross_between", "_llsg_cb");

}  // namespace scidb
