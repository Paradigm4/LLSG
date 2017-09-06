/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2008-2016 SciDB, Inc.
* All Rights Reserved.
*
* LLSG is a plugin for SciDB, an Open Source Array DBMS maintained
* by Paradigm4. See http://www.paradigm4.com/
*
* LLSG is free software: you can redistribute it and/or modify
* it under the terms of the AFFERO GNU General Public License as published by
* the Free Software Foundation.
*
* LLSG is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
* the AFFERO GNU General Public License for the complete license terms.
*
* You should have received a copy of the AFFERO GNU General Public License
* along with LLSG.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
*
* END_COPYRIGHT
*/


#ifndef LLSG_H_
#define LLSG_H_

#include <query/Operator.h>
#include <array/RLE.h>
#include <util/Network.h>

namespace scidb
{

namespace llsg
{

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.llcj"));

using std::string;
using std::vector;
using std::shared_ptr;

class ArrayReader
{
private:
    shared_ptr<Array>                       _input;
    size_t const                            _numIterators;
    vector<Value const*>                    _tupleInputs;
    Coordinates const*                      _cellCoords;
    vector<shared_ptr<ConstArrayIterator> > _aiters;
    vector<shared_ptr<ConstChunkIterator> > _citers;

public:
    ArrayReader( shared_ptr<Array>const& input):
        _input(input),
        _numIterators( input->getArrayDesc().getAttributes(true).size() ),
        _tupleInputs( _numIterators ),
        _aiters(_numIterators),
        _citers(_numIterators)
    {
        for(size_t i =0; i<_numIterators; ++i)
        {
            _aiters[i] = input->getConstIterator(i);
        }
        if(!end())
        {
            for(size_t i =0; i<_numIterators; ++i)
            {
                _citers[i] = _aiters[i]->getChunk().getConstIterator();
            }
            if(_citers[0]->end())
            {
                next();
            }
            else
            {
                setTuple();
            }
        }
    }

private:
    void setTuple()
    {
        for(size_t i =0; i<_numIterators; ++i)
        {
            Value const* item = &(_citers[i]->getItem());
            _tupleInputs[i] = item;
        }
        _cellCoords = &(_citers[0]->getPosition());
    }

public:
    void next()
    {
        if(!_citers[0]->end())
        {
            for(size_t i =0; i<_numIterators; ++i)
            {
                ++(*_citers[i]);
            }
        }
        while (_citers[0]->end())
        {
            for(size_t i =0; i<_numIterators; ++i)
            {
                ++(*_aiters[i]);
            }
            if(end())
            {
                return;
            }
            for(size_t i =0; i<_numIterators; ++i)
            {
                _citers[i] = _aiters[i]->getChunk().getConstIterator();
            }
        }
        setTuple();
    }

    bool end() const
    {
        return _aiters[0]->end();
    }

    vector<Value const*> const& getAttributes() const
    {
        if(end())
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Internal inconsistency";
        }
        return _tupleInputs;
    }

    Coordinates const& getPosition() const
    {
        if(end())
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Internal inconsistency";
        }
        return *(_cellCoords);
    }
};

class SmallArrayBuffer
{
private:
    size_t const         _nDims;
    size_t const         _nAttrs;
    vector<bool> const   _attrNullable;
    vector<size_t> const _attrSize;

    vector<char> _data;
    size_t*      _numTuplesPtr;
    size_t       _remainingSpace;
    char*        _nextPtr;

public:
    SmallArrayBuffer (size_t const nDims, size_t const nAttrs, vector<bool> const &attrNullable, vector<size_t> const &attrSize,
                      size_t initialSize = 8*1024*1024):
        _nDims             (nDims),
        _nAttrs            (nAttrs),
        _attrNullable      (attrNullable),
        _attrSize          (attrSize),
        _data              (initialSize),
        _numTuplesPtr      (reinterpret_cast<size_t*> (&(_data[0]))),
        _nextPtr           (reinterpret_cast<char*> (_numTuplesPtr+1))
    {
        *_numTuplesPtr = 0;
    }


private:
    inline void expand ()
    {
        size_t curSize = getSize();
        _data.resize (_data.size() * 2);
        _numTuplesPtr = reinterpret_cast<size_t*> (&(_data[0]));
        _nextPtr = &(_data[0]) + curSize;
    }

    inline void addData (void const* data, size_t const size)
    {
        while(size > _data.size() - getSize())
        {
            expand();
        }
        memcpy(_nextPtr, data, size);
        _nextPtr = _nextPtr + size;
    }

    //tuple:= [Coordinate dim1][Coordinate dim2]..[value1][value2][...]
    //value:= ([int8 missing_code])([uint32 size])([data]) //missing_code only present for nullable; size only present for variable-sized types
public:
    void addTuple(Coordinates const& pos, vector<Value const*> const& values)
    {
        ++(*_numTuplesPtr);
        addData( &(pos[0]), _nDims* sizeof(Coordinate) );
        for(size_t att =0; att<_nAttrs; ++att)
        {
            Value const* val = values[att];
            if(_attrNullable[att])
            {
                int8_t const mc = val->getMissingReason();
                addData( &mc, sizeof(int8_t));
                if(mc != -1)
                {
                    continue;
                }
            }
            if(_attrSize[att]==0)
            {
                size_t vs = val->size();
                addData( &vs, sizeof(size_t));
            }
            addData(val->data(), _attrSize[att] != 0 ? _attrSize[att] : val->size());
        }
    }

    shared_ptr<SharedBuffer> getMemBuffer(bool copy = true)
    {
        return shared_ptr<SharedBuffer> (new MemoryBuffer(&(_data[0]), getSize(), copy));
    }

    void addMemBuffer(shared_ptr<SharedBuffer>const &memBuffer)
    {
        size_t* numTuples = static_cast<size_t*>(memBuffer->getData());
        if( (*numTuples) == 0)
        {
            return;
        }
        *_numTuplesPtr = *_numTuplesPtr + *numTuples;
        addData( (void*)(numTuples+1), memBuffer->getSize() - sizeof(size_t));
    }

    size_t getNumTuples() const
    {
        return *_numTuplesPtr;
    }

    size_t getSize() const
    {
        return (_nextPtr - &(_data[0]));
    }

    void addToMemArray(shared_ptr<Array>& result, shared_ptr<Query> query)
    {
        if(getNumTuples() == 0)
        {
            return;
        }
        ArrayDesc const& schema = result->getArrayDesc();
        vector<shared_ptr<ArrayIterator > > oaiters(_nAttrs);
        for(size_t att =0; att<_nAttrs; ++att)
        {
            oaiters[att] = result->getIterator(att);
        }
        vector<shared_ptr<ChunkIterator > > ociters(_nAttrs);
        Coordinates cellPos(_nDims);
        Coordinates newChunkPos(_nDims);
        Coordinates chunkPos;
        vector<Value> buf(_nAttrs);
        char* readPtr = &(_data[0]) + sizeof(size_t);
        for(size_t i=0, n=getNumTuples(); i<n; ++i)
        {
            //read the coordinates of the cell
            for(size_t j=0; j<_nDims; ++j)
            {
                cellPos[j] = *(reinterpret_cast<Coordinate*>(readPtr));
                readPtr += sizeof(Coordinate);
            }
            //find the chunk
            newChunkPos = cellPos;
            schema.getChunkPositionFor(newChunkPos);
            if(chunkPos != newChunkPos)
            {
                if(chunkPos.size()!=0) // not the first chunk
                {
                    for(size_t att =0; att<_nAttrs; ++att)
                    {
                        ociters[att]->flush();
                        ociters[att].reset();
                    }
                }
                chunkPos = newChunkPos;
                for(size_t att=0; att<_nAttrs; ++ att)
                {
                    ociters[att] = oaiters[att]->newChunk(chunkPos).getIterator(query, att == 0 ?
                            ChunkIterator::SEQUENTIAL_WRITE :
                            ChunkIterator::NO_EMPTY_CHECK | ChunkIterator::SEQUENTIAL_WRITE);
                }
            }
            //read the cells
            for(size_t att=0; att<_nAttrs; ++ att)
            {
                ociters[att] -> setPosition(cellPos);
                if(_attrNullable[att])
                {
                    int8_t const mc = *(reinterpret_cast<int8_t*>(readPtr));
                    readPtr += sizeof(int8_t);
                    if(mc != -1)
                    {
                        buf[att].setNull(mc);
                        ociters[att]->writeItem(buf[att]);
                        continue;
                    }
                }
                size_t valSize = _attrSize[att];
                if(valSize == 0)
                {
                    valSize = *(reinterpret_cast<size_t*>(readPtr));
                    readPtr += sizeof(size_t);
                }
                buf[att].setData(readPtr, valSize);
                readPtr += valSize;
                ociters[att]->writeItem(buf[att]);
            }
        }
        for(size_t att =0 ; att<_nAttrs; ++ att)
        {
            ociters[att]->flush();

        }
    }
};

static bool agreeOnBoolean(bool value, shared_ptr<Query>& query)
{
   shared_ptr<SharedBuffer> buf(new MemoryBuffer(NULL, sizeof(bool)));
   InstanceID myId = query->getInstanceID();
   *((bool*) buf->getData()) = value;
   for(InstanceID i=0; i<query->getInstancesCount(); i++)
   {
       if(i != myId)
       {
           BufSend(i, buf, query);
       }
   }
   for(InstanceID i=0; i<query->getInstancesCount(); i++)
   {
       if(i != myId)
       {
           buf = BufReceive(i,query);
           bool otherInstanceVal = *((bool*) buf->getData());
           value = value && otherInstanceVal;
       }
   }
   return value;
}

static shared_ptr<Array> adaptiveReplicate(shared_ptr<Array> & input, shared_ptr<Query>& query, ArrayResPtr const& residency)
{
    if(input->getSupportedAccess() == Array::RANDOM)
    {
        size_t const memLimit = Config::getInstance()->getOption<int>(CONFIG_MERGE_SORT_BUFFER) * 1024 * 1024;
        ArrayDesc const& inputSchema = input->getArrayDesc();
        size_t const nDims = inputSchema.getDimensions().size();
        vector<AttributeDesc> inputAtts = inputSchema.getAttributes(true);
        size_t const nAtts = inputSchema.getAttributes(true).size();
        vector<bool> attNullable(nAtts);
        vector<size_t> attSizes(nAtts);
        for(size_t i=0; i<nAtts; ++i)
        {
            attNullable[i] = inputAtts[i].isNullable();
            attSizes[i] = inputAtts[i].getSize();
        }
        SmallArrayBuffer buffer(nDims, nAtts, attNullable, attSizes);
        ArrayReader reader(input);
        while(!reader.end() && buffer.getSize() < memLimit)
        {
            buffer.addTuple(reader.getPosition(), reader.getAttributes());
            reader.next();
        }
        bool fitsInMemory = buffer.getSize() < memLimit;
        fitsInMemory = agreeOnBoolean(fitsInMemory, query);
        if(fitsInMemory)
        {
            size_t const nInstances = query->getInstancesCount();
            InstanceID myId = query->getInstanceID();
            std::shared_ptr<SharedBuffer> bufToSend = buffer.getMemBuffer();
            for(InstanceID i=0; i<nInstances; i++)
            {
                if(i != myId)
                {
                    BufSend(i, bufToSend, query);
                }
            }
            for(InstanceID i=0; i<nInstances; i++)
            {
                if(i != myId)
                {
                    std::shared_ptr<SharedBuffer> bufReceived = BufReceive(i,query);
                    buffer.addMemBuffer(bufReceived);
                }
            }
            shared_ptr<Array> result(new MemArray(inputSchema, query));
            buffer.addToMemArray(result, query);
            return result;
        }
    }
    return redistributeToRandomAccess(input, createDistribution(psReplication), residency, query);
}

} //namespace llsg

} //namespace scidb


#endif /* LLSG_H_ */
