#ifndef LFSPARSEHASHTABLE_H_
#define LFSPARSEHASHTABLE_H_

#include <functional>
#include <atomic>
#include <cstdlib>
#include <cstring>
#include <stdexcept>
#include <utility>
#include <memory>
#include <math.h>

#include <x86intrin.h>

//#define SPARSEHASHTABLE_DEBUG 1

#ifdef SPARSEHASHTABLE_DEBUG
#include <map>
#endif

#include "bittwiddlinghacks.hh"

// TODO: add rvalueref support to insert insertOrSet and set operations
// OPTIONAL: for kv pairs larger than a cacheline implement double hashing and storing 50 elements per bucket (1 element bit, 8 small hash bits)*50 + 62 bits for pointer
// OPTIONAL: Implement support for monotonous keys for given key - truncate last bits for number of pairs to fit into cache line - hash, add truncated bits
// OPTIONAL DESIGN: This table design works great with tables that are saved/loaded AND most operations are get/set. with setups where most operations are inserts deletes
// OPTIONAL DESIGN: quickly lead to high memory fragmentation and overall performance degradation. This calls for a design where memory is preallocated. Preallocating whole
// OPTIONAL DESIGN: bucket will have high variance on density even with high load factors, and thus space will be wasted. 
// OPTIONAL DESIGN: I suggest dividing hashtable into sectors say 1024 spots long. Every sector should have a bitmap of used bits, a pointer to array and a function T* idx2ptr(size_t)
// OPTIONAL DESIGN: Every sector should have array of 1024*(target_load_factor + margin) elements preallocated. Everything above that should be allocated/deallocated as needed on per element basis
// OPTIONAL DESIGN: With memory allocation taken care of, the buckets themselves should have following structure {elementsBitmap, locksBitmap(?), secondHashArray(?), indexArray} where indexArray contains
// OPTIONAL DESIGN: indicies to be resolved via relevant sector idx2ptr call. Every index should be of log2(sector size) bits long. Since every sector size is >> bucket size (64 for now) it should 
// OPTIONAL DESIGN: average out the bucket size variance quite a bit. Memory spent per element in best case depends on sector size. With sector size of 8192 it is 24 bits per element with second hashes or 16 without.
// OPTIONAL DESIGN: All this sounds comples but should be fine and will have a lot less fragmentation and little wasted space at target load factor
// OPTIONAL DESIGN: Another option is to have the sector backed up by two lockfree freelist (one continuously preallocated of size sectorSize*loadFactor) another with elements allocated on per element basis of size
// OPTIONAL DESIGN: up to sectorSize*(1 - loadFactor), and have idx intrusively stored in elements to form smaller bucket lists. Space / time complexities for this are not clear, but this should give completely LF reads

template<typename K, typename V, class HashFunc = std::hash<K> >
class LFSparseHashTable
{
#ifdef SPARSEHASHTABLE_DEBUG
public:
    friend int main(int argc, char** argv);
    bool debugPrints;
private:
#endif

    template<typename KK, typename VV, class HH> friend class LFSparseHashTableUtil;

public:

    struct SparseBucketElement
    {
        template<typename KK, typename VV, class HH> friend class LFSparseHashTableUtil;

        K key;
        V value;
    }__attribute__((packed,aligned(1)));

    typedef K key_type;
    typedef V mapped_type;
    typedef SparseBucketElement value_type;
    typedef HashFunc hasher;
    typedef std::size_t size_type;
    typedef std::ptrdiff_t difference_type;
    typedef value_type* pointer;
    typedef const value_type* const_pointer;
    typedef value_type& reference;
    typedef const value_type& const_reference;

    // CANT CHANGE THIS IN LF TABLE! HAS TO BE 64
    // "spin locking" is built around bucket of this size, excuse the caps
    static const unsigned long long HOLY_GRAIL_SIZE = 64; 

    struct SparseBucket
    {
        template<typename KK, typename VV, class HH> friend class LFSparseHashTableUtil;

        unsigned long long elementLocks; //8
        unsigned long long elementBitmap; //8
        SparseBucketElement* elements; //8

        inline void lockElement(unsigned long long pos)
        {
            __builtin_prefetch(this, 1, 3 /* _MM_HINT_T0 */);
            unsigned long long newLockset, oldLockset, expectedLockset = 0;
            expectedLockset = elementLocks & ~(1ULL << pos); // relaxed read from elementLocks
            while(true)
            {
                newLockset = expectedLockset | (1ULL << pos);
                oldLockset = __sync_val_compare_and_swap(&elementLocks, expectedLockset, newLockset);
                if(oldLockset != expectedLockset)
                {
                    expectedLockset = oldLockset &= ~(1ULL << pos);
                }
                else
                    break;
            }
        }
        inline void unlockElement(unsigned long long pos)
        {
            __builtin_prefetch(this, 1, 3 /* _MM_HINT_T0 */);
            unsigned long long newLockset, oldLockset, expectedLockset = 0;
            expectedLockset = elementLocks | (1ULL << pos); // relaxed read from elementLocks
            while(true)
            {
                newLockset = expectedLockset & ~(1ULL << pos);
                oldLockset = __sync_val_compare_and_swap(&elementLocks, expectedLockset, newLockset);
                if(oldLockset != expectedLockset)
                {
                    expectedLockset = oldLockset |= (1ULL << pos);
                }
                else
                    break;
            }
        }
        inline void lockBucket()
        {
            __builtin_prefetch(this, 1, 3);
            unsigned long long newLockset, oldLockset, expectedLockset;
            while(true)
            {
                expectedLockset = 0;
                newLockset = 0xFFFFFFFFFFFFFFFFULL;
                oldLockset = __sync_val_compare_and_swap(&elementLocks, expectedLockset, newLockset);
                if(oldLockset == expectedLockset)
                    break;
            }
        }
        inline void unlockBucket()
        {
            __builtin_prefetch(this, 1, 3);
            unsigned long long newLockset = 0ULL, oldLockset = 0ULL, expectedLockset = 0xFFFFFFFFFFFFFFFFULL;
            oldLockset = __sync_val_compare_and_swap(&elementLocks, expectedLockset, newLockset);
            if(oldLockset != expectedLockset)
                abort();
        }
        inline void unlockBucket(const unsigned long long begin, const unsigned long long end)
        {
            __builtin_prefetch(this, 1, 3 /* _MM_HINT_T0 */);
            unsigned long long lockedBits = (((1ULL << (end - begin + 1ULL)) - 1ULL) << begin);
            if(!lockedBits)
                lockedBits = 0xFFFFFFFFFFFFFFFFULL;
            unsigned long long newLockset, oldLockset, expectedLockset = 0;
            expectedLockset |= lockedBits;
            while(true)
            {
                newLockset = expectedLockset & ~(lockedBits);
                oldLockset = __sync_val_compare_and_swap(&elementLocks, expectedLockset, newLockset);
                if(oldLockset != expectedLockset)
                {
                    expectedLockset = oldLockset | lockedBits;
                }
                else
                    break;
            }
        }
        inline void bitmapSet(unsigned long long pos)
        {
            elementBitmap |= (1ULL << pos);
        }
        inline void bitmapClear(unsigned long long pos)
        {
            elementBitmap &= ~(1ULL << pos);
        }
        inline bool bitmapTest(unsigned long long pos) const
        {
            return elementBitmap & (1ULL << pos);
        }
    };

private:

    SparseBucket * buckets; //8
    unsigned long long maxElements; //8
    double maxLoadFactor; // 8
    double minLoadFactor; // 8
    HashFunc hasherFunc; // padded to 4
    char padding0[28]; // padding to cacheline

    void unlockRange(unsigned long long lockRangeStartIdx, unsigned long long lockRangeEndIdx);

#ifdef SPARSEHASHTABLE_DEBUG
    std::map<unsigned long long, unsigned long long> collisionAudit;
#endif

public:
    LFSparseHashTable(unsigned long long inMaxElements = 1024, double inMaxLoadFactor = 0.5, double inMinLoadFactor = 0.0, const HashFunc & inHasher =
                                      HashFunc());
    ~LFSparseHashTable();
    LFSparseHashTable(const LFSparseHashTable& other);
    LFSparseHashTable& operator=(const LFSparseHashTable& other);

    // below operations are lock-free (as in based on "spinlock" instead of mutex)
    // IMPORTANT: additional flags for set and get allows for implementation of CAS, FETCH_ADD, FETCH_SUB etc. in client code.
    // Example: if (get(false)) { ++value; set ( true ) } is equivalent to FETCH_ADD
    // Example: if (get(false) && (value==expected)) set ( true ); is equivalent to CAS
    bool insert(const K & inKey, const V & inValue/*, bool unlock = true*/);
    bool insertOrSet(const K & inKey, const V & inValue/*, bool unlock = true*/); // NOTE: true if inserted
    bool get(const K & inKey, V & inValue, bool unlock = true); // NOTE: if called with unlock == false, it will try to get the element and leave it locked if found
    bool set(const K & inKey, const V & inValue, bool locked = false); // NOTE: if called with locked == true, it will not try to lock element but will unlock it after set
    bool remove(const K & inKey);
    void unlockBuckets(unsigned long long startBucketIdx, unsigned long long endBucketIdx);

    // these will come useful
    bool lockElement(const K & inKey);
    bool unlockElement(const K & inKey);

    void swap(LFSparseHashTable& other);

    size_t packHash(std::function<bool(SparseBucketElement&)> itemPredicate);
    void processHash(std::function<void(SparseBucketElement&)> itemProcessor);
}__attribute__((aligned(HOLY_GRAIL_SIZE)));

template<typename K, typename V, class HashFunc>
void LFSparseHashTable<K, V, HashFunc>::unlockRange(unsigned long long lockRangeStartIdx, unsigned long long lockRangeEndIdx)
{
    const unsigned long long rangeStartBucketPos = lockRangeStartIdx / HOLY_GRAIL_SIZE;
    const unsigned long long rangeStartBucketOffset = lockRangeStartIdx % HOLY_GRAIL_SIZE;
    const unsigned long long rangeEndBucketPos = lockRangeEndIdx / HOLY_GRAIL_SIZE;
    const unsigned long long rangeEndBucketOffset = lockRangeEndIdx % HOLY_GRAIL_SIZE;
    if(lockRangeEndIdx == lockRangeStartIdx)
    {
        SparseBucket * bucket = &(buckets[rangeStartBucketPos]);
        bucket->unlockElement(rangeStartBucketOffset);
    }
    else if(lockRangeEndIdx > lockRangeStartIdx)
    {
        if(rangeEndBucketPos == rangeStartBucketPos)
        { // same bucket
            buckets[rangeStartBucketPos].unlockBucket(rangeStartBucketOffset, rangeEndBucketOffset);
        }
        else
        {
            buckets[rangeEndBucketPos].unlockBucket(0, rangeEndBucketOffset);
            if(rangeEndBucketPos > rangeStartBucketPos + 1ULL)
                unlockBuckets(rangeStartBucketPos + 1, rangeEndBucketPos - 1);
            buckets[rangeStartBucketPos].unlockBucket(rangeStartBucketOffset, 63ULL);
        }
    }
    else // wrapped the bitch
    {
        buckets[rangeEndBucketPos].unlockBucket(0, rangeEndBucketOffset);
        if(rangeEndBucketPos > 0ULL)
            unlockBuckets(0, rangeEndBucketPos - 1);

        if(maxElements / HOLY_GRAIL_SIZE + (maxElements % HOLY_GRAIL_SIZE ? 1ULL : 0ULL) - rangeStartBucketPos > 1ULL)
            unlockBuckets(rangeStartBucketPos, maxElements / HOLY_GRAIL_SIZE + (maxElements % HOLY_GRAIL_SIZE ? 1ULL : 0ULL));

        // below lines are a hack to solve the following: when wrapping around table on maxElements that is not multiple of HOLY_GRAIL_SIZE
        // remaining lock bits in last bucket remain unset which will deadlock on unlockBuckets. Just flip them here instead of
        // costly checks in set/get code
        unsigned long long lastLockMask = buckets[rangeStartBucketPos].elementBitmap;
        lastLockMask |= lastLockMask >> 1;
        lastLockMask |= lastLockMask >> 2;
        lastLockMask |= lastLockMask >> 4;
        lastLockMask |= lastLockMask >> 8;
        lastLockMask |= lastLockMask >> 16;
        lastLockMask |= lastLockMask >> 32;
        lastLockMask = ~lastLockMask;
        buckets[rangeStartBucketPos].elementLocks |= lastLockMask;

        buckets[rangeStartBucketPos].unlockBucket(rangeStartBucketOffset, 63ULL);
    }
}

template<typename K, typename V, class HashFunc>
void LFSparseHashTable<K, V, HashFunc>::unlockBuckets(unsigned long long startBucketIdx, unsigned long long endBucketIdx)
{

    if(startBucketIdx == endBucketIdx)
        buckets[startBucketIdx].unlockBucket();
    else if(endBucketIdx > startBucketIdx)
    {
        while(endBucketIdx > startBucketIdx)
        {
            buckets[endBucketIdx--].unlockBucket();
        };
        buckets[startBucketIdx].unlockBucket();
    }
    else // wrapped the bitch
    {
        unsigned long long tmp = 0;
        while(endBucketIdx > tmp)
        {
            buckets[endBucketIdx--].unlockBucket();
        };
        buckets[0].unlockBucket();

        tmp = maxElements / HOLY_GRAIL_SIZE + (maxElements % HOLY_GRAIL_SIZE ? 1ULL : 0ULL);
        while(tmp > startBucketIdx)
        {
            buckets[--tmp].unlockBucket();
        };
    }
}

template<typename K, typename V, class HashFunc>
void LFSparseHashTable<K, V, HashFunc>::swap(LFSparseHashTable<K, V, HashFunc>& other)
{
    std::swap(hasherFunc, other.hasherFunc);
    std::swap(buckets, other.buckets);
    std::swap(maxElements, other.maxElements);
    std::swap(maxLoadFactor, other.maxLoadFactor);
    std::swap(minLoadFactor, other.minLoadFactor);
}

template<typename K, typename V, class HashFunc>
LFSparseHashTable<K, V, HashFunc>::LFSparseHashTable(unsigned long long inMaxElements, double inMaxLoadFactor, double inMinLoadFactor,
                                                     const HashFunc & inHasher) :
#ifdef SPARSEHASHTABLE_DEBUG
                                debugPrints(false),
#endif
                                buckets(0), maxElements(inMaxElements), maxLoadFactor(inMaxLoadFactor), minLoadFactor(inMinLoadFactor), hasherFunc(inHasher)
{
    if(minLoadFactor >= maxLoadFactor)
        minLoadFactor = maxLoadFactor / 2.0;
    buckets = (SparseBucket*) std::malloc((maxElements / HOLY_GRAIL_SIZE + (maxElements % HOLY_GRAIL_SIZE ? 1ULL : 0ULL)) * sizeof(SparseBucket));
    memset(buckets, 0, (maxElements / HOLY_GRAIL_SIZE + (maxElements % HOLY_GRAIL_SIZE ? 1ULL : 0ULL)) * sizeof(SparseBucket));
}

template<typename K, typename V, class HashFunc>
LFSparseHashTable<K, V, HashFunc>::~LFSparseHashTable()
{
#ifdef SPARSEHASHTABLE_DEBUG
    std::map<unsigned long long, unsigned long long> distributions;
    unsigned long long totalItems = 0;
#endif
    for(unsigned long long bucketPos = 0; bucketPos < maxElements / HOLY_GRAIL_SIZE + (maxElements % HOLY_GRAIL_SIZE ? 1ULL : 0ULL); ++bucketPos)
    {
#ifdef SPARSEHASHTABLE_DEBUG
        if (debugPrints)
        {
#endif
        SparseBucket * bucket = &(buckets[bucketPos]);
        unsigned long long count = googlerank((const unsigned char*) &(bucket->elementBitmap), HOLY_GRAIL_SIZE);
        SparseBucketElement * bucketElements = bucket->elements;
        for(unsigned long long j = 0; j < count; ++j)
        {
            bucketElements[j].~SparseBucketElement();
        }
#ifdef SPARSEHASHTABLE_DEBUG
        ++(distributions[count]);
        totalItems += count;
    }
#endif
        std::free(bucket->elements);
    }
#ifdef SPARSEHASHTABLE_DEBUG
    if (debugPrints)
    {
        printf("minLoadFactor:%f, maxLoadFactor:%f, maxElements:%d\n", minLoadFactor, maxLoadFactor, maxElements);

        for (unsigned long long idx = 0; idx <= HOLY_GRAIL_SIZE; ++idx)
        {
            double percentage = (double)distributions[idx]*100; //*idx*100;
            percentage /= (double)(maxElements/HOLY_GRAIL_SIZE);//(double)totalItems;
            if (percentage != 0.0)
            {
                printf("%.10lu buckets of %.10lu elements, %f percent: ", distributions[idx], idx, percentage);
                unsigned long long percents = ceil(percentage);
                for (unsigned long long i = 0; i < percents; ++i)
                printf("*");
                printf("\n");
            }
        }
        printf("\n");

        totalItems = 0;
        for (std::map<unsigned long long, unsigned long long>::iterator it = collisionAudit.begin(); it != collisionAudit.end(); ++it)
        {
            totalItems += it->value;
        }
        for (std::map<unsigned long long, unsigned long long>::iterator it = collisionAudit.begin(); it != collisionAudit.end(); ++it)
        {
            double percentage = it->value*100; //*idx*100;
            percentage /= (double)(totalItems);//(double)totalItems;
            if (percentage != 0.0)
            {
                printf("%.10lu length run %f percent: ", it->key, percentage);
                unsigned long long percents = ceil(percentage);
                for (unsigned long long i = 0; i < percents; ++i)
                printf("*");
                printf("\n");
            }
        }
        printf("\n");
    }
#endif
    std::free(buckets);
}

template<typename K, typename V, class HashFunc>
LFSparseHashTable<K, V, HashFunc>::LFSparseHashTable(const LFSparseHashTable& other) :
                buckets(0), maxElements(other.maxElements), maxLoadFactor(other.maxLoadFactor), minLoadFactor(other.minLoadFactor), hasherFunc(other.hasherFunc)
{
    if(minLoadFactor >= maxLoadFactor)
        minLoadFactor = maxLoadFactor / 2.0;
    buckets = (SparseBucket*) std::malloc((maxElements / HOLY_GRAIL_SIZE + (maxElements % HOLY_GRAIL_SIZE ? 1ULL : 0ULL)) * sizeof(SparseBucket));
    memset(buckets, 0, (maxElements / HOLY_GRAIL_SIZE + (maxElements % HOLY_GRAIL_SIZE ? 1ULL : 0ULL)) * sizeof(SparseBucket));
    for(unsigned long long bucketPos = 0; bucketPos < other.maxElements / HOLY_GRAIL_SIZE + (maxElements % HOLY_GRAIL_SIZE ? 1ULL : 0ULL); ++bucketPos)
    {
        buckets[bucketPos] = other.buckets[bucketPos];
        unsigned long long count = googlerank((const unsigned char*) &(buckets[bucketPos]->elementBitmap), HOLY_GRAIL_SIZE);
        buckets[bucketPos].elements = (SparseBucketElement *) std::malloc(count * sizeof(SparseBucketElement));
        memcpy(buckets[bucketPos].elements, other.buckets[bucketPos].elements, count * sizeof(SparseBucketElement));
    }
}

template<typename K, typename V, class HashFunc>
LFSparseHashTable<K, V, HashFunc>& LFSparseHashTable<K, V, HashFunc>::operator=(const LFSparseHashTable& other)
{
    if(&other != this)
    {
        LFSparseHashTable<K, V, HashFunc> tmpTable(other);
        swap(tmpTable);
    }
    return *this;
}

// returns true if inserted, false if set
template<typename K, typename V, class HashFunc>
bool LFSparseHashTable<K, V, HashFunc>::insertOrSet(const K & inKey, const V & inValue)
{
    unsigned long long idx = hasherFunc(inKey) % maxElements; // TODO: seed
    unsigned long long bucketPos = idx / HOLY_GRAIL_SIZE;
    unsigned long long startBucketIdx = bucketPos, endBucketIdx = bucketPos;
    unsigned long long bucketOffset = idx % HOLY_GRAIL_SIZE;
    SparseBucket * bucket = &(buckets[bucketPos]);
    __builtin_prefetch(bucket, 1, 3 /* _MM_HINT_T0 */);
    bucket->lockBucket();
    SparseBucketElement * bucketElements = bucket->elements;
    if(bucketElements == 0)
    {
        bucket->elements = (SparseBucketElement *) std::malloc(sizeof(SparseBucketElement));
        bucketElements = bucket->elements;
        new (&(bucketElements[0].key)) K(inKey);
        new (&(bucketElements[0].value)) V(inValue);
        bucket->bitmapSet(bucketOffset);
        unlockBuckets(startBucketIdx, endBucketIdx);
        return true;
    }
    else
    {
        bool stepBack = false; // to avoid extra rank calculation later
        unsigned long long rank = googlerank((const unsigned char*) &(bucket->elementBitmap), bucketOffset + 1);
        while(true) // linear probing
        {
            bool elementExists = bucket->bitmapTest(bucketOffset);
            if(elementExists)
            {
                if(bucketElements[rank - 1].key == inKey)
                {
                    bucketElements[rank - 1].value = inValue; // possible move assignment
                    unlockBuckets(startBucketIdx, endBucketIdx);
                    return false;
                }
                else
                {
                    stepBack = true;
                    ++rank;
                    ++bucketOffset;
                    idx = (idx + 1) % maxElements;
                    if(bucketOffset == HOLY_GRAIL_SIZE || !idx)
                    {
                        rank = 1;
                        bucketPos = idx / HOLY_GRAIL_SIZE;
                        bucketOffset = idx % HOLY_GRAIL_SIZE;
                        bucket = &(buckets[bucketPos]);
                        __builtin_prefetch(bucket, 1, 3 /* _MM_HINT_T0 */);
                        bucket->lockBucket();
                        endBucketIdx = bucketPos;
                        bucketElements = bucket->elements;
                        if(bucketElements == 0)
                        {
                            bucket->elements = (SparseBucketElement *) std::malloc(sizeof(SparseBucketElement));
                            bucketElements = bucket->elements;
                            new (&(bucketElements[0].key)) K(inKey); // possible move construction
                            new (&(bucketElements[0].value)) V(inValue);
                            bucket->bitmapSet(bucketOffset);
                            unlockBuckets(startBucketIdx, endBucketIdx);
                            return true;
                        }
                    }
                }
            }
            else
            {
                if(stepBack)
                    --rank;
                unsigned long long count = googlerank((const unsigned char*) &(bucket->elementBitmap), HOLY_GRAIL_SIZE);
                bucketElements = (SparseBucketElement *) std::realloc(bucketElements, (count + 1) * sizeof(SparseBucketElement));
                bucket->elements = bucketElements;
                if(rank < count)
                    memmove(bucketElements + rank + 1, bucketElements + rank, (count - rank) * sizeof(SparseBucketElement));
                new (&(bucketElements[rank].key)) K(inKey); // possible move construction
                new (&(bucketElements[rank].value)) V(inValue);
                bucket->bitmapSet(bucketOffset);
                unlockBuckets(startBucketIdx, endBucketIdx);
                return true;
            }
        }
    }
}

template<typename K, typename V, class HashFunc>
bool LFSparseHashTable<K, V, HashFunc>::get(const K & inKey, V & value, bool unlock)
{
    __builtin_prefetch(&value, 1, 3 /* _MM_HINT_T0 */);
    unsigned long long idx = hasherFunc(inKey) % maxElements; // TODO: seed
    unsigned long long bucketPos = idx / HOLY_GRAIL_SIZE;
    unsigned long long bucketOffset = idx % HOLY_GRAIL_SIZE;
    unsigned long long lockRangeStart = idx;
    unsigned long long lockRangeEnd = idx;
    SparseBucket * bucket = &(buckets[bucketPos]);
    __builtin_prefetch(bucket, 1, 3 /* _MM_HINT_T0 */);
    bucket->lockElement(bucketOffset);
    SparseBucketElement * bucketElements = bucket->elements;
#ifdef SPARSEHASHTABLE_DEBUG
    unsigned long long collisions = 0;
#endif
    unsigned long long rank = googlerank((const unsigned char*) &(bucket->elementBitmap), bucketOffset + 1);
    while(bucket->bitmapTest(bucketOffset)) // linear probing
    {
        if(bucketElements && bucketElements[rank - 1].key == inKey)
        {
#ifdef SPARSEHASHTABLE_DEBUG
            ++(collisionAudit[collisions]);
#endif
            value = bucketElements[rank - 1].value; // read value
            if(unlock)
            {
                if(lockRangeStart == lockRangeEnd) // unlock element
                {
                    bucket->unlockElement(bucketOffset);
                }
                else // or element range is there were collisions
                {
                    unlockRange(lockRangeStart, lockRangeEnd);
                }
            }
            return true;
        }
        else
        {
#ifdef SPARSEHASHTABLE_DEBUG
            ++collisions;
#endif
            ++rank;
            ++bucketOffset;
            lockRangeEnd = (lockRangeEnd + 1) % maxElements;
            idx = (idx + 1) % maxElements;
            if(bucketOffset == HOLY_GRAIL_SIZE || !idx)
            {
                rank = 1;
                bucketPos = idx / HOLY_GRAIL_SIZE;
                bucketOffset = idx % HOLY_GRAIL_SIZE;
                bucket = &(buckets[bucketPos]);
                __builtin_prefetch(bucket, 1, 3 /* _MM_HINT_T0 */);
                bucket->lockElement(bucketOffset);
                bucketElements = bucket->elements;
            }
            else
                bucket->lockElement(bucketOffset);
        }
    }
    if(unlock)
    {
        if(lockRangeStart == lockRangeEnd) // unlock element
        {
            bucket->unlockElement(bucketOffset);
        }
        else // or element range is there were collisions
        {
            unlockRange(lockRangeStart, lockRangeEnd);
        }
    }
    return false;
}

template<typename K, typename V, class HashFunc>
bool LFSparseHashTable<K, V, HashFunc>::remove(const K & inKey)
{
    unsigned long long idx = hasherFunc(inKey) % maxElements; // TODO: seed
    unsigned long long bucketPos = idx / HOLY_GRAIL_SIZE;
    unsigned long long startBucketIdx = bucketPos, endBucketIdx = bucketPos;
    unsigned long long bucketOffset = idx % HOLY_GRAIL_SIZE;
    SparseBucket * bucket = &(buckets[bucketPos]);
    __builtin_prefetch(bucket, 1, 3 /* _MM_HINT_T0 */);
    bucket->lockBucket();
    SparseBucketElement * bucketElements = bucket->elements;
    bool elementFound = false;

    // first, find the matching record
    unsigned long long rank;
    while(bucket->bitmapTest(bucketOffset))
    {
        if(bucketElements)
            rank = googlerank((const unsigned char*) &(bucket->elementBitmap), bucketOffset + 1);
        if(bucketElements && bucketElements[rank - 1].key == inKey)
        {
            elementFound = true;
            break;
        }
        ++bucketOffset;
        idx = (idx + 1) % maxElements;
        if(bucketOffset == HOLY_GRAIL_SIZE || !idx)
        {
            bucketPos = idx / HOLY_GRAIL_SIZE;
            bucketOffset = idx % HOLY_GRAIL_SIZE;
            bucket = &(buckets[bucketPos]);
            __builtin_prefetch(bucket, 1, 3 /* _MM_HINT_T0 */);
            bucket->lockBucket();
            endBucketIdx = bucketPos;
            bucketElements = bucket->elements;
        }
    }
    if(!elementFound)
    {
        unlockBuckets(startBucketIdx, endBucketIdx);
        return false;
    }
    else
    {
        bucketElements[rank - 1].~SparseBucketElement(); // destroy the deleted element
    }

    // walk forward until next hole to see if any records need to be moved back
    unsigned long long deletedIdx = idx;
    ++bucketOffset;
    idx = (idx + 1) % maxElements;
    if(bucketOffset == HOLY_GRAIL_SIZE || !idx)
    {
        rank = 0; // if there are window elements rank will be incremented later
        bucketPos = idx / HOLY_GRAIL_SIZE;
        bucketOffset = idx % HOLY_GRAIL_SIZE;
        bucket = &(buckets[bucketPos]);
        __builtin_prefetch(bucket, 1, 3 /* _MM_HINT_T0 */);
        bucket->lockBucket();
        endBucketIdx = bucketPos;
        bucketElements = bucket->elements;
        bucketElements = (SparseBucketElement *) (((unsigned long long) bucketElements) & (~15ULL));
    }
    while(bucketElements && (bucket->bitmapTest(bucketOffset)))
    {
        // calc hash. If its less or equal new hole position, swap, save new hole and move on
        ++rank;
        if(bucketElements && hasherFunc(bucketElements[rank - 1].key) % maxElements <= deletedIdx)
        {
            // swap
            unsigned int swapWindowPos = deletedIdx / HOLY_GRAIL_SIZE;
            unsigned int swapWindowOffset = deletedIdx % HOLY_GRAIL_SIZE;
            SparseBucket * swapBucket = &(buckets[swapWindowPos]);
            SparseBucketElement * swapWindowElements = swapBucket->elements;
            unsigned long long swaprank = googlerank((const unsigned char*) &(swapBucket->elementBitmap), swapWindowOffset + 1);
            swapWindowElements[swaprank - 1] = bucketElements[rank - 1];
            deletedIdx = idx;
        }
        ++bucketOffset;
        idx = (idx + 1) % maxElements;
        if(bucketOffset == HOLY_GRAIL_SIZE || !idx)
        {
            rank = 0; // if there are window elements rank will be incremented later
            bucketPos = idx / HOLY_GRAIL_SIZE;
            bucketOffset = idx % HOLY_GRAIL_SIZE;
            bucket = &(buckets[bucketPos]);
            __builtin_prefetch(bucket, 1, 3 /* _MM_HINT_T0 */);
            bucket->lockBucket();
            endBucketIdx = bucketPos;
            bucketElements = bucket->elements;
        }
    }

    // remove record
    unsigned long long deletedBucketPos = deletedIdx / HOLY_GRAIL_SIZE;
    unsigned long long deletedBucketOffset = deletedIdx % HOLY_GRAIL_SIZE;
    SparseBucket * deletedBucket = &(buckets[deletedBucketPos]);
    SparseBucketElement * deletedBucketElements = deletedBucket->elements;
    unsigned long long deletedCount = googlerank((const unsigned char*) &(deletedBucket->elementBitmap), HOLY_GRAIL_SIZE);
    if(deletedCount == 1)
    {
        std::free(deletedBucketElements);
        deletedBucket->elements = deletedBucketElements = 0;
    }
    else
    {
        unsigned long long deletedrank = googlerank((const unsigned char*) &(deletedBucket->elementBitmap), deletedBucketOffset + 1);
        if(deletedrank < deletedCount)
            memmove(deletedBucketElements + deletedrank - 1, deletedBucketElements + deletedrank, (deletedCount - deletedrank) * sizeof(SparseBucketElement));
        deletedBucketElements = (SparseBucketElement *) std::realloc(deletedBucketElements, (deletedCount - 1) * sizeof(SparseBucketElement));
        deletedBucket->elements = deletedBucketElements;
    }
    deletedBucket->bitmapClear(deletedBucketOffset);
    unlockBuckets(startBucketIdx, endBucketIdx);
    return true;
}

template<typename K, typename V, class HashFunc>
bool LFSparseHashTable<K, V, HashFunc>::insert(const K & inKey, const V & inValue)
{
    unsigned long long idx = hasherFunc(inKey) % maxElements; // TODO: seed
    unsigned long long bucketPos = idx / HOLY_GRAIL_SIZE;
    unsigned long long startBucketIdx = bucketPos, endBucketIdx = bucketPos;
    unsigned long long bucketOffset = idx % HOLY_GRAIL_SIZE;
    SparseBucket * bucket = &(buckets[bucketPos]);
    __builtin_prefetch(bucket, 1, 3 /* _MM_HINT_T0 */);
    bucket->lockBucket();
    SparseBucketElement * bucketElements = bucket->elements;
    if(bucketElements == 0)
    {
        bucket->elements = (SparseBucketElement *) std::malloc(sizeof(SparseBucketElement));
        bucketElements = bucket->elements;
        new (&(bucketElements[0].key)) K(inKey);
        new (&(bucketElements[0].value)) V(inValue);
        bucket->bitmapSet(bucketOffset);
        unlockBuckets(startBucketIdx, endBucketIdx);
        return true;
    }
    else
    {
        bool stepBack = false; // to avoid extra rank calculation later
        unsigned long long rank = googlerank((const unsigned char*) &(bucket->elementBitmap), bucketOffset + 1);
        while(true) // linear probing
        {
            bool elementExists = bucket->bitmapTest(bucketOffset);
            if(elementExists)
            {
                if(bucketElements[rank - 1].key == inKey)
                {
                    unlockBuckets(startBucketIdx, endBucketIdx);
                    return false;
                }
                else
                {
                    stepBack = true;
                    ++rank;
                    ++bucketOffset;
                    idx = (idx + 1) % maxElements;
                    if(bucketOffset == HOLY_GRAIL_SIZE || !idx)
                    {
                        rank = 1;
                        bucketPos = idx / HOLY_GRAIL_SIZE;
                        bucketOffset = idx % HOLY_GRAIL_SIZE;
                        bucket = &(buckets[bucketPos]);
                        __builtin_prefetch(bucket, 1, 3 /* _MM_HINT_T0 */);
                        bucket->lockBucket();
                        endBucketIdx = bucketPos;
                        bucketElements = bucket->elements;
                        if(bucketElements == 0)
                        {
                            bucket->elements = (SparseBucketElement *) std::malloc(sizeof(SparseBucketElement));
                            bucketElements = bucket->elements;
                            new (&(bucketElements[0].key)) K(inKey);
                            new (&(bucketElements[0].value)) V(inValue);
                            bucket->bitmapSet(bucketOffset);
                            unlockBuckets(startBucketIdx, endBucketIdx);
                            return true;
                        }
                    }
                }
            }
            else
            {
                if(stepBack)
                    --rank;
                unsigned long long count = googlerank((const unsigned char*) &(bucket->elementBitmap), HOLY_GRAIL_SIZE);
                bucketElements = (SparseBucketElement *) std::realloc(bucketElements, (count + 1) * sizeof(SparseBucketElement));
                bucket->elements = bucketElements;
                if(rank < count)
                    memmove(bucketElements + rank + 1, bucketElements + rank, (count - rank) * sizeof(SparseBucketElement));
                new (&(bucketElements[rank].key)) K(inKey);
                new (&(bucketElements[rank].value)) V(inValue);
                bucket->bitmapSet(bucketOffset);
                unlockBuckets(startBucketIdx, endBucketIdx);
                return true;
            }
        }
    }
}

template<typename K, typename V, class HashFunc>
bool LFSparseHashTable<K, V, HashFunc>::set(const K & inKey, const V & inValue, bool locked)
{
    unsigned long long idx = hasherFunc(inKey) % maxElements; // TODO: seed
    unsigned long long bucketPos = idx / HOLY_GRAIL_SIZE;
    unsigned long long bucketOffset = idx % HOLY_GRAIL_SIZE;
    unsigned long long lockRangeStart = idx;
    unsigned long long lockRangeEnd = idx;
    SparseBucket * bucket = &(buckets[bucketPos]);
    __builtin_prefetch(bucket, 1, 3 /* _MM_HINT_T0 */);
    if(!locked)
        bucket->lockElement(bucketOffset);
    SparseBucketElement * bucketElements = bucket->elements;
#ifdef SPARSEHASHTABLE_DEBUG
    unsigned long long collisions = 0;
#endif
    unsigned long long rank = googlerank((const unsigned char*) &(bucket->elementBitmap), bucketOffset + 1);
    while(bucket->bitmapTest(bucketOffset)) // linear probing
    {
        if(bucketElements && bucketElements[rank - 1].key == inKey)
        {
#ifdef SPARSEHASHTABLE_DEBUG
            ++(collisionAudit[collisions]);
#endif
            bucketElements[rank - 1].value = inValue; // write value
            if(lockRangeStart == lockRangeEnd) // unlock element
            {
                bucket->unlockElement(bucketOffset);
            }
            else // or element range is there were collisions
            {
                unlockRange(lockRangeStart, lockRangeEnd);
            }
            return true;
        }
        else
        {
#ifdef SPARSEHASHTABLE_DEBUG
            ++collisions;
#endif
            ++rank;
            ++bucketOffset;
            lockRangeEnd = (lockRangeEnd + 1) % maxElements;
            idx = (idx + 1) % maxElements;
            if(bucketOffset == HOLY_GRAIL_SIZE || !idx)
            {
                rank = 1;
                bucketPos = idx / HOLY_GRAIL_SIZE;
                bucketOffset = idx % HOLY_GRAIL_SIZE;
                bucket = &(buckets[bucketPos]);
                __builtin_prefetch(bucket, 1, 3 /* _MM_HINT_T0 */);
                if(!locked)
                    bucket->lockElement(bucketOffset);
                bucketElements = bucket->elements;
            }
            else
            {
                if(!locked)
                    bucket->lockElement(bucketOffset);
            }
        }
    }
    if(lockRangeStart == lockRangeEnd) // unlock element
    {
        bucket->unlockElement(bucketOffset);
    }
    else // or element range is there were collisions
    {
        unlockRange(lockRangeStart, lockRangeEnd);
    }
    return false;
}

template<typename K, typename V, class HashFunc>
bool LFSparseHashTable<K, V, HashFunc>::lockElement(const K & inKey)
{
    unsigned long long idx = hasherFunc(inKey) % maxElements; // TODO: seed
    unsigned long long bucketPos = idx / HOLY_GRAIL_SIZE;
    unsigned long long bucketOffset = idx % HOLY_GRAIL_SIZE;
    unsigned long long lockRangeStart = idx;
    unsigned long long lockRangeEnd = idx;
    SparseBucket * bucket = &(buckets[bucketPos]);
    __builtin_prefetch(bucket, 1, 3 /* _MM_HINT_T0 */);
    bucket->lockElement(bucketOffset);
    SparseBucketElement * bucketElements = bucket->elements;
    unsigned long long rank = googlerank((const unsigned char*) &(bucket->elementBitmap), bucketOffset + 1);
    while(bucket->bitmapTest(bucketOffset)) // linear probing
    {
        if(bucketElements && bucketElements[rank - 1].key == inKey)
        {
            return true;
        }
        else
        {
            ++rank;
            ++bucketOffset;
            lockRangeEnd = (lockRangeEnd + 1) % maxElements;
            idx = (idx + 1) % maxElements;
            if(bucketOffset == HOLY_GRAIL_SIZE || !idx)
            {
                rank = 1;
                bucketPos = idx / HOLY_GRAIL_SIZE;
                bucketOffset = idx % HOLY_GRAIL_SIZE;
                bucket = &(buckets[bucketPos]);
                __builtin_prefetch(bucket, 1, 3 /* _MM_HINT_T0 */);
                bucket->lockElement(bucketOffset);
                bucketElements = bucket->elements;
            }
            else
                bucket->lockElement(bucketOffset);
        }
    }
    if(lockRangeStart == lockRangeEnd) // unlock element
    {
        bucket->unlockElement(bucketOffset);
    }
    else // or element range is there were collisions
    {
        unlockRange(lockRangeStart, lockRangeEnd);
    }
    return false;
}

template<typename K, typename V, class HashFunc>
bool LFSparseHashTable<K, V, HashFunc>::unlockElement(const K & inKey)
{
    unsigned long long idx = hasherFunc(inKey) % maxElements; // TODO: seed
    unsigned long long bucketPos = idx / HOLY_GRAIL_SIZE;
    unsigned long long bucketOffset = idx % HOLY_GRAIL_SIZE;
    unsigned long long lockRangeStart = idx;
    unsigned long long lockRangeEnd = idx;
    SparseBucket * bucket = &(buckets[bucketPos]);
    __builtin_prefetch(bucket, 1, 3 /* _MM_HINT_T0 */);
    SparseBucketElement * bucketElements = bucket->elements;
    unsigned long long rank = googlerank((const unsigned char*) &(bucket->elementBitmap), bucketOffset + 1);
    while(bucket->bitmapTest(bucketOffset)) // linear probing
    {
        if(bucketElements && bucketElements[rank - 1].key == inKey)
        {
            if(lockRangeStart == lockRangeEnd) // unlock element
            {
                bucket->unlockElement(bucketOffset);
            }
            else // or element range is there were collisions
            {
                unlockRange(lockRangeStart, lockRangeEnd);
            }
            return true;
        }
        else
        {
            ++rank;
            ++bucketOffset;
            lockRangeEnd = (lockRangeEnd + 1) % maxElements;
            idx = (idx + 1) % maxElements;
            if(bucketOffset == HOLY_GRAIL_SIZE || !idx)
            {
                rank = 1;
                bucketPos = idx / HOLY_GRAIL_SIZE;
                bucketOffset = idx % HOLY_GRAIL_SIZE;
                bucket = &(buckets[bucketPos]);
                __builtin_prefetch(bucket, 1, 3 /* _MM_HINT_T0 */);
                bucketElements = bucket->elements;
            }
        }
    }
    return false;
}

template<typename K, typename V, class HashFunc>
size_t LFSparseHashTable<K, V, HashFunc>::packHash(std::function<bool(SparseBucketElement&)> itemPredicate)
{
    size_t numElementsProcessed = 0;
    for(unsigned long long packBucketPos = 0; packBucketPos < maxElements / HOLY_GRAIL_SIZE + (maxElements % HOLY_GRAIL_SIZE ? 1ULL : 0ULL); ++packBucketPos)
    {
        SparseBucket * packBucket = &(buckets[packBucketPos]);
        packBucket->lockBucket();
        unsigned long long packElementCount = googlerank((const unsigned char*) &(packBucket->elementBitmap), HOLY_GRAIL_SIZE);
        SparseBucketElement * packBucketElements = packBucket->elements;
        for(unsigned long long j = 0; j < packElementCount; ++j)
        {
            bool predicateRes = !itemPredicate(packBucketElements[j]);
            if(predicateRes)
            {
                ++numElementsProcessed;
                unsigned long long idx = hasherFunc(packBucketElements[j].key) % maxElements; // TODO: seed
                unsigned long long bucketPos = idx / HOLY_GRAIL_SIZE;
                unsigned long long startBucketIdx = bucketPos, endBucketIdx = bucketPos;
                unsigned long long bucketOffset = idx % HOLY_GRAIL_SIZE;
                SparseBucket * bucket = &(buckets[bucketPos]);
                __builtin_prefetch(bucket, 1, 3 /* _MM_HINT_T0 */);
                SparseBucketElement * bucketElements = bucket->elements;

                unsigned long long rank;
                rank = googlerank((const unsigned char*) &(bucket->elementBitmap), bucketOffset + 1);
                bucketElements[rank - 1].~SparseBucketElement(); // destroy the deleted element

                // walk forward until next hole to see if any records need to be moved back
                unsigned long long deletedIdx = idx;
                ++bucketOffset;
                idx = (idx + 1) % maxElements;
                if(bucketOffset == HOLY_GRAIL_SIZE || !idx)
                {
                    rank = 0; // if there are window elements rank will be incremented later
                    bucketPos = idx / HOLY_GRAIL_SIZE;
                    bucketOffset = idx % HOLY_GRAIL_SIZE;
                    bucket = &(buckets[bucketPos]);
                    __builtin_prefetch(bucket, 1, 3 /* _MM_HINT_T0 */);
                    bucket->lockBucket();
                    endBucketIdx = bucketPos;
                    bucketElements = bucket->elements;
                    bucketElements = (SparseBucketElement *) (((unsigned long long) bucketElements) & (~15ULL));
                }
                while(bucketElements && (bucket->bitmapTest(bucketOffset)))
                {
                    // calc hash. If its less or equal new hole position, swap, save new hole and move on
                    ++rank;
                    if(bucketElements && hasherFunc(bucketElements[rank - 1].key) % maxElements <= deletedIdx)
                    {
                        // swap
                        unsigned int swapWindowPos = deletedIdx / HOLY_GRAIL_SIZE;
                        unsigned int swapWindowOffset = deletedIdx % HOLY_GRAIL_SIZE;
                        SparseBucket * swapBucket = &(buckets[swapWindowPos]);
                        SparseBucketElement * swapWindowElements = swapBucket->elements;
                        unsigned long long swaprank = googlerank((const unsigned char*) &(swapBucket->elementBitmap), swapWindowOffset + 1);
                        swapWindowElements[swaprank - 1] = bucketElements[rank - 1];
                        deletedIdx = idx;
                    }
                    ++bucketOffset;
                    idx = (idx + 1) % maxElements;
                    if(bucketOffset == HOLY_GRAIL_SIZE || !idx)
                    {
                        rank = 0; // if there are window elements rank will be incremented later
                        bucketPos = idx / HOLY_GRAIL_SIZE;
                        bucketOffset = idx % HOLY_GRAIL_SIZE;
                        bucket = &(buckets[bucketPos]);
                        __builtin_prefetch(bucket, 1, 3 /* _MM_HINT_T0 */);
                        bucket->lockBucket();
                        endBucketIdx = bucketPos;
                        bucketElements = bucket->elements;
                    }
                }

                // remove record
                unsigned long long deletedBucketPos = deletedIdx / HOLY_GRAIL_SIZE;
                unsigned long long deletedBucketOffset = deletedIdx % HOLY_GRAIL_SIZE;
                SparseBucket * deletedBucket = &(buckets[deletedBucketPos]);
                SparseBucketElement * deletedBucketElements = deletedBucket->elements;
                unsigned long long deletedCount = googlerank((const unsigned char*) &(deletedBucket->elementBitmap), HOLY_GRAIL_SIZE);
                if(deletedCount == 1)
                {
                    std::free(deletedBucketElements);
                    deletedBucket->elements = deletedBucketElements = 0;
                }
                else
                {
                    unsigned long long deletedrank = googlerank((const unsigned char*) &(deletedBucket->elementBitmap), deletedBucketOffset + 1);
                    if(deletedrank < deletedCount)
                        memmove(deletedBucketElements + deletedrank - 1, deletedBucketElements + deletedrank,
                                (deletedCount - deletedrank) * sizeof(SparseBucketElement));
                    deletedBucketElements = (SparseBucketElement *) std::realloc(deletedBucketElements, (deletedCount - 1) * sizeof(SparseBucketElement));
                    deletedBucket->elements = deletedBucketElements;
                }
                deletedBucket->bitmapClear(deletedBucketOffset);
                if(endBucketIdx > startBucketIdx)
                    unlockBuckets(startBucketIdx + 1, endBucketIdx);

                packElementCount = googlerank((const unsigned char*) &(packBucket->elementBitmap), HOLY_GRAIL_SIZE);
                packBucketElements = packBucket->elements;
                --j;
            }
        }
        packBucket->unlockBucket();
    }
    return numElementsProcessed;
}

template<typename K, typename V, class HashFunc>
void LFSparseHashTable<K, V, HashFunc>::processHash(std::function<void(SparseBucketElement&)> itemProcessor)
{
    for(unsigned long long bucketPos = 0; bucketPos < maxElements / HOLY_GRAIL_SIZE + (maxElements % HOLY_GRAIL_SIZE ? 1ULL : 0ULL); ++bucketPos)
    {
        SparseBucket * bucket = &(buckets[bucketPos]);
        bucket->lockBucket();
        unsigned long long count = googlerank((const unsigned char*) &(bucket->elementBitmap), HOLY_GRAIL_SIZE);
        SparseBucketElement * bucketElements = bucket->elements;
        for(unsigned long long j = 0; j < count; ++j)
        {
            itemProcessor(bucketElements[j]);
        }
        bucket->unlockBucket();
    }
}
#endif /* LFSPARSEHASHTABLE_H_ */
