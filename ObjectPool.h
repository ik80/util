/* Copyright 2018-2020 Kalujny Ilya

1. only CAS lowest level 
2. store lock with free bits, use 128 
3. think about correctness, restarts cause live locks

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.*/

#ifndef BS9000_WF_OBJECT_POOL_H
#define BS9000_WF_OBJECT_POOL_H

#include <stdlib.h>
#include <stdio.h>
#include <stdexcept>
#include <cstring>
#include <atomic>
#include <thread>

// Aquire continuous memory at the start. Have a tree of bitmaps, every chunk with size of cacheline.
// Have a bitmap tree: On the lowest level 1 means element is free, on upper level 1 means chunk has free elements, etc , etc. Overhead is ~2 bit per element
// Allocations/Deallocations are Log512(N) complexity.

/* Classes stored in the pool can overload new and delete (new[] and delete[] should still work like std::malloc/std::free!) in a similar way:

 struct A
 {
 virtual ~A();
 virtual void foo();
 ...
 static void* operator new (std::size_t count)
 {
 A* pA = WFObjectPool<A>::Aquire();
 return (void*)pA;
 }
 static void operator delete ( void* ptr )
 {
 if (ptr)
 WFObjectPool<A>::Release((A*)ptr);
 }
 }

 struct B: public A
 {
 virtual ~B();
 virtual void foo();
 ...
 static void* operator new (std::size_t count)
 {
 B* pB = WFObjectPool<B>::Aquire();
 return (void*)pB;
 }
 static void operator delete ( void* ptr )
 {
 if (ptr)
 WFObjectPool<B>::Release((B*)ptr);
 }
 }

 // this is fine:
 A* pA = new B();
 pA->foo();
 delete pA;

 */



template<typename T>
class WFObjectPool
{
public:

    static const size_t BMP_TREE_HEIGHT = 8;
    static const size_t POOL_CHUNK_SIZE = 512;
    static const size_t QWORD_BITS = 64;
    static const size_t POOL_CHUNK_SIZE_QWORDS = 8;

    static inline void bitmapLockBit(std::atomic_ulong * pBitmap, size_t pos)
    {
    	unsigned long newBitmap, expectedBitmap = 0;
        expectedBitmap = pBitmap->load(std::memory_order_acquire) & ~(1ULL << pos);
        newBitmap = expectedBitmap | (1ULL << pos);
        while(!pBitmap->compare_exchange_weak(expectedBitmap, newBitmap, std::memory_order_release, std::memory_order_acquire))
        {
            expectedBitmap &= ~(1ULL << pos);
            newBitmap = expectedBitmap | (1ULL << pos);
        }
    }
    static inline void bitmapUnlockBit(std::atomic_ulong * pBitmap, size_t pos)
    {
        unsigned long newBitmap, expectedBitmap = 0;
        expectedBitmap = pBitmap->load(std::memory_order_acquire) | (1ULL << pos);
        newBitmap = expectedBitmap & ~(1ULL << pos);
        while(!pBitmap->compare_exchange_weak(expectedBitmap, newBitmap, std::memory_order_release, std::memory_order_acquire))
        {
            expectedBitmap |= (1ULL << pos);
            newBitmap = expectedBitmap & ~(1ULL << pos);
        }
    }
    static inline void bitmapSet(size_t & bitmap, size_t pos)
    {
        bitmap |= (1ULL << pos);
    }
    static inline void bitmapClear(size_t & bitmap, size_t pos)
    {
        bitmap &= ~(1ULL << pos);
    }
    static inline bool bitmapTest(size_t bitmap, size_t pos)
    {
        return bitmap & (1ULL << pos);
    }

    // Pool size MUST be a multiple of 512
    static void setUp(size_t in_poolSize)
    {
        if (in_poolSize % POOL_CHUNK_SIZE)
            abort();
        if (storage != nullptr)
            abort();
        poolSize = in_poolSize;
        storage = static_cast<T*>(std::malloc(sizeof(T)*poolSize));
        memset(storage, 0xFF, poolSize * sizeof(T));
        memset(treeMap, 0x0, BMP_TREE_HEIGHT * sizeof(uint64_t *));
        memset(treeLocks, 0x0, BMP_TREE_HEIGHT * sizeof(std::atomic_ulong *));
        size_t counter = poolSize;
        size_t locksCounter = counter;
        size_t prevCounter;
        int levels = 0;
        do
        {
        	// init element bitmaps
            prevCounter = counter;
            counter /= POOL_CHUNK_SIZE;
            treeMap[levels] = new uint64_t[(counter ? counter : 1) * POOL_CHUNK_SIZE_QWORDS ]; // bit per element
            if (prevCounter >= POOL_CHUNK_SIZE)
                memset(treeMap[levels], 0xFF, counter * POOL_CHUNK_SIZE_QWORDS * sizeof(uint64_t));
            else
            {
                topLevelElements = prevCounter;
                memset(treeMap[levels], 0, POOL_CHUNK_SIZE_QWORDS * sizeof(uint64_t));
                //topmost level is partially filled, MUST set zeros at relevant bits
                size_t pos = 0;
                while (prevCounter >= QWORD_BITS)
                {
                    treeMap[levels][pos++] = (uint64_t) (-1);
                    prevCounter -= QWORD_BITS;
                }
                treeMap[levels][pos++] = (1ULL << prevCounter) - 1ULL;
            }

        	// init locks bitmaps
            if (counter)
            {
                locksCounter = counter/POOL_CHUNK_SIZE;
                treeLocks[levels + 1] = new std::atomic_ulong[(locksCounter ? locksCounter : 1) * POOL_CHUNK_SIZE_QWORDS ]; // bit lock per 512 elements
    			memset(treeLocks[levels + 1], 0x0, (locksCounter ? locksCounter : 1) * POOL_CHUNK_SIZE_QWORDS * sizeof(std::atomic_ulong));
            }

            ++levels;
        }
        while (counter);

        treeMapLevels = levels;

        // rearrange bitmap tree so root is at the top, easier to think about it this way
        for (size_t i = 0; i < treeMapLevels / 2; ++i)
        {
            uint64_t * tmp = treeMap[i];
            treeMap[i] = treeMap[treeMapLevels - i - 1];
            treeMap[treeMapLevels - i - 1] = tmp;
            std::atomic_ulong * tmp2 = treeLocks[i];
            treeLocks[i] = treeLocks[treeMapLevels - i - 1];
            treeLocks[treeMapLevels - i - 1] = tmp2;
        }
        topLevelLocked.store(false); // let the game begin
    }

    static void tearDown()
    {
        if (storage == nullptr)
            abort();
        std::free(storage);
        storage = nullptr;
        for (size_t i = 0; i < treeMapLevels; ++i)
            delete[] treeMap[i]; //TODO: leak checks!
        for (size_t i = 0; i < treeMapLevels; ++i)
            delete[] treeLocks[i];
    }

    // Ugh
    static T * acquire()
    {
    	// thread local static dual stacks for rolling back the locks
     // TODO: uncomment thread_local static when done with debugging (fucking gdb)
    	/*thread_local static*/ size_t upStack[BMP_TREE_HEIGHT];
    	/*thread_local static*/ size_t upStackCount = 0;
        static size_t lastFreeIdx = poolSize - 1;
    	/*thread_local static size_t lastFreeIdx = {(((size_t)upStack)*65521 + 4294967291)%poolSize}; // hack to use pointer to thread local as thread identifier */
        T * pRes = 0;

        // acquire restarts when facing a locked element. Restart happens at the point in the tree to the right of the last subtree.
        while (true)
        {
        	upStackCount = 0;
            bool needToUnlockTopLevel = false;
            bool restartRequired = false;
            size_t restartLevel = 0;
            size_t restartIdx = 0;

            //walk forward/up from last free idx
            size_t idx = (lastFreeIdx + 1) % poolSize;
            size_t level = treeMapLevels - 1;
            bool wentUp = false;
            unsigned long bitPos = 0;
            restartIdx = (idx + 1) % poolSize; // if everything else fails to set restartIdx correctly, retry at the next one
            do
            {
                //check the remainder of nearest ull
                const size_t mask = ((treeMap[level][idx / QWORD_BITS]) & (((size_t) (-1)) << idx % QWORD_BITS));
                if (mask)
                {
                    bitPos = __builtin_ffsll(mask);
                    idx = (idx - idx % QWORD_BITS + bitPos - 1);
                    break;
                }
                else // check remaining ulls in the chunk
                {
                    size_t stepsToGoThisLevel = 7 - (idx % POOL_CHUNK_SIZE) / QWORD_BITS;
                    bitPos = 0;
                    while (!bitPos && stepsToGoThisLevel)
                    {
                   		if (treeMap[level][((idx - idx % POOL_CHUNK_SIZE) / QWORD_BITS + (8 - stepsToGoThisLevel))] == 0)
                            --stepsToGoThisLevel; // TODO: bug here, needs assert that the idx doesnt spill over nearest ULL (will UB at array's ends on all levels)
                   		else
                   			bitPos = __builtin_ffsll(treeMap[level][((idx - idx % POOL_CHUNK_SIZE) / QWORD_BITS + (8 - stepsToGoThisLevel))]);
                    }
                    if (bitPos)
                    {
                        idx = (idx - idx % POOL_CHUNK_SIZE + (8 - stepsToGoThisLevel) * QWORD_BITS + bitPos - 1);
                        break;
                    }
                }
                // no luck move up
                idx = idx / POOL_CHUNK_SIZE;
                --level;
                wentUp = true;
            }
            while (level > 0);


            if (!restartRequired && !level)
            {
            	// level zero requires special treatment
                bitPos = 0;
                // need to zero out bits pre-index on first iteration
                const size_t mask = ((treeMap[0][idx / QWORD_BITS]) & (((size_t) (-1)) << idx % QWORD_BITS));
                if (mask)
                {
                    bitPos = __builtin_ffsll(mask);
                    idx = (idx - idx % QWORD_BITS + bitPos - 1);
                }
                else // and then wrap the whole top level starting from next position. UGLY CODE!
                {
                    size_t stepsThisLevel = topLevelElements / QWORD_BITS + 1;
                    idx = ((idx / QWORD_BITS + 1) * QWORD_BITS) % topLevelElements;
                    while ((stepsThisLevel && treeMap[0][(idx/QWORD_BITS + topLevelElements / QWORD_BITS + 1 - stepsThisLevel) % (topLevelElements / QWORD_BITS + 1)] == 0) && (stepsThisLevel <= topLevelElements / QWORD_BITS + 1))
                        --stepsThisLevel;
                    bitPos = __builtin_ffsll(treeMap[0][(idx/QWORD_BITS + topLevelElements / QWORD_BITS + 1 - stepsThisLevel) % (topLevelElements / QWORD_BITS + 1)]);
                    idx = ((idx/QWORD_BITS + topLevelElements / QWORD_BITS + 1 - stepsThisLevel) % (topLevelElements / QWORD_BITS + 1) * QWORD_BITS + bitPos - 1);
                }
                if (bitPos)
                {
                    ++level;
                    if (level < treeMapLevels)
                        idx *= POOL_CHUNK_SIZE;
                }
                else // failure point here, if we locked everything and wrapped the top level, pool is depleted. unlock everything and bail
                    return nullptr;
            }

            //and now forward/down to the next free idx
            if (!restartRequired && (wentUp || level < treeMapLevels - 1))
            {
                do
                {
                    bitPos = 0;
                    size_t stepsThisLevel = 0;
                    while (treeMap[level][((idx - idx % POOL_CHUNK_SIZE) / QWORD_BITS + stepsThisLevel)] == 0)
                        ++stepsThisLevel; // TODO: bug here, needs assert that the idx doesnt spill over nearest ULL (will UB at array's ends on all levels)
                    bitPos = __builtin_ffsll(treeMap[level][((idx - idx % POOL_CHUNK_SIZE) / QWORD_BITS + stepsThisLevel)]);
                    idx = ((idx - idx % POOL_CHUNK_SIZE) + stepsThisLevel * QWORD_BITS + bitPos - 1);
                    ++level;
                    if (level < treeMapLevels)
                        idx *= POOL_CHUNK_SIZE;
                    if (!bitPos)
                    {
                        restartRequired = true;
                        restartLevel = level;
                        restartIdx = idx + 1;
                        break;
                    }

                }
                while (level < treeMapLevels);
            }

            if (!restartRequired && bitPos)
            {
                // OK, supposedly we got something, now we need to lock and check again
                level = treeMapLevels - 1;
                bitmapLockBit(static_cast<std::atomic_ulong*>(&(treeLocks[level - 1][(idx/POOL_CHUNK_SIZE)/QWORD_BITS])), (idx/POOL_CHUNK_SIZE)%QWORD_BITS);
                upStack[level] = idx;
                ++upStackCount;

                if (!bitmapTest(treeMap[level][idx / QWORD_BITS], idx % QWORD_BITS))
                {
                    // restart otherwise
                    bitmapUnlockBit(static_cast<std::atomic_ulong*>(&(treeLocks[level - 1][(idx/POOL_CHUNK_SIZE)/QWORD_BITS])), (idx/POOL_CHUNK_SIZE)%QWORD_BITS);
                    restartRequired = true;
                    restartIdx = (idx + 1) % poolSize; // if everything else fails to set restartIdx correctly, retry at the next one
                }
                else // got it!
                {
                    //save previous free position
                    lastFreeIdx = idx;
                    pRes = &(storage[idx]);
                    //set zero bits upwards as needed
                    level = treeMapLevels - 1;
                    while (level >= 0 && level < treeMapLevels)
                    {

                        if (level < treeMapLevels - 1)
                        {
                            if (level)
                            {
                                bitmapLockBit(static_cast<std::atomic_ulong*>(&(treeLocks[level - 1][(idx/POOL_CHUNK_SIZE)/QWORD_BITS])), (idx/POOL_CHUNK_SIZE)%QWORD_BITS);
                                upStack[level] = idx;
                                ++upStackCount;
                            }
                            else
                            {
                                // try lock the top level
                                bool topLevelLockState = false;
                                while (!topLevelLocked.compare_exchange_weak(topLevelLockState, true, std::memory_order_acq_rel)) {topLevelLockState = false;}
                                needToUnlockTopLevel = true;
                            }
                        }

                        bitmapClear(treeMap[level][idx / QWORD_BITS], idx % QWORD_BITS);
                        bool bail = false;
                        //check neighbors
                        size_t chunkStart = idx - (idx % POOL_CHUNK_SIZE);
                        for (size_t nearIdx = 0; nearIdx < 8 && !bail; ++nearIdx)
                            if (treeMap[level][chunkStart / QWORD_BITS + nearIdx])
                                bail = true;
                        if (!bail)
                        {
                            idx /= POOL_CHUNK_SIZE;
                            --level;
                        }
                        else
                            break;
                    }
                }

                // unlock stuff in reverse order
                {
                    // back up
                    size_t level;
                    // unlock top if needed
                    if (needToUnlockTopLevel)
                    {
                        bool topLevelLockState = true;
                        topLevelLocked.compare_exchange_strong(topLevelLockState, false, std::memory_order_acq_rel);
                    }

                    // back down
                    if (bitPos)
                    {
                        if (!restartRequired)
                            level = treeMapLevels - upStackCount;
                        else
                            level = restartLevel;

                        while (level <= treeMapLevels - 1)
                        {
                            idx = upStack[level];
                            bitmapUnlockBit(static_cast<std::atomic_ulong*>(&(treeLocks[level++ - 1][(idx/POOL_CHUNK_SIZE)/QWORD_BITS])), (idx/POOL_CHUNK_SIZE)%QWORD_BITS);
                        }
                    }
                }
            }
            else
			{
				lastFreeIdx = restartIdx << (treeMapLevels - restartLevel); // restart after failed position. TODO: break the cycle
				continue;
			}
            break;
        }
        return pRes;
    }

    static void release(T * objPtr)
    {
        /*thread_local*/ static size_t upStack[BMP_TREE_HEIGHT];
        /*thread_local*/ static size_t upStackCount;
    	bool topLevelLockState = false;
    	bool needToUnlockTopLevel = false;
        // objPtr - storage == idx. set one bits upwards as needed
        size_t idx = objPtr - storage;
        size_t level = treeMapLevels - 1;
        upStackCount = 0;
        while (level >= 0 && level < treeMapLevels)
        {
        	// lock chunk, add to upStack at current level
        	if (level)
        		bitmapLockBit(static_cast<std::atomic_ulong*>(&(treeLocks[level - 1][(idx/POOL_CHUNK_SIZE)/QWORD_BITS])), (idx/POOL_CHUNK_SIZE)%QWORD_BITS);
        	else
        	{
            	while (!topLevelLocked.compare_exchange_weak(topLevelLockState, true, std::memory_order_acq_rel)) {topLevelLockState = true;}
            	needToUnlockTopLevel = true;
        	}

        	upStack[level] = idx;
        	++upStackCount;

        	if (!bitmapTest(treeMap[level][idx / QWORD_BITS], idx % QWORD_BITS))
            {
                bitmapSet(treeMap[level][idx / QWORD_BITS], idx % QWORD_BITS);
                --level;
                idx /= POOL_CHUNK_SIZE;
            }
            else
                break;
        }
		// unlock stuff in reverse order
        {
            size_t level = treeMapLevels - upStackCount;
			while (level <= treeMapLevels - 1)
			{
				idx = upStack[level];
				if (level)
				    bitmapUnlockBit(static_cast<std::atomic_ulong*>(&(treeLocks[level-1][(idx/POOL_CHUNK_SIZE)/QWORD_BITS])), (idx/POOL_CHUNK_SIZE)%QWORD_BITS);
				else if (needToUnlockTopLevel)
	            {
	                bool topLevelLockState = true;
	                topLevelLocked.compare_exchange_strong(topLevelLockState, false, std::memory_order_acq_rel);
	            }
				++level;
			}
        }

    }

    static bool ready()
    {
        return storage != nullptr;
    }

private:
    static uint64_t * treeMap[BMP_TREE_HEIGHT]; // neat cacheline
    static std::atomic_ulong * treeLocks[BMP_TREE_HEIGHT];
    static T * storage;
    static size_t treeMapLevels;
    static size_t poolSize;
    static size_t topLevelElements;
    static std::atomic_bool topLevelLocked;
};

template<typename T>
uint64_t * WFObjectPool<T>::treeMap[BMP_TREE_HEIGHT];
template<typename T>
std::atomic_ulong * WFObjectPool<T>::treeLocks[BMP_TREE_HEIGHT];
template<typename T>
T * WFObjectPool<T>::storage = nullptr;
template<typename T>
size_t WFObjectPool<T>::treeMapLevels;
template<typename T>
size_t WFObjectPool<T>::poolSize;
template<typename T>
size_t WFObjectPool<T>::topLevelElements;
template<typename T>
std::atomic_bool WFObjectPool<T>::topLevelLocked = {true};

/*
    //Sample std::allocator below, can be used like this

    WFObjectPool<std::_Rb_tree_node <std::pair<const size_t, size_t> > >::setUp(100*1024);
    {
        std::map<size_t, size_t, std::less<size_t>, WFObjectPoolAllocator<std::pair<const size_t, size_t> > > daMap;
        for (size_t i = 0; i < 102400; ++i)
            daMap[i] = i;
    }
    WFObjectPool<std::_Rb_tree_node <std::pair<const size_t, size_t> > >::tearDown();

    WFObjectPool<std::__detail::_Hash_node<std::pair<unsigned long const, unsigned long>, false> >::setUp(100*1024);
    {
        std::unordered_map<size_t, size_t, std::hash<size_t>, std::equal_to<size_t>, WFObjectPoolAllocator<std::pair<const size_t, size_t> > > daMap;
        for (size_t i = 0; i < 102400; ++i)
            daMap[i] = i;
    }
    WFObjectPool<std::__detail::_Hash_node<std::pair<unsigned long const, unsigned long>, false> >::tearDown();

*/

template<typename T>
class WFObjectPoolAllocator : public std::allocator<T>
{
public:
    typedef size_t size_type;
    typedef T* pointer;
    typedef const T* const_pointer;

    template<typename _Tp1>
    struct rebind
    {
        typedef WFObjectPoolAllocator<_Tp1> other;
    };

    pointer allocate(size_type n, const void *hint = 0)
    {
        if (n == 1)
            return (pointer) WFObjectPool<T>::acquire();
        else
            return std::allocator<T>::allocate(n, hint);
    }

    void deallocate(pointer p, size_type n)
    {
        if (n == 1)
            WFObjectPool<T>::release((T*) p);
        else
            std::allocator<T>::deallocate(p, n);
    }

    WFObjectPoolAllocator() throw () :
                    std::allocator<T>()
    {
    }
    WFObjectPoolAllocator(const WFObjectPoolAllocator &a) throw () :
                    std::allocator<T>(a)
    {
    }
    template<class U>
    WFObjectPoolAllocator(const WFObjectPoolAllocator<U> &a) throw () :
                    std::allocator<T>(a)
    {
    }
    ~WFObjectPoolAllocator() throw ()
    {
    }
};

#endif //BS9000_WF_OBJECT_POOL_H
