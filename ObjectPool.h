/* Copyright 2018 Kalujny Ilya

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.*/

#ifndef BS9000_OBJECT_POOL_H
#define BS9000_OBJECT_POOL_H

#include <stdlib.h>
#include <stdio.h>
#include <stdexcept>
#include <cstring>

// Bullshittenator9000 single threaded object pool. Aquire continuous memory at the start. Have a tree of bitmaps, every chunk with size of cacheline.
// On the lowest level 1 means element is free, on upper level 1 means chunk has free elements, etc , etc. Overhead is ~2 bit per element

/* Classes stored in the pool can overload new and delete (new[] and delete[] should still work like std::malloc/std::free!) in a similar way:

 struct A
 {
 virtual ~A();
 virtual void foo();
 ...
 static void* operator new (std::size_t count)
 {
 A* pA = ObjectPool<A>::Aquire();
 return (void*)pA;
 }
 static void operator delete ( void* ptr )
 {
 if (ptr)
 ObjectPool<A>::Release((A*)ptr);
 }
 }

 struct B: public A
 {
 virtual ~B();
 virtual void foo();
 ...
 static void* operator new (std::size_t count)
 {
 B* pB = ObjectPool<B>::Aquire();
 return (void*)pB;
 }
 static void operator delete ( void* ptr )
 {
 if (ptr)
 ObjectPool<B>::Release((B*)ptr);
 }
 }

 // this is fine:
 A* pA = new B();
 pA->foo();
 delete pA;

 */



template<typename T>
class ObjectPool
{
public:

    static const size_t BMP_TREE_HEIGHT = 9;
    static const size_t POOL_CHUNK_SIZE = 512;
    static const size_t QWORD_BITS = 64;
    static const size_t POOL_CHUNK_SIZE_QWORDS = 8;

    static inline void bitmapLockBit(size_t & bitmap, size_t pos)
    {
        __builtin_prefetch(&bitmap, 1, 0);
        size_t newBitmap, oldBitmap, expectedBitmap = 0;
        expectedBitmap = bitmap & ~(1ULL << pos);
        while(true)
        {
            newBitmap = expectedBitmap | (1ULL << pos);
            oldBitmap = __sync_val_compare_and_swap(&bitmap, expectedBitmap, newBitmap);
            if(oldBitmap != expectedBitmap)
            {
                expectedBitmap = oldBitmap &= ~(1ULL << pos);
            }
            else
                break;
        }
    }
    static inline void bitmapUnlockBit(size_t & bitmap, size_t pos)
    {
        __builtin_prefetch(&bitmap, 1, 0);
        size_t newBitmap, oldBitmap, expectedBitmap = 0;
        expectedBitmap = bitmap | (1ULL << pos);
        while(true)
        {
            newBitmap = expectedBitmap & ~(1ULL << pos);
            oldBitmap = __sync_val_compare_and_swap(&bitmap, expectedBitmap, newBitmap);
            if(oldBitmap != expectedBitmap)
            {
                expectedBitmap = oldBitmap |= (1ULL << pos);
            }
            else
                break;
        }
    }
    static inline void bitmapSet(size_t & bitmap, size_t pos)
    {
        bitmap |= (1ULL << (pos & 63ULL));
    }
    static inline void bitmapClear(size_t & bitmap, size_t pos)
    {
        bitmap &= ~(1ULL << (pos & 63ULL));
    }
    static inline bool bitmapTest(size_t bitmap, size_t pos)
    {
        return bitmap & (1ULL << (pos & 63ULL));
    }

    // Pool size MUST be a multiple of 512
    static void setUp(size_t in_poolSize)
    {
        if (in_poolSize % POOL_CHUNK_SIZE)
            abort();
        if (storage != nullptr)
            abort();
        poolSize = in_poolSize;
        freeSize = poolSize;
        storage = static_cast<T*>(std::malloc(sizeof(T)*poolSize));
        memset(storage, 0x0, poolSize * sizeof(T));
        size_t counter = poolSize;
        size_t prevCounter = counter;
        int levels = 0;

        do
        {
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
            ++levels;
        }
        while (counter);

        treeMapLevels = levels;

        for (size_t i = 0; i < treeMapLevels / 2; ++i)
        {
            uint64_t * tmp = treeMap[i];
            treeMap[i] = treeMap[treeMapLevels - i - 1];
            treeMap[treeMapLevels - i - 1] = tmp;
        }
        lastFreeIdx = poolSize - 1; // let the game begin
    }

    static void tearDown()
    {
        if (storage == nullptr)
            abort();
        std::free(storage);
        storage = nullptr;
        for (size_t i = 0; i < treeMapLevels; ++i)
            delete[] treeMap[i]; //TODO: leak checks!
    }

    // Ugh
    static T * acquire()
    {
        T * pRes = 0;
        if (freeSize)
        {
            //walk forward/up from last free idx
            size_t idx = (lastFreeIdx + 1) % poolSize;
            size_t level = treeMapLevels - 1;
            bool wentUp = false;
            unsigned long bitPos = 0;
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
                   		{
                            --stepsToGoThisLevel;
                   		}
                   		else
                   		{
                   			bitPos = __builtin_ffsll(treeMap[level][((idx - idx % POOL_CHUNK_SIZE) / QWORD_BITS + (8 - stepsToGoThisLevel))]);
                   		}
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


            if (!level)
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
                    while (stepsThisLevel && treeMap[0][(idx/QWORD_BITS + topLevelElements / QWORD_BITS + 1 - stepsThisLevel) % (topLevelElements / QWORD_BITS + 1)] == 0)
                        --stepsThisLevel;
                    bitPos = __builtin_ffsll(treeMap[0][(idx/QWORD_BITS + topLevelElements / QWORD_BITS + 1 - stepsThisLevel) % (topLevelElements / QWORD_BITS + 1)]);
                    idx = ((idx/QWORD_BITS + topLevelElements / QWORD_BITS + 1 - stepsThisLevel) % (topLevelElements / QWORD_BITS + 1) * QWORD_BITS + bitPos - 1);
                }
                ++level;
                if (level < treeMapLevels)
                    idx *= POOL_CHUNK_SIZE;
            }

            //and now forward/down to the next free idx
            if (wentUp || level < treeMapLevels - 1)
            {
                do
                {
                    bitPos = 0;
                    size_t stepsThisLevel = 0;
                    while (treeMap[level][((idx - idx % POOL_CHUNK_SIZE) / QWORD_BITS + stepsThisLevel)] == 0)
                        ++stepsThisLevel;
                    bitPos = __builtin_ffsll(treeMap[level][((idx - idx % POOL_CHUNK_SIZE) / QWORD_BITS + stepsThisLevel)]);
                    idx = ((idx - idx % POOL_CHUNK_SIZE) + stepsThisLevel * QWORD_BITS + bitPos - 1);
                    ++level;
                    if (level < treeMapLevels)
                        idx *= POOL_CHUNK_SIZE;
                }
                while (level < treeMapLevels);
            }

            //save previous free position
            lastFreeIdx = idx;
            pRes = &(storage[idx]);
            --freeSize;
            //set zero bits upwards as needed
            level = treeMapLevels - 1;
            while (level >= 0 && level < treeMapLevels)
            {
                bitmapClear(treeMap[level][idx / QWORD_BITS], idx % QWORD_BITS);
                bool done = false;
                //check neighbors
                size_t chunkStart = idx - (idx % POOL_CHUNK_SIZE);
                for (size_t nearIdx = 0; nearIdx < 8 && !done; ++nearIdx)
                    if (treeMap[level][chunkStart / QWORD_BITS + nearIdx])
                        done = true;
                if (!done)
                {
                    idx /= POOL_CHUNK_SIZE;
                    --level;
                }
                else
                    break;
            }
        }
        return pRes;
    }

    static void release(T * objPtr)
    {
        ++freeSize;
        // objPtr - storage == idx. set one bits upwards as needed
        size_t idx = objPtr - storage;
        size_t level = treeMapLevels - 1;
        while (level >= 0 && level < treeMapLevels)
        {
            if (!bitmapTest(treeMap[level][idx / QWORD_BITS], idx % QWORD_BITS))
            {
                bitmapSet(treeMap[level][idx / QWORD_BITS], idx % QWORD_BITS);
                --level;
                idx /= POOL_CHUNK_SIZE;
            }
            else
                break;
        }
    }

    static bool ready()
    {
        return storage != nullptr;
    }

private:
    static uint64_t * treeMap[BMP_TREE_HEIGHT];
    static T * storage;
    static size_t treeMapLevels;
    static size_t poolSize;
    static size_t freeSize;
    static size_t lastFreeIdx;
    static size_t topLevelElements;
};

template<typename T>
uint64_t * ObjectPool<T>::treeMap[BMP_TREE_HEIGHT];
template<typename T>
T * ObjectPool<T>::storage = nullptr;
template<typename T>
size_t ObjectPool<T>::treeMapLevels;
template<typename T>
size_t ObjectPool<T>::poolSize;
template<typename T>
size_t ObjectPool<T>::freeSize;
template<typename T>
size_t ObjectPool<T>::lastFreeIdx;
template<typename T>
size_t ObjectPool<T>::topLevelElements;

/*
    //Sample std::allocator below, can be used like this

    ObjectPool<std::_Rb_tree_node <std::pair<const size_t, size_t> > >::setUp(100*1024);
    {
        std::map<size_t, size_t, std::less<size_t>, ObjectPoolAllocator<std::pair<const size_t, size_t> > > daMap;
        for (size_t i = 0; i < 102400; ++i)
            daMap[i] = i;
    }
    ObjectPool<std::_Rb_tree_node <std::pair<const size_t, size_t> > >::tearDown();

    ObjectPool<std::__detail::_Hash_node<std::pair<unsigned long const, unsigned long>, false> >::setUp(100*1024);
    {
        std::unordered_map<size_t, size_t, std::hash<size_t>, std::equal_to<size_t>, ObjectPoolAllocator<std::pair<const size_t, size_t> > > daMap;
        for (size_t i = 0; i < 102400; ++i)
            daMap[i] = i;
    }
    ObjectPool<std::__detail::_Hash_node<std::pair<unsigned long const, unsigned long>, false> >::tearDown();

*/

template<typename T>
class ObjectPoolAllocator : public std::allocator<T>
{
public:
    typedef size_t size_type;
    typedef T* pointer;
    typedef const T* const_pointer;

    template<typename _Tp1>
    struct rebind
    {
        typedef ObjectPoolAllocator<_Tp1> other;
    };

    pointer allocate(size_type n, const void *hint = 0)
    {
        if (n == 1)
            return (pointer) ObjectPool<T>::acquire();
        else
            return std::allocator<T>::allocate(n, hint);
    }

    void deallocate(pointer p, size_type n)
    {
        if (n == 1)
            return ObjectPool<T>::release((T*) p);
        else
            return std::allocator<T>::deallocate(p, n);
    }

    ObjectPoolAllocator() throw () :
                    std::allocator<T>()
    {
    }
    ObjectPoolAllocator(const ObjectPoolAllocator &a) throw () :
                    std::allocator<T>(a)
    {
    }
    template<class U>
    ObjectPoolAllocator(const ObjectPoolAllocator<U> &a) throw () :
                    std::allocator<T>(a)
    {
    }
    ~ObjectPoolAllocator() throw ()
    {
    }
};

#endif //BS9000_OBJECT_POOL_H
