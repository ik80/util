/*
 * LFMMPMCQueue.h
 *
 *  Created on: Nov 22, 2017
 *      Author: kalujny
 */

#ifndef RELEASE_LFMMPMCQUEUE_H_
#define RELEASE_LFMMPMCQUEUE_H_

#include <atomic>
#include <mutex>
#include <map>

#include "LFSPSCQueue.h"

// This is only good for benchmarks. Anyway, both pop and push can fail spuriously
// so both producers and consumers should call push/pop in a tight loop with a tiny yield
template<typename T, size_t NUM_PRODUCERS, size_t NUM_CONSUMERS>
class LFMMPMCQueue
{
private:
    static const size_t CACHE_LINE_SIZE = 64;

    template <size_t NUM_QUEUE_POINTERS>
    struct PerThreadData
    {
        size_t              curPipe;
        LFSPSCQueue<T> *    pipes[NUM_QUEUE_POINTERS];
        char                pad1[CACHE_LINE_SIZE - (sizeof(size_t) + sizeof(pipes)%CACHE_LINE_SIZE)%CACHE_LINE_SIZE];

        inline void nextPipe()
        {
            curPipe = (curPipe + 1) % NUM_QUEUE_POINTERS;
        }
    }__attribute((aligned(CACHE_LINE_SIZE)));

    PerThreadData<NUM_CONSUMERS> * registerProducer()
    {
        size_t curProducersCount = producersCount.load();
        if (curProducersCount < NUM_CONSUMERS && producersCount.compare_exchange_strong(curProducersCount, curProducersCount + 1))
        {
            return &producers[curProducersCount];
        }
        return nullptr;
    }

    PerThreadData<NUM_PRODUCERS> * registerConsumer()
    {
        size_t curConsumersCount = consumersCount.load();
        if (curConsumersCount < NUM_PRODUCERS && consumersCount.compare_exchange_strong(curConsumersCount, curConsumersCount + 1))
        {
            return &consumers[curConsumersCount];
        }
        return nullptr;
    }

    std::atomic_size_t consumersCount;
    std::atomic_size_t producersCount;

    PerThreadData<NUM_PRODUCERS> consumers[NUM_CONSUMERS];
    PerThreadData<NUM_CONSUMERS> producers[NUM_PRODUCERS];

public:

    LFMMPMCQueue(size_t singleQueueSize) : consumersCount(0), producersCount(0)
    {
        for (size_t i = 0; i < NUM_PRODUCERS; ++i)
        {
            producers[i].curPipe = i % NUM_CONSUMERS;
            for (size_t j = 0; j < NUM_CONSUMERS; ++j)
            {
                producers[i].pipes[j] = new LFSPSCQueue<T>(singleQueueSize);
            }
        }
        for (size_t i = 0; i < NUM_CONSUMERS; ++i)
        {
            consumers[i].curPipe = i % NUM_PRODUCERS;
            for (size_t j = 0; j < NUM_PRODUCERS; ++j)
            {
                consumers[i].pipes[j] = producers[j].pipes[i];
            }
        }
    }

    ~LFMMPMCQueue()
    {
        for (size_t i = 0; i < NUM_PRODUCERS; ++i)
        {
            for (size_t j = 0; j < NUM_CONSUMERS; ++j)
            {
                delete producers[i].pipes[j];
            }
        }
    }

    bool push(const T & value)
    {
        static __thread PerThreadData<NUM_CONSUMERS> * pData = nullptr;
        if (!pData)
            pData = registerProducer();
        size_t retries = NUM_CONSUMERS;
        while(retries--)
        {
            if (pData->pipes[pData->curPipe % NUM_CONSUMERS]->push(value))
                return true;
            else
                ++(pData->curPipe);
        }
        return false;
    }

    bool pop(T & value)
    {
        static __thread PerThreadData<NUM_PRODUCERS> * pData = nullptr;
        if (!pData)
            pData = registerConsumer();
        size_t retries = NUM_CONSUMERS;
        while(retries--)
        {
            if (pData->pipes[pData->curPipe % NUM_CONSUMERS]->pop(value))
                return true;
            else
                ++(pData->curPipe);
        }
        return false;
    }

}__attribute((aligned(CACHE_LINE_SIZE)));

#endif /* RELEASE_LFMMPMCQUEUE_H_ */
