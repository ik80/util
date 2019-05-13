#ifndef DYNA_WFSPSCQ_H
#define DYNA_WFSPSCQ_H

#include <atomic>

// ATTENTION: this queue is faster than a single circular buffer but its not stable in terms of push/pop order, ie. its not FIFO
template<typename T>
class LFSPSCQueue {
    static const size_t CACHE_LINE_SIZE = 64;
    struct RingBuffer
    {
        T * ring_;
        char padding0[CACHE_LINE_SIZE - sizeof(T *)];
        std::atomic<size_t> head;
        char padding1[CACHE_LINE_SIZE - sizeof(std::atomic<size_t>)];
        std::atomic<size_t> tail;
        char padding2[CACHE_LINE_SIZE - sizeof(std::atomic<size_t>)];
    } __attribute((aligned(CACHE_LINE_SIZE)));
public:
    LFSPSCQueue(size_t buffer_size) : buffer_mask_(buffer_size - 1)
    {
        assert((buffer_size >= 2) && ((buffer_size & (buffer_size - 1)) == 0));
        laneSwitch = 0;
        lanes[0].ring_ = new T[buffer_size];
        lanes[1].ring_ = new T[buffer_size];
        lanes[0].tail = 0;
        lanes[1].tail = 0;
        lanes[0].head = 0;
        lanes[1].head = 0;
    }
    ~LFSPSCQueue()
    {
        delete[] lanes[0].ring_;
        delete[] lanes[1].ring_;
    }

    bool push(const T & value)
    {
        bool done = false;
        size_t curLaneSwitch = laneSwitch.load(std::memory_order_acquire);
        while (!done)
        {
            size_t head = lanes[curLaneSwitch%2].head.load(std::memory_order_relaxed);
            size_t next_head = next(head);
            if (next_head != lanes[curLaneSwitch%2].tail.load(std::memory_order_acquire))
            {
                lanes[curLaneSwitch%2].ring_[head] = value;
                lanes[curLaneSwitch%2].head.store(next_head, std::memory_order_release);
                return true;
            }

            if (laneSwitch.compare_exchange_strong(curLaneSwitch, curLaneSwitch + 1, std::memory_order_release))
            {
                ++curLaneSwitch;
                done = true;

                head = lanes[curLaneSwitch%2].head.load(std::memory_order_relaxed);
                next_head = next(head);
                if (next_head != lanes[curLaneSwitch%2].tail.load(std::memory_order_acquire))
                {
                    lanes[curLaneSwitch%2].ring_[head] = value;
                    lanes[curLaneSwitch%2].head.store(next_head, std::memory_order_release);
                    return true;
                }
            }
            else
                curLaneSwitch = laneSwitch.load(std::memory_order_acquire);
        }
        return false;
    }
    bool pop(T & value)
    {
        bool done = false;
        size_t curLaneSwitch = laneSwitch.load(std::memory_order_acquire);
        while (!done)
        {
            size_t tail = lanes[(curLaneSwitch + 1)%2].tail.load(std::memory_order_relaxed);
            if (tail != lanes[(curLaneSwitch + 1)%2].head.load(std::memory_order_acquire))
            {
                value = lanes[(curLaneSwitch + 1)%2].ring_[tail];
                lanes[(curLaneSwitch + 1)%2].tail.store(next(tail), std::memory_order_release);
                return true;
            }

            if (laneSwitch.compare_exchange_strong(curLaneSwitch, curLaneSwitch + 1, std::memory_order_release))
            {
                ++curLaneSwitch;
                done = true;

                tail = lanes[(curLaneSwitch + 1)%2].tail.load(std::memory_order_relaxed);
                if (tail != lanes[(curLaneSwitch + 1)%2].head.load(std::memory_order_acquire))
                {
                    value = lanes[(curLaneSwitch + 1)%2].ring_[tail];
                    lanes[(curLaneSwitch + 1)%2].tail.store(next(tail), std::memory_order_release);
                    return true;
                }
            }
            else
                curLaneSwitch = laneSwitch.load(std::memory_order_acquire);

        }
        return false;
    }
private:
    inline size_t next(size_t current)
    {
        return (current + 1) & buffer_mask_;
    }
    RingBuffer lanes[2]; // lanes are cache line aligned
    size_t const buffer_mask_;
    char padding1[CACHE_LINE_SIZE - sizeof(size_t)];
    std::atomic_size_t laneSwitch;
    char padding2[CACHE_LINE_SIZE - sizeof(std::atomic_size_t)];
} __attribute((aligned(CACHE_LINE_SIZE)));

#endif //DYNA_WFSPSCQ_H
