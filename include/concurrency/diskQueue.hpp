#pragma once

#include <condition_variable>
#include <mutex>
#include <queue>

namespace pubsub::concurrency {

// this class has the code for a queue which is used to write records to the disk.
// epoll thread will push to his queue and worker thread pool will pop and write to disk.
template <typename T> class DiskQueue {
    std::queue<T> queue;
    std::mutex mu;
    std::condition_variable cv;
    bool isClosed = false; // to indicate that the queue is closed for cond var.
  public:
    DiskQueue() = default;
    // new concepts
    // disable the copy constructor [DiskQueue q1 = q2;]
    DiskQueue(const DiskQueue &) = delete;
    DiskQueue &operator=(const DiskQueue &) = delete;
    // enable move constructor [DiskQueue q2 = std::move(q1);]
    DiskQueue(DiskQueue &&) = default;
    DiskQueue &operator=(DiskQueue &&) = default;
    ~DiskQueue() { closeQueue(); }
    void push(const T &item) {
        /*
            std::lock_guard has a very simple interface:
            Locks the mutex in its constructor.
            Unlocks the mutex in its destructor.
            There are no lock() or unlock() member functions.
            RAII (Resource Acquisition Is Initialization).
         */
        std::lock_guard<std::mutex> lock(mu);
        if (isClosed)
            return;
        queue.push(item);
        cv.notify_one();
    }
    bool pop(T &item) {
        std::unique_lock<std::mutex> lock(mu);
        cv.wait(lock, [&]() { return !queue.empty() or isClosed; });
        if (isClosed or queue.empty())
            return false;
        item = std::move(queue.front());
        queue.pop();
        return true;
    }
    void closeQueue() {
        std::lock_guard<std::mutex> lock(mu);
        isClosed = true;
        cv.notify_all();
    }
};
} // namespace pubsub::concurrency