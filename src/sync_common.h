#ifndef PARASYNC_COMMON_H
#define PARASYNC_COMMON_H

#include <mutex>
#include <condition_variable>
#include <string>
#include <queue>
#include <atomic>
#include <oneapi/tbb/concurrent_queue.h>

static uint8_t adj_core_set[32] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
                              16, 17, 18, 19, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51};
static uint8_t cross_core_set[32] = {0, 40, 1, 41, 2, 42, 3, 43, 4, 44, 5, 45, 6, 46, 7, 47,
                              8, 48, 9, 49, 10, 50, 11, 51, 12, 52, 13, 53, 14, 54, 15, 55};
static uint8_t cross_cpu_set[32] = {0, 20, 1, 21, 2, 22, 3, 23, 4, 24, 5, 25, 6, 26, 7, 27,
                              8, 28, 9, 29, 10, 30, 11, 31, 12, 32, 13, 33, 14, 34, 15, 35};

void calc_blake3(uint8_t *b3_hash, uint8_t *buf, uint32_t len);

template <typename T>
class DataQueue {
public:
    DataQueue() : done(false) {}

    ~DataQueue() {}

    void push(const T& data) {
        std::unique_lock<std::mutex> lock(mtx);
        queue.push(data);
        cv.notify_one();
    }

    T pop() {
        std::unique_lock<std::mutex> lock(mtx);
        cv.wait(lock, [this] { return !queue.empty() || done; });
        if (queue.empty()) {
            return T(); // Return a default-constructed T instead of throwing an exception
        }
        T data = queue.front();
        queue.pop();
        return data;
    }

    void setDone() {
        std::unique_lock<std::mutex> lock(mtx);
        done = true;
        cv.notify_all();
    }

    bool isDone() const {
        std::unique_lock<std::mutex> lock(mtx);
        return done && queue.empty();
    }
    
    size_t size() const {
        std::unique_lock<std::mutex> lock(mtx);
        return queue.size();
    }

    bool empty() const {
        std::unique_lock<std::mutex> lock(mtx);
        return queue.empty();
    }

    void init() {
        done = false;
        while (!queue.empty())
            queue.pop();
    }
private:
    std::queue<T> queue;
    mutable std::mutex mtx;
    std::condition_variable cv;
    std::atomic<bool> done;
};

#endif // PARASYNC_COMMON_H