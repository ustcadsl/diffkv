//
// Created by wujy on 9/6/18.
//
#pragma once

#include <condition_variable>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

class ThreadPool {
 public:
  // create a thread pool witch contains n worker threads
  ThreadPool(int n) {
    for (int i = 0; i < n; i++) {
      workers.emplace_back(
          // worker's execution
          [this] {
            std::function<void()> task;
            // until thread thread pool terminated
            while (true) {
              {
                std::unique_lock<std::mutex> lock(mu);
                // if task queue is not empty or thread pool has been
                // terminated, continue else wait for notifying
                while (tasks.empty() && !terminated) {
                  cond.wait(lock);
                }
                // if task queue is empty here, thread pool must have been
                // terminated
                if (tasks.empty()) {
                  return;
                }
                task = std::move(tasks.front());
                tasks.pop();
              }
              task();
            }
          });
    }
  }

  ~ThreadPool() {
    {
      std::unique_lock<std::mutex> lock(mu);
      terminated = true;
    }
    cond.notify_all();
    for (std::thread &w : workers) {
      w.join();
    }
  }

  // add a task to queue, return a future struct witch contains the return value
  // of the task
  template <class Func, class... Args>
  auto addTask(Func &&func, Args &&... args)
      -> std::future<typename std::result_of<Func(Args...)>::type> {
    // return type of func
    using retType = typename std::result_of<Func(Args...)>::type;
    auto task = std::make_shared<std::packaged_task<retType()>>(
        (std::bind(std::forward<Func>(func), std::forward<Args>(args)...)));
    // store result of task
    std::future<retType> ret = task->get_future();

    {
      std::unique_lock<std::mutex> lock(mu);
      // package task function to type void()
      tasks.emplace([task] { (*task)(); });
    }
    cond.notify_one();
    return ret;
  }

 private:
  // thread pool
  std::vector<std::thread> workers;
  // task queue, contains packaged functions
  std::queue<std::function<void()>> tasks;
  // to protect task queue
  std::mutex mu;
  // for workers coordination
  std::condition_variable cond;
  // termination sign
  bool terminated;
};