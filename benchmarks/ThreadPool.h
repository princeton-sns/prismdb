#ifndef THREAD_POOL_H
#define THREAD_POOL_H

#include <vector>
#include <queue>
#include <memory>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <future>
#include <functional>
#include <stdexcept>
#include <chrono>
#include <atomic>

#define MAX_THREADS 32

class ThreadPool {
public:
    ThreadPool(size_t);
    template<class F, class... Args>
    void enqueueOp(int pid, F&& f, Args&&... args);
    template<class F, class... Args>
    auto enqueue(F&& f, Args&&... args) 
        -> std::future<typename std::result_of<F(Args...)>::type>;
    ~ThreadPool();
private:
    // need to keep track of threads so we can join them
    std::vector< std::thread > workers;
    // the task queue
    std::queue< std::function<void()> > tasks[MAX_THREADS];
    
    // synchronization
    std::mutex queue_mutex[MAX_THREADS];
    std::condition_variable condition[MAX_THREADS];
    std::atomic_flag spinlock[MAX_THREADS];
    bool stop;
};
 
// the constructor just launches some amount of workers
inline ThreadPool::ThreadPool(size_t threads)
    :   stop(false)
{
    if (threads > MAX_THREADS){
      fprintf(stderr, "ERROR: Threadpool only supports %d threads\n", MAX_THREADS);
      return;
    }
    for(size_t i = 0;i<threads;++i){
        spinlock[i].clear(std::memory_order_release);
        workers.emplace_back(
            [this](int i)
            {
	              int pid = i;
		            int j = 0;
                uint32_t num_tasks = 0;
                uint32_t queue_size_max = 0;
                //using nano = std::chrono::nanoseconds;
                //auto begin = std::chrono::high_resolution_clock::now();
                //auto end = std::chrono::high_resolution_clock::now();
                //uint64_t task_time_ns = 0;
                for(;;)
                {
                    /*if (num_tasks%50000 == 0){
                      end = std::chrono::high_resolution_clock::now();
                      fprintf(stderr, "THREADPOOL tid %d 50K time %llu ms task_time_ms %llu\n", i, std::chrono::duration_cast<nano>(end - begin).count()/1000000, task_time_ns/1000000);
                      begin = end;
                      task_time_ns = 0;
                    }*/
                    std::function<void()> task;
                    {
                        /*std::unique_lock<std::mutex> lock(this->queue_mutex[pid]);
                        this->condition[pid].wait(lock);
                            //[this](int k){ return this->stop || !this->tasks[k].empty();}, pid);
                        if(this->stop && this->tasks[pid].empty())
                            return;*/
                        /*while(true){
			                    int p = 0;
			                    //if (this->tasks[pid].empty()) { p++; } else {break;}
			                    if (this->tasks[pid].empty()) { std::this_thread::sleep_for(std::chrono::microseconds(1)); }
                          else {break;}
			                  }*/
                        while (true) {
                          while(spinlock[pid].test_and_set(std::memory_order_acquire));
                          if (!this->tasks[pid].empty()) {
                            task = std::move(this->tasks[pid].front());
                            this->tasks[pid].pop();
			                      spinlock[pid].clear(std::memory_order_release);
			                      break;
                          } else {
                            spinlock[pid].clear(std::memory_order_release);
                            // if kill signal is set, stop running this thread
                            if (this->stop) {
                              return;
                            }
                          }
                        }
                    }
		                //fprintf(stderr, "Tasks num %d\n", this->tasks[pid].size());
		                //fprintf(stderr, "Task dequeue time %llu\n", std::chrono::duration_cast<nano>(std::chrono::high_resolution_clock::now() - begin).count());
                    //auto begin_task = std::chrono::high_resolution_clock::now();
                    task();
                    //auto end_task = std::chrono::high_resolution_clock::now();
                    //fprintf(stderr, "THREADPOOL tid %d time_ns %llu\n", i, std::chrono::duration_cast<nano>(end_task - begin_task).count());
                    //task_time_ns += std::chrono::duration_cast<nano>(end_task - begin_task).count();
                    //num_tasks++;
             
                    if (++num_tasks%10000 == 0){
                      if (this->tasks[pid].size() > queue_size_max) {
                        queue_size_max = this->tasks[pid].size();
                      }
                      fprintf(stderr, "%X\tTHREADPOOL: pid %llu queue_size %llu queue_size_max %llu\n", std::this_thread::get_id(), pid, this->tasks[pid].size(), queue_size_max);
                    }
                }
            }, i);
    }
}

template<class F, class... Args>
void ThreadPool::enqueueOp(int pid, F&& f, Args&&... args)
{
    using nano = std::chrono::nanoseconds;
    auto begin = std::chrono::high_resolution_clock::now();
    using return_type = typename std::result_of<F(Args...)>::type;

    auto task = std::make_shared< std::packaged_task<return_type()> >(
            std::bind(std::forward<F>(f), std::forward<Args>(args)...)
        );

    //std::future<return_type> res = task->get_future();
    {
        //std::unique_lock<std::mutex> lock(queue_mutex[0]);

        // don't allow enqueueing after stopping the pool
        //if(stop)
        //    //throw std::runtime_error("enqueue on stopped ThreadPool");
        //    fprintf(stderr, "enqueue on stopped ThreadPool");
        //    assert(false);
        while(spinlock[pid].test_and_set(std::memory_order_acquire));
        tasks[pid].emplace([task](){ (*task)(); });
        spinlock[pid].clear(std::memory_order_release);
        //fprintf(stderr, "Enqueue time %llu\n", std::chrono::duration_cast<nano>(std::chrono::high_resolution_clock::now() - begin).count());
    }
    //condition[0].notify_one();
    //return res;
}

// add new work item to the pool
/*template<class F, class... Args>
auto ThreadPool::enqueue(F&& f, Args&&... args) 
    -> std::future<typename std::result_of<F(Args...)>::type>
{
    using return_type = typename std::result_of<F(Args...)>::type;

    auto task = std::make_shared< std::packaged_task<return_type()> >(
            std::bind(std::forward<F>(f), std::forward<Args>(args)...)
        );
        
    std::future<return_type> res = task->get_future();
    {
        std::unique_lock<std::mutex> lock(queue_mutex);

        // don't allow enqueueing after stopping the pool
        if(stop)
            //throw std::runtime_error("enqueue on stopped ThreadPool");
            fprintf(stderr, "enqueue on stopped ThreadPool");
            assert(false);    

        tasks.emplace([task](){ (*task)(); });
    }
    condition.notify_one();
    return res;
}*/

// the destructor joins all threads
inline ThreadPool::~ThreadPool()
{
    /*int i=0;
    for(std::thread &worker: workers){
        std::unique_lock<std::mutex> lock(queue_mutex[i]);
        stop = true;
	      condition[i].notify_all();
	      i++;
    }*/
    stop = true;
    for(std::thread &worker: workers)
        worker.join();
}

#endif
