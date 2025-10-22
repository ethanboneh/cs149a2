#include "tasksys.h"
#include <iostream>


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads), num_threads(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    
    // initialized num_threads
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    
    int threads_to_use = std::min(num_threads, num_total_tasks);
    std::vector<std::thread> threads;
    threads.reserve(threads_to_use);

    int chunk = (num_total_tasks + threads_to_use - 1) / threads_to_use;
    for(int t = 0; t < threads_to_use; t++) {
        int start = t * chunk;
        int end = std::min(start + chunk, num_total_tasks);
        threads.emplace_back([=]() {
            for(int i = start; i < end; i++) {
                runnable->runTask(i, num_total_tasks);
            }
        });
    }

    for (auto& th : threads) {
        th.join();
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads), num_threads(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    
    threads.reserve(num_threads);
    

    for (int i = 0; i < num_threads; i++) {
        threads.emplace_back([this]() {
            while (true) {
                // next task from the task queue
                
                // LOCK
                mutex.lock();
                int local_remaining_tasks = remaining_tasks;
                while (remaining_tasks == 0 && !stop) {

                    mutex.unlock();
                    
            
                    mutex.lock();
                }

                if (stop) {
                    
                    mutex.unlock();
                    return;
                }

                while(remaining_tasks != 0) {
                    
                    remaining_tasks--;
                    local_remaining_tasks = remaining_tasks;

                    
                    mutex.unlock();
                    
                    
                    
                    current_runnable->runTask(local_remaining_tasks, total_tasks);
                    mutex.lock();
                    finished_remaining_tasks--;
                }
    
                mutex.unlock();
                

            }
        });
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    
    mutex.lock();
    // std::cout << "Shutting down spinning thread pool... from new destructor " << remaining_tasks << std::endl;
    stop = true;
    mutex.unlock();

    for (std::thread &thread : threads) {
        if (thread.joinable()) {
            thread.join();
        }
        
    }
    // std::cout << "Joined a spinning thread" << std::endl; 
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    
    mutex.lock();
    total_tasks = num_total_tasks;
    remaining_tasks = num_total_tasks;
    finished_remaining_tasks = num_total_tasks;
    current_runnable = runnable;

    while (finished_remaining_tasks > 0) {
        
        mutex.unlock();
        mutex.lock();
    }
    mutex.unlock();
    // std::cout << "Finished all spinning tasks!" << std::endl;
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    threads.reserve(num_threads);

    for (int i = 0; i < num_threads; i++) {
        threads.emplace_back([this]() {
            while (true) {
                // next task from the task queue
                std::pair<IRunnable*, int> task;
                {
                    std::unique_lock<std::mutex> lock(mutex);
                    cv.wait(lock, [this]() { return !task_queue.empty() || stop; });
                    
                    if (stop && task_queue.empty()) return;
                    task = task_queue.front();
                    task_queue.pop();
                }
                
                IRunnable* new_task = task.first;
                int task_id = task.second;
                
                new_task->runTask(task_id, total_tasks);
                remaining_tasks--;
                if(remaining_tasks == 0) {
                    {
                        std::unique_lock<std::mutex> lock(mutex);
                        cv_finished.notify_all();
                    }
                }
            }
        });
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    {
        std::lock_guard<std::mutex> lock(mutex);
        stop = true;
    }
    cv.notify_all();
    for (std::thread &thread : threads) {
        if (thread.joinable()) {
            thread.join();
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    {
        std::unique_lock<std::mutex> lock(mutex);
        total_tasks = num_total_tasks;
        remaining_tasks = num_total_tasks;
        for (int i = 0; i < num_total_tasks; i++) {
            task_queue.push(std::make_pair(runnable, i));
        }
    }
    cv.notify_all();

    {
        std::unique_lock<std::mutex> lock(mutex);
        cv_finished.wait(lock, [this]() { return remaining_tasks == 0; });
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
