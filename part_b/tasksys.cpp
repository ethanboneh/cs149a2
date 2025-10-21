#include "tasksys.h"


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
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync() {
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

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
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

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
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
    scheduler_thread = std::thread(&TaskSystemParallelThreadPoolSleeping::schedulerThreadFunc, this);

    for (int i = 0; i < num_threads; i++) {
        threads.emplace_back([this]() {
            while (true) {
                // next task from the task queue
                std::pair<TaskID, int> task;
                TaskBlock* task_block;
                int task_index;
                {
                    std::unique_lock<std::mutex> lock(mutex);
                    cv.wait(lock, [this]() { return !task_queue.empty() || stop; }); // wait for task or stop signal
                    
                    if (stop && task_queue.empty()) return; 
                    task = task_queue.front(); 
                    task_queue.pop();
                    TaskID task_id = task.first;
                    task_index = task.second;
                    task_block = &dependencies_map[task_id];
                }

                task_block->runnable->runTask(task_index, task_block->num_total_tasks); // can this race?
                
                {
                    std::unique_lock<std::mutex> lock(mutex);
                    task_block->num_finished_tasks++; // finished task !!
                    

                    if (task_block->num_finished_tasks == task_block->num_total_tasks) {
                        task_block->completed = true;
                        unfinished_task_group--;
                        for (auto& child_id : task_block->children) {
                            dependencies_map[child_id].num_parents--;
                            if (dependencies_map[child_id].num_parents == 0) {
                                active_queue.push(child_id);
                                scheduler_cv.notify_all();
                            }
                        }

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
        std::unique_lock<std::mutex> lock(mutex);
        stop = true;
    }
    cv.notify_all();
    cv_finished.notify_all();
    scheduler_cv.notify_all();  
    for (std::thread &thread : threads) {
        if (thread.joinable()) {
            thread.join();
        }
    }
    if (scheduler_thread.joinable()) {
        scheduler_thread.join();
    }

}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    /*
    if active queue is empty, sleep
    else: 
    while we still have threads:
        pop from active queue
        assign threads
        if this finishes out a taskID,access child to parent map
            decrement parent's dep count
            if parent's dep count is 0, push to active queue
        do we want to do this here or in sync? probably here right
    */
    TaskID fake_id = -1;
    {
        std::unique_lock<std::mutex> lock(mutex);
        TaskBlock new_block(runnable, num_total_tasks);
        new_block.num_parents = 0;
        dependencies_map[fake_id] = new_block;
        for (int i = 0; i < num_total_tasks; i++) {
            task_queue.push(std::make_pair(fake_id, i));
        }
    }
    cv.notify_all();

    {
        std::unique_lock<std::mutex> lock(mutex);
        cv_finished.wait(lock, [&]() { return dependencies_map[fake_id].num_finished_tasks == dependencies_map[fake_id].num_total_tasks; });
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks, TaskID task_id) {
    {
        std::unique_lock<std::mutex> lock(mutex);
        for (int i = 0; i < num_total_tasks; i++) {
            task_queue.push(std::make_pair(task_id, i));
        }
    }
    cv.notify_all();

    {
        std::unique_lock<std::mutex> lock(mutex); // wait until this specific task is done
        cv_finished.wait(lock, [&]() { return dependencies_map[task_id].num_finished_tasks == dependencies_map[task_id].num_total_tasks; });
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //
    /*
    if deps is empty, push tasks to active queue, wake up run.
    else:
        create entry in child to parent map with corresponding tree node.
    */
    std::unique_lock<std::mutex> lock(mutex);
    index++;
    unfinished_task_group++;

    TaskBlock new_block(runnable, num_total_tasks);
    new_block.num_parents = deps.size();
    

    for(auto& dep: deps) {
        if (dependencies_map.find(dep) == dependencies_map.end() || dependencies_map[dep].completed) { // check completed parents
            new_block.num_parents--;
        } else {
            dependencies_map[dep].children.push_back(index);
        }
    }

    dependencies_map[index] = new_block; // add new block to map

    if (new_block.num_parents == 0) { // if no dependencies, can run right away
        active_queue.push(index);
        scheduler_cv.notify_all();
    }

    return index;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //
    /*
    while there are still unfinished tasks:
    sleep
    */
    std::unique_lock<std::mutex> lock(mutex);
    cv_finished.wait(lock, [this]() { return unfinished_task_group.load() == 0; });
}

void TaskSystemParallelThreadPoolSleeping::schedulerThreadFunc() {
    while (true) {
        std::unique_lock<std::mutex> lock(mutex);
        scheduler_cv.wait(lock, [this]() { return !active_queue.empty() || stop; });

        if (stop && active_queue.empty()) {
            // need to tell sync cv to wake up
            cv_finished.notify_all();
            return;
        }
        

        int current_task = active_queue.front();
        active_queue.pop();

        for (int i = 0; i < dependencies_map[current_task].num_total_tasks; i++) {
            task_queue.push({current_task, i});
        }
        cv.notify_all();

        lock.unlock();

        // run(dependencies_map[current_task].runnable,
        //     dependencies_map[current_task].num_total_tasks,
        //     current_task); // assume this'll finish by the time it returns
        
        // {
        //     std::unique_lock<std::mutex> lock(mutex);
        //     dependencies_map[current_task].completed = true;
        //     for (auto& child_id : dependencies_map[current_task].children) {
        //         dependencies_map[child_id].num_parents--;
        //         if (dependencies_map[child_id].num_parents == 0) {
        //             active_queue.push(child_id);
        //         }
        //     }
        // }
    }
}