#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"
#include <unordered_map>
#include <condition_variable>
#include <mutex>
#include <queue>
#include <vector>
#include <thread>
#include <atomic>
#include <map>




struct TaskBlock {
    int num_parents;
    std::vector<TaskID> children;
    IRunnable* runnable;
    bool completed = false;

    int num_total_tasks;
    int num_finished_tasks = 0;
    
    TaskBlock(IRunnable* r, int n)
        : num_parents(0), runnable(r), num_total_tasks(n) {}
    
    TaskBlock() = default;
};

/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial: public ITaskSystem {
    public:
        TaskSystemSerial(int num_threads);
        ~TaskSystemSerial();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn: public ITaskSystem {
    public:
        TaskSystemParallelSpawn(int num_threads);
        ~TaskSystemParallelSpawn();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSpinning: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSpinning(int num_threads);
        ~TaskSystemParallelThreadPoolSpinning();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSleeping: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSleeping(int num_threads);
        ~TaskSystemParallelThreadPoolSleeping();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();

    private:
        // FROM PART A
        int num_threads;
        std::vector<std::thread> threads;
        std::queue<std::pair<TaskID, int>> task_queue;
        std::atomic<int> remaining_tasks{0};

        std::mutex mutex;
        std::condition_variable cv;
        std::condition_variable cv_finished;
        bool stop = false;
        int total_tasks = 0;

        // new stuff:
        void schedulerThreadFunc(); // will run new tasks
        std::thread scheduler_thread; // thread to run ^
        std::condition_variable scheduler_cv; // gets woken up for ^

        std::unordered_map<TaskID, TaskBlock> dependencies_map; // track dependencies
        std::queue<TaskID> active_queue; // ready to run task groups, based on ID.
        
        TaskID index = 0; // for labeling TaskIDs.
        std::atomic<int> unfinished_task_group{0};

        void run(IRunnable* runnable, int num_total_tasks, TaskID task_id);
};

#endif
