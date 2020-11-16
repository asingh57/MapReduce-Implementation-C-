#pragma once


#include <grpc++/grpc++.h>
#include "masterworker.grpc.pb.h"
#include "masterworker.pb.h"
#include "mr_task_factory.h"
#include "mr_tasks.h"
#include "mapreduce_spec.h"
#include "file_shard.h"
#include <thread>
#include <vector>
#include <queue>
using namespace std;
using namespace grpc;
using namespace masterworker;

class MasterService;

struct WorkerInfo{
    string ipAddress;
    std::shared_ptr<workerJob> assignedJob;
    std::shared_ptr<jobResultsInfo> jobResults;
    workerStatus status;
    std::shared_ptr<Channel> channel;
    std::shared_ptr<worker::Stub> stub;
};


/* CS6210_TASK: Handle all the bookkeeping that Master is supposed to do.
	This is probably the biggest task for this project, will test your understanding of map reduce */
class Master {

	public:
		/* DON'T change the function signature of this constructor */
		Master(const MapReduceSpec&, const std::vector<FileShard>&);

		/* DON'T change this function's signature */
		bool run();

        ~Master(){
            m_backgroundThread.join();
        }
	private:
		/* NOW you can add below, data members and member functions as per the need of your implementation*/

        std::thread m_backgroundThread;


        vector<std::shared_ptr<WorkerInfo>> m_workers;

        vector<std::shared_ptr<workerJob>> m_mapperJobs;
        vector<std::shared_ptr<workerJob>> m_reducerJobs;
        vector<std::shared_ptr<workerJob>> m_unassignedJobs;
        vector<std::shared_ptr<workerJob>> m_jobsInProgress;
        mutex m_jobQueueLock; //protects job list

        vector<std::shared_ptr<jobResultsInfo>> m_completedTasks;
        //after map is done, we should have m completed tasks
        //after reduce, goal is to reach m+n completed tasks


        //TODO
        //This checks all workers' health as well as assigns jobs
        //if a worker fails, does fault handling
        //this must set jobid
        void checkThreadHealth(){
            while(true){

            }
        }

        void distributeDataToMappers(){
            //queue data
            m_jobQueueLock.lock();
            for(auto &jb : m_mapperJobs){
                m_unassignedJobs.push_back(jb);
            }
            m_jobQueueLock.unlock();


            //check if mapper jobs are done

            bool done=false;
            do{
                m_jobQueueLock.lock();
                done=m_mapperJobs.size()==m_completedTasks.size();
                m_jobQueueLock.unlock();
                if(!done){
                    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
                }
                else{
                    break;
                }
            }
            while(true);

        }


        //TODO: create reducer jobs
        void distributeDataToReducers(){
            //queue data
            m_jobQueueLock.lock();
            for(auto &jb : m_reducerJobs){
                m_unassignedJobs.push_back(jb);
            }
            m_jobQueueLock.unlock();


            //block till completed jobs list = sum of mapper and reducer jobs
            bool done=false;
            do{
                m_jobQueueLock.lock();
                done=m_mapperJobs.size()+m_reducerJobs.size()==m_completedTasks.size();
                m_jobQueueLock.unlock();
                if(!done){
                    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
                }
                else{
                    break;
                }
            }
            while(true);
        }

};



/* CS6210_TASK: This is all the information your master will get from the framework.
	You can populate your other class data members here if you want */
Master::Master(const MapReduceSpec& mr_spec, const std::vector<FileShard>& file_shards):
    m_workers(),
    m_mapperJobs(),
    m_reducerJobs(),
    m_unassignedJobs(),
    m_jobQueueLock(),
    m_jobsInProgress(),
    m_completedTasks()
{

    //create workerInfos
    for(auto& ip: mr_spec.workerIPaddresses){
        shared_ptr<WorkerInfo> worker(new WorkerInfo);
        worker->ipAddress=ip;
        m_workers.push_back(worker);
    }



    //create mapper jobs
    for(auto& shard:file_shards){
        shared_ptr<workerJob> mapjob(new workerJob);
        mapjob->set_jobtype(workerJob::MAPPER);
        mapjob->set_userid(mr_spec.userID);
        for(auto& subshard:shard.fileData){
            mapFilePortion* fileportion=mapjob->add_fileportions();
            fileportion->set_mapfilepath(subshard.filename);
            fileportion->set_startidxmapper(subshard.startIdx);
            fileportion->set_size(subshard.size);
        }
        mapjob->set_mapfilesplitcount(mr_spec.numOutputFiles);

        //TODO
        m_mapperJobs.push_back(mapjob);
    }


    //create initial reducer job templates
    for(int i=0;i<mr_spec.numOutputFiles;i++){
        shared_ptr<workerJob> reducerJob(new workerJob);
        reducerJob->set_jobtype(workerJob::REDUCER);
        reducerJob->set_userid(mr_spec.userID);
        reducerJob->set_reduceroutputpath(mr_spec.outputDir);
        m_reducerJobs.push_back(reducerJob);
    }



    m_jobQueueLock.lock();//lock so that the health checker blocks until run() is called

    //spawn thread to check health of all registered workers
     m_backgroundThread=std::thread([&](){
        checkThreadHealth();
    });

}


/* CS6210_TASK: Here you go. once this function is called you will complete whole map reduce task and return true if succeeded */
bool Master::run() {

    //start thread health checker and assigner;
    m_jobQueueLock.unlock();

    //distribute map shards
    distributeDataToMappers();//this will block till done

    //distribute reduce shards
    distributeDataToReducers();//block till its done

    //prepare if run gets called again
    m_jobQueueLock.lock();
    m_unassignedJobs.clear();
    m_completedTasks.clear();
	return true;
}
