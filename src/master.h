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
#include <chrono>
#include <stdio.h>

using namespace std;
using namespace grpc;
using namespace masterworker;

class MasterService;

struct WorkerInfo{
    static long jobIDCounter;
    string ipAddress;
    std::shared_ptr<workerJob> assignedJob;
    std::shared_ptr<jobResultsInfo> jobResults;
    std::shared_ptr<Channel> channel;
    std::shared_ptr<worker::Stub> stub;
    workerStatus::Status status;

    bool createConnection(){
        channel=grpc::CreateChannel(ipAddress, grpc::InsecureChannelCredentials());
        stub=shared_ptr(worker::NewStub(channel));
        return true;

    }
    bool getStatus(){
        ClientContext context;
        std::chrono::time_point deadline = std::chrono::system_clock::now() +
            std::chrono::milliseconds(300);
        context.set_deadline(deadline);//if we don't get health status in 300 ms, we consider it dead

        masterInfo minf;
        workerStatus statquery;
        Status querystatus = stub->getHealth(&context, minf, &statquery);


        if (!querystatus.ok()) {
            std::cout << ipAddress<< " " << querystatus.error_code() << ": " << querystatus.error_message()
                      << std::endl;
            return false;
        }

        status= workerStatus::Status(statquery.status());

        cout << "received status from "<< ipAddress << endl;
        return true;


    }
    bool assignJob(std::shared_ptr<workerJob> jb){
        assignedJob=jb;
        jb->set_jobid(jobIDCounter++);
        ClientContext context;
        std::chrono::time_point deadline = std::chrono::system_clock::now() +
            std::chrono::milliseconds(300);
        context.set_deadline(deadline);//if we don't get health status in 300 ms, we consider the worker dead
        workerStatus statquery;
        Status querystatus = stub->setJob(&context, *assignedJob, &statquery);


        if (!querystatus.ok()) {
            std::cout << ipAddress << querystatus.error_code() << ": " << querystatus.error_message()
                      << std::endl;
            return false;
        }

        status= workerStatus::Status(statquery.status());

        cout << "job assigned to "<< ipAddress << endl;

        return true;


    }
    bool getJobResults(){
        ClientContext context;
        std::chrono::time_point deadline = std::chrono::system_clock::now() +
            std::chrono::milliseconds(500);
        context.set_deadline(deadline);//if we don't get results within timeout, we consider it dead


        jobResults= std::shared_ptr<jobResultsInfo>(new jobResultsInfo);
        Status querystatus = stub->jobDoneResults(&context, *assignedJob, jobResults.get());


        if (!querystatus.ok()) {
            std::cout << ipAddress << querystatus.error_code() << ": " << querystatus.error_message()
                      << std::endl;
            return false;
        }


        cout << "received results from "<< ipAddress << endl;
        return true;


    }
};
long WorkerInfo::jobIDCounter=0;


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


        vector<std::shared_ptr<WorkerInfo>> m_uninitialisedworkers;
        vector<std::shared_ptr<WorkerInfo>> m_busyworkers;
        vector<std::shared_ptr<WorkerInfo>> m_freeworkers;

        vector<std::shared_ptr<workerJob>> m_mapperJobs;
        vector<std::shared_ptr<workerJob>> m_reducerJobs;
        vector<std::shared_ptr<workerJob>> m_unassignedJobs;
        vector<std::shared_ptr<workerJob>> m_jobsInProgress;
        mutex m_jobQueueLock; //protects job list

        vector<std::shared_ptr<jobResultsInfo>> m_completedTasks;
        //after map is done, we should have m completed tasks
        //after reduce, goal is to reach m+n completed tasks


        //This checks all workers' health as well as assigns jobs
        //if a worker fails, does fault handling
        //this must set jobid


        void checkThreadHealth(){
            //first establish communication with all the threads
            //thread is down, remove from worker queue


            for(auto& worker: m_uninitialisedworkers){
                if(worker->createConnection()){
                    m_freeworkers.push_back(worker);
                }
            }


            while(true){
                cout << "checking free workers" << endl;
                for(int i=0;i<m_freeworkers.size();i++){
                    //check if worker is alive, if not, remove from worker list
                    if(!m_freeworkers[i]->getStatus()){
                        m_freeworkers.erase(m_freeworkers.begin() + i);
                        i--;
                    }
                    //assign job to worker
                    else{
                        m_jobQueueLock.lock();
                        if(m_unassignedJobs.size()){
                            auto job =  m_unassignedJobs.back();
                            m_unassignedJobs.pop_back();
                            //try to assign job
                            if(!m_freeworkers[i]->assignJob(job)){
                                //worker died during assignment
                                //remove from list
                                m_freeworkers.erase(m_freeworkers.begin() + i);
                                i--;
                            }
                            else{
                                //move worker to busy queue
                                m_busyworkers.push_back(m_freeworkers[i]);
                                m_freeworkers.erase(m_freeworkers.begin() + i);
                                i--;
                            }
                        }
                        m_jobQueueLock.unlock();

                    }
                }
                //sleep to avoid too much connection traffic
                std::this_thread::sleep_for(std::chrono::milliseconds(1000));

                for(int i=0;i<m_busyworkers.size();i++){
                    cout << "checking busy workers" << endl;
                    //check worker status,
                    //if not alive, first take its job and then remove from worker list
                    if(!m_busyworkers[i]->getStatus()){
                        //take this worker's job and put it back on the pool
                        m_jobQueueLock.lock();
                        m_unassignedJobs.push_back(m_busyworkers[i]->assignedJob);
                        m_busyworkers.erase(m_busyworkers.begin() + i);
                        i--;
                        m_jobQueueLock.unlock();
                    }
                    //worker is done, take its results and move to free queue
                    else if((m_busyworkers[i]->status)==workerStatus::JOB_DONE){
                        //request job results
                        if(!m_busyworkers[i]->getJobResults()){
                            m_jobQueueLock.lock();
                            //worker died during this query. remove it and take its job
                            m_unassignedJobs.push_back(m_busyworkers[i]->assignedJob);
                            m_jobQueueLock.lock();

                            m_busyworkers.erase(m_busyworkers.begin() + i);
                            i--;

                        }
                        else{
                            //put jobresults on queue and free this worker
                            m_jobQueueLock.lock();
                            m_completedTasks.push_back(m_busyworkers[i]->jobResults);
                            m_jobQueueLock.unlock();
                            m_freeworkers.push_back(m_busyworkers[i]);
                            m_busyworkers.erase(m_busyworkers.begin() + i);
                            i--;
                        }
                    }


                }
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
                cout << "done mapper jobs= "<<m_completedTasks.size()<<"/"<<m_mapperJobs.size() <<endl;
                m_jobQueueLock.unlock();

                if(!done){
                    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
                }
                else{
                    cout <<"mapper jobs all done" << endl;
                    break;
                }
            }
            while(true);

        }


        void distributeDataToReducers(){
            //read results of mapper jobs and create reducer jobs

            m_jobQueueLock.lock();
            for(auto&taskResult :m_completedTasks){
                for(int i=0;i<taskResult->keysandvalues_size();i++){
                    int idx= stoi(taskResult->keysandvalues(i).key());
                    m_reducerJobs[idx]->add_filespathlistreducer(taskResult->keysandvalues(i).value());
                }
            }
            m_jobQueueLock.unlock();

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
                cout << "done total jobs= "<<m_completedTasks.size()<<"/"<<m_mapperJobs.size()+m_reducerJobs.size()<<endl;
                m_jobQueueLock.unlock();
                if(!done){
                    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
                }
                else{
                    cout <<"reduce done" <<endl;
                    break;
                }
            }
            while(true);
        }

};



/* CS6210_TASK: This is all the information your master will get from the framework.
	You can populate your other class data members here if you want */
Master::Master(const MapReduceSpec& mr_spec, const std::vector<FileShard>& file_shards):
    m_uninitialisedworkers(),
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
        m_uninitialisedworkers.push_back(worker);
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
    cout <<"Cleaning the intermediate files"<< endl;
    int fileCounter=0;
    for(std::shared_ptr<jobResultsInfo> task: m_completedTasks){
        if(fileCounter==m_mapperJobs.size()){
            break;
        }
        fileCounter++;
        cout <<"Cleaning up intermediate mapper files for mapper task "<< endl;
        for(int i=0;i<task->keysandvalues_size();i++){
            string path= task->keysandvalues(i).value();
            if(remove(path.c_str())!= 0 ){
                cout <<"failed to remove intermediate file " << path << endl;
            }
            else{
                cout <<"removed intermediate file " << path << endl;

            }
        }
    }


    m_unassignedJobs.clear();
    m_completedTasks.clear();
    m_mapperJobs.clear();
    m_reducerJobs.clear();



    cout <<"MAPREDUCE job done" <<endl;

	return true;
}
