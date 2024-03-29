#ifndef WORKER_H
#define WORKER_H
#pragma once

#include <iostream>
#include <fstream>
#include <chrono>
#include <thread>
#include <mutex>
#include <vector>
#include <sstream>
#include <queue>
#include <grpc++/grpc++.h>
#include "masterworker.grpc.pb.h"
#include "masterworker.pb.h"
#include "mr_task_factory.h"
#include "mr_tasks.h"
#include <algorithm>
#include <functional>

using namespace std;
using namespace grpc;
using namespace masterworker;
class WorkerService;

/* CS6210_TASK: Handle all the task a Worker is supposed to do.
    This is a big task for this project, will test your understanding of map reduce */
class Worker {

    public:
        /* DON'T change the function signature of this constructor */
        Worker(std::string ip_addr_port);

        /* DON'T change this function's signature */
        bool run();

    private:
        /* NOW you can add below, data members and member functions as per the need of your implementation*/

        //grpc related classes
        std::shared_ptr<ServerBuilder> m_builder;
        std::shared_ptr<WorkerService>m_service;
        std::shared_ptr<Server> m_server;
        mutable workerJob m_jb;
        friend class BaseReducerInternal;
        friend class BaseMapperInternal;
        friend class WorkerService;

        //ip address of worker
        string m_workerip;
        //lambda for emitter to be shared with the emit function
        std::function<void (std::string,std::string)> m_shared_emitter;

        //queue of key value pairs
        queue<keyValuePair> m_queue;
        //list of file handles to be written to
        map<int,pair<string,shared_ptr<ofstream>>>m_fileHandles;
        //this worker's status
        mutable workerStatus m_workerStatus;
        //worker's results
        jobResultsInfo m_results;

        std::hash<std::string> m_hasher;


        void doWrites(){//do key value queue writes to file
            int numFilesOpen=m_fileHandles.size();

            if(m_jb.jobtype()==workerJob::MAPPER){//mapper job
                while(m_queue.size()){
                    keyValuePair x= m_queue.front();
                    cout <<"writing key "<< x.key() << endl;
                    //use hasher to find which key to write to
                    cout << "to file name" <<m_fileHandles[m_hasher(x.key())%numFilesOpen].first << endl;
                    *(m_fileHandles[m_hasher(x.key())%numFilesOpen].second) << x.key()<<" "<< x.value()<<endl;
                    m_queue.pop();
                }
            }
            else{
                while(m_queue.size()){//reducer jobs
                    keyValuePair x= m_queue.front();
                    cout <<"writing key "<< x.key() << endl;
                    cout << "file name" <<m_fileHandles[m_jb.jobid()].first << endl;
                    *(m_fileHandles[m_jb.jobid()].second) << x.key()<<" "<< x.value()<<endl;
                    m_queue.pop();
                }
            }
            cout << "batch write done for current queue of keys and values" << endl;
        }

        //handle emitted value this is passed as a lambda to the friend class that handles emit
        void dealWithEmittedValue(const std::string& key, const std::string& val){
            keyValuePair kvp;
            kvp.set_key(key);
            kvp.set_value(val);
            cout <<"emitted key" <<key;
            cout <<" emitted val" <<val << endl;
            m_queue.push(kvp);
            if(m_jb.jobtype()==workerJob::MAPPER){
                //do occasional writes to disk (if num in the queue is 5 times number of files we need to write)
                if(m_fileHandles.size()*5<m_queue.size()){
                    doWrites();
                }
            }
        }

        //finish worker job
        void finishWorkerJob(){

            cout <<"finishing the job's final writes"<< endl;
            doWrites();

            m_results= jobResultsInfo();
            for(auto& out:m_fileHandles){
                keyValuePair* kp = m_results.add_keysandvalues();
                kp->set_key(to_string(out.first));
                kp->set_value(out.second.first);
                out.second.second->close();
            }
            m_fileHandles.clear();

            cout <<"setting job status"<< endl;
            m_workerStatus.set_status(workerStatus::JOB_DONE);


        }
};

//service to be used by master to send jobs and get status by master
class WorkerService final : public worker::Service{
public:
    WorkerService(const Worker *wp):m_wp(wp) {}

  private:
    friend class Worker;//access worker state
    const Worker* m_wp;//access to worker instance

    //heartbeat for the master to use
    Status getHealth(ServerContext* context, const masterInfo* request,
                     workerStatus* reply) override {
       reply->set_status(m_wp->m_workerStatus.status());
       return Status::OK;
     }

    //rpc for master to assign worker a task
    Status setJob(ServerContext* context, const workerJob* request,
                     workerStatus* reply) override {

       m_wp->m_jb =workerJob(*request);
       m_wp->m_workerStatus.set_status(workerStatus::BUSY);
       reply->set_status(m_wp->m_workerStatus.status());

       cout <<"worker received job" <<endl;
       return Status::OK;
     }

    //rpc stub for master to get results of a job
    Status jobDoneResults(ServerContext* context, const workerJob* request,
                     jobResultsInfo* reply) override {

        //go through all the keys and send them to the master
        for(auto& keyval:m_wp->m_results.keysandvalues()){

            keyValuePair* kp =reply->add_keysandvalues();
            kp->set_key(keyval.key());
            kp->set_value(keyval.value());
        }
        //set worker as free
       m_wp->m_workerStatus.set_status(workerStatus::FREE);
       return Status::OK;
     }

};



/* CS6210_TASK: ip_addr_port is the only information you get when started.
    You can populate your other class data members here if you want */
Worker::Worker(std::string ip_addr_port) :
    m_service(new WorkerService(this)),
    m_builder(new ServerBuilder()),
    m_queue(),
    m_fileHandles(),
    m_jb(),
    m_workerip(ip_addr_port),
    m_workerStatus(),
    m_hasher()

{
    m_workerStatus.set_status(workerStatus::FREE);

    //open grpc listener for master requests

    m_builder->AddListeningPort(m_workerip, grpc::InsecureServerCredentials());
    m_builder->RegisterService(m_service.get());
    m_server=std::shared_ptr<Server>(m_builder->BuildAndStart());
    cout <<"Opened worker server at address" << m_workerip <<endl;


    //capture lambda to be used to emit values
    m_shared_emitter= [&](const std::string& key, const std::string& val) { dealWithEmittedValue(key,val); };

}


extern std::shared_ptr<BaseMapper> get_mapper_from_task_factory(const std::string& user_id);
extern std::shared_ptr<BaseReducer> get_reducer_from_task_factory(const std::string& user_id);

/* CS6210_TASK: Here you go. once this function is called your woker's job is to keep looking for new tasks
    from Master, complete when given one and again keep looking for the next one.
    Note that you have the access to BaseMapper's member BaseMapperInternal impl_ and
    BaseReduer's member BaseReducerInternal impl_ directly,
    so you can manipulate them however you want when running map/reduce tasks*/
bool Worker::run() {
    while(true){
        //clean out existing queues
        m_queue= queue<keyValuePair>();
        m_fileHandles.clear();
    //wait for a job
        if(m_workerStatus.status()!=workerStatus::BUSY){
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));

            cout <<"no job at the moment, sleeping"<< endl;
            continue;
        }



        cout <<"job received"<< endl;

        //do job
        if(m_jb.jobtype()==workerJob::MAPPER){

            cout <<"received mapper job"<< endl;

            //get mapper from factory
            std::shared_ptr<BaseMapper> mapper = get_mapper_from_task_factory(m_jb.userid());
            //set the lambda to emit data
            mapper->impl_->m_workerEmit=m_shared_emitter;
            //read file portion into string and call map
            string stringToMap="";

            //read everything that is part of the shard into one string
            for(auto& mapPortion:m_jb.fileportions()){
                std::ifstream strm(mapPortion.mapfilepath());
                strm.seekg (mapPortion.startidxmapper());
                std::copy_n(std::istreambuf_iterator<char>(strm.rdbuf()),
                            mapPortion.size(), std::back_inserter(stringToMap));
                strm.close();
                stringToMap+=" ";//add whitespace at the end
            }

            //create file handles to be written to
            for(int i=0;i<m_jb.mapfilesplitcount();i++){
                string pt=to_string(m_jb.jobid())+"_"+to_string(i)+".out";
                m_fileHandles[i]=pair(pt,shared_ptr<ofstream>(new ofstream(pt)));
            }

            //replace any line breaks (if any)
            std::replace( stringToMap.begin(), stringToMap.end(), '\n', ' ');

            //apply the map function of the factory to the concatenated string
            mapper->map(stringToMap);


            cout <<"mapping done"<< endl;

        }
        else{//REDUCER
            std::shared_ptr<BaseReducer> reducer = get_reducer_from_task_factory(m_jb.userid());
            reducer->impl_->m_workerEmit=m_shared_emitter;


            cout <<"reducer job received"<< endl;

            //reducer key: list of values pair that we will be reducing
            map<string,vector<string>> keyMultiValuePair;
            //read individual files, sort and merge them
            vector<string> alphabeticalOrderKeys;
            for(string filename: m_jb.filespathlistreducer()){
                ifstream infile(filename);
                std::string line;
                //read lines of the assigned mapper files
                while (std::getline(infile, line))
                {
                    std::istringstream ss(line);
                    string key;
                    string value;
                    string token;
                    bool foundKey=false;
                    while(std::getline(ss, token, ' ')) {
                        if(!foundKey){
                            key=token;
                            foundKey=true;
                        }
                        else{
                            value=token;
                            if ( keyMultiValuePair.find(key) == keyMultiValuePair.end() ) {
                              // not found in the hashmap, initialise
                                alphabeticalOrderKeys.push_back(key);
                                keyMultiValuePair[key]=vector<string>();
                            }
                            keyMultiValuePair[key].push_back(value);
                            cout <<"adding kp" <<key <<"" << value << endl;
                        }
                    }
                }
                infile.close();
            }

            //sort keys alphabetically
            std::sort(alphabeticalOrderKeys.begin(), alphabeticalOrderKeys.end());


            //create output file handle
            string pt=m_jb.reduceroutputpath()+"/"+to_string(m_jb.jobid())+".out";
            m_fileHandles[m_jb.jobid()]=pair(pt,shared_ptr<ofstream>(new ofstream(pt)));
            //apply reduce on individual key, vector of value pairs
            for(auto& key: alphabeticalOrderKeys){
                reducer->reduce(key,keyMultiValuePair[key]);
            }

            cout <<"reducing done"<< endl;

        }

        //do final writes for the job
        finishWorkerJob();



    }

    return true;
}
#endif
