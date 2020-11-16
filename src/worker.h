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
        std::shared_ptr<ServerBuilder> m_builder;
        std::shared_ptr<WorkerService>m_service;
        std::shared_ptr<Server> m_server;
        unique_ptr<master::Stub> m_masterStub;
        workerJob m_jb;
        /* NOW you can add below, data members and member functions as per the need of your implementation*/
        friend class BaseReducerInternal;
        friend class BaseMapperInternal;
        friend class WorkerService;

        mutable mutex m_lock;
        mutable shared_ptr<string> m_masterIPAddress;
        string m_workerip;
        std::function<void (std::string,std::string)> m_shared_emitter;

        queue<keyValuePair> m_queue;
        vector<string> m_outputPaths;
        vector<shared_ptr<ofstream>>m_fileHandles;

        void setMasterIP(const string masterIP) const{
            if(!m_masterIPAddress){
                m_masterIPAddress=std::shared_ptr<string>(new string(masterIP));
            }
            m_lock.unlock();
        }


        bool getJobFromMaster(){
            ClientContext context;
            workerInfo query;
            query.set_workerip(m_workerip);
            Status status = m_masterStub->getJob(&context,query,&m_jb);

            if (!status.ok()) {
                std::cout << status.error_code() << ": " << status.error_message()
                          << std::endl;
                return false;
            }

            return true;
        }

        void doWrites(){
            //empty out the queue
            int sz=m_queue.size();
            int numFilesOpen=m_outputPaths.size();
            for(int i=0;i<sz;i++){
                keyValuePair x= m_queue.back();
                *(m_fileHandles[i%numFilesOpen]) << x.key()<<" "<< x.value()<<endl;
                m_queue.pop();
            }
        }

        //handle emitted value
        void dealWithEmittedValue(const std::string& key, const std::string& val){
            keyValuePair kvp;
            kvp.set_key(key);
            kvp.set_value(val);
            m_queue.push(kvp);
            if(m_jb.jobtype()==workerJob::MAPPER==workerJob::MAPPER){
                //do occasional writes to disk
                if(m_outputPaths.size()*5<m_queue.size()){
                    doWrites();
                }
            }
        }

        //finish worker job
        void finishWorkerJob(){
            doWrites();

            jobResultsInfo results;
            for(string outPath:m_outputPaths){
                keyValuePair* kp = results.add_keysandvalues();
                kp->set_key(outPath);
                kp->set_value(outPath);
            }

            for(auto& handle: m_fileHandles){
                handle->close();
            }

            ClientContext context;
            masterInfo minfo;
            //query.set_workerip(m_workerip);
            Status status = m_masterStub->jobDone(&context,results,&minfo);

            if (!status.ok()) {
                std::cout << status.error_code() << ": " << status.error_message()
                          << std::endl;
            }


        }
};

class WorkerService final : public worker::Service{
public:
    WorkerService(const Worker *wp):m_wp(wp) {}

  private:
    friend class Worker;
    const Worker* m_wp;
    Status getHealth(ServerContext* context, const masterInfo* request,
                     workerStatus* reply) override {
       reply->set_ishealthy(true);
       (m_wp)->setMasterIP(request->ipaddress());
       return Status::OK;
     }
};



/* CS6210_TASK: ip_addr_port is the only information you get when started.
    You can populate your other class data members here if you want */
Worker::Worker(std::string ip_addr_port) :
    m_service(new WorkerService(this)),
    m_builder(new ServerBuilder()),
    m_queue(),
    m_outputPaths(),
    m_fileHandles(),
    m_jb(),
    m_workerip(ip_addr_port),
    m_masterIPAddress() ,
    m_lock()

{
    m_lock.lock();

    //open grpc listener for master requests

    m_builder->AddListeningPort(ip_addr_port, grpc::InsecureServerCredentials());
    m_builder->RegisterService(m_service.get());
    m_server=std::shared_ptr<Server>(m_builder->BuildAndStart());
    cout <<"Opened worker server at address" << ip_addr_port <<endl;

    cout <<"waiting for master communication" << endl;

    //block till we fill m_masterIPAddress
    m_lock.lock();
    m_lock.unlock();
    cout <<"established communication with the master" << endl;
    //launch grpc client on port
    m_masterStub=(master::NewStub(CreateChannel(*m_masterIPAddress, InsecureChannelCredentials())));

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
        m_queue.empty();
        m_outputPaths.empty();
        m_fileHandles.empty();
    //poll for a job
        if(!getJobFromMaster()){
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            continue;
        }

    //do job
        if(m_jb.jobtype()==workerJob::MAPPER){


            std::shared_ptr<BaseMapper> mapper = get_mapper_from_task_factory(m_jb.userid());
            //set the lambda to emit data
            mapper->impl_->m_workerEmit=m_shared_emitter;
            //read file portion into string and call map
            string stringToMap="";

            for(auto& mapPortion:m_jb.fileportions()){
                std::ifstream strm(mapPortion.mapfilepath());
                strm.seekg (mapPortion.startidxmapper());
                //strm.read (buffer, y);
                std::copy_n(std::istreambuf_iterator<char>(strm.rdbuf()),
                            mapPortion.size(), std::back_inserter(stringToMap));
                strm.close();
            }
            for(int i=0;i<m_jb.mapfilesplitcount();i++){
                string pt=m_jb.jobid()+"_"+to_string(i)+".out";
                m_outputPaths.push_back(pt);
                m_fileHandles.push_back(shared_ptr<ofstream>(new ofstream(pt)));
            }
            mapper->map(stringToMap);

        }
        else{//REDUCER
            std::shared_ptr<BaseReducer> reducer = get_reducer_from_task_factory(m_jb.userid());
            reducer->impl_->m_workerEmit=m_shared_emitter;



            map<string,vector<string>> keyMultiValuePair;
            string readData="";
            //read individual files, sort and merge them
            vector<string> alphabeticalOrderKeys;
            for(string filename: m_jb.filespathlistreducer()){
                ifstream infile(filename);
                std::string line;
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
                              // not found
                                alphabeticalOrderKeys.push_back(key);
                                keyMultiValuePair[key]=vector<string>(1);
                            }
                            keyMultiValuePair[key].push_back(value);
                        }
                    }
                }
                infile.close();
            }

            std::sort(alphabeticalOrderKeys.begin(), alphabeticalOrderKeys.end());


            string pt=m_jb.jobid()+".out";
            m_outputPaths.push_back(pt);
            m_fileHandles.push_back(shared_ptr<ofstream>(new ofstream(pt)));
            for(auto& key: alphabeticalOrderKeys){
                reducer->reduce(key,keyMultiValuePair[key]);
            }

        }
        finishWorkerJob();



    }

    return true;
}
#endif
