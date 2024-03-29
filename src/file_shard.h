#pragma once

#include <vector>
#include "mapreduce_spec.h"
#include <string>

/* CS6210_TASK: Create your own data structure here, where you can hold information about file splits,
     that your master would use for its own bookkeeping and to convey the tasks to the workers for mapping */

struct FileInfo{//information about individual file
    string filename;
    long startIdx;
    long size;
};
struct FileShard {
    //each shard can have more than one file
    std::vector<FileInfo> fileData;
    //constructor
    FileShard(): fileData(){}
};


/* CS6210_TASK: Create fileshards from the list of input files, map_kilobytes etc. using mr_spec you populated  */ 
inline bool shard_files(const MapReduceSpec& mr_spec, std::vector<FileShard>& fileShards) {
    fileShards=std::vector<FileShard>();

    //first read info of each file
    std::vector<FileInfo> fileInfos;

    for(auto& filename: mr_spec.inputFiles){
        ifstream in_file(filename, ios::binary);
        in_file.seekg(0, ios::end);
        FileInfo inf={filename,0,in_file.tellg()};
        fileInfos.push_back(inf);
        if(in_file.is_open()){
            in_file.close();
        }
    }

    //now make the shards


    cout <<"now sharding files" <<endl;
    while(fileInfos.size()){
        //cout <<fileInfos.size() <<endl;
        FileShard shard;
        long remainingShardSize=mr_spec.mapSizeBytes;
        while(remainingShardSize){
            FileInfo *currInfo=&fileInfos.back();

            FileInfo subShard;
            //cout << currInfo->filename <<" "<< currInfo->startIdx<<" "<< currInfo->size <<endl;
            if(currInfo->size==0){
                fileInfos.pop_back();
                if(!fileInfos.size()){
                    remainingShardSize=0;
                    continue;
                }
                currInfo=&fileInfos.back();
            }
            subShard.filename=currInfo->filename;
            subShard.startIdx=currInfo->startIdx;
            subShard.size=min(currInfo->size,remainingShardSize);
            {
                cout <<"opening file" <<currInfo->filename << endl;
                std::ifstream strm(currInfo->filename);
                strm.seekg(subShard.startIdx+subShard.size);

                char curr;
                int additionalCount=0;
                //make sure we don't cut off the word half way, wait till end of line
                while(strm.get(curr) &&(curr!=' '&&curr!='\n')){
                    additionalCount++;
                }
                strm.close();
                subShard.size+=additionalCount;

            }
            remainingShardSize=max(remainingShardSize-subShard.size,(long)0);
            currInfo->startIdx+=subShard.size;
            currInfo->size=max(currInfo->size-subShard.size,(long)0);
            //currInfo->size-=subShard.size;

            shard.fileData.push_back(subShard);

        }
        if(shard.fileData.size()){
            fileShards.push_back(shard);
        }
    }




    cout <<"num shards: "<<fileShards.size() <<endl;
    for(auto & sd: fileShards){
        cout <<"\nshard details" <<endl;
        for(auto & sub : sd.fileData){
            cout << "filename: " << sub.filename << " idx:" << sub.startIdx << " size:" <<sub.size << endl;
        }
    }

    cout <<"done sharding"<<endl;

	return true;
}
