#pragma once

#include <vector>
#include "mapreduce_spec.h"
#include <string>

/* CS6210_TASK: Create your own data structure here, where you can hold information about file splits,
     that your master would use for its own bookkeeping and to convey the tasks to the workers for mapping */

struct FileInfo{
    string filename;
    long startIdx;
    long size;
};
struct FileShard {
    std::vector<FileInfo> fileData;
    FileShard(): fileData(){}
};


/* CS6210_TASK: Create fileshards from the list of input files, map_kilobytes etc. using mr_spec you populated  */ 
inline bool shard_files(const MapReduceSpec& mr_spec, std::vector<FileShard>& fileShards) {
    fileShards=std::vector<FileShard>();

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
            remainingShardSize-=subShard.size;
            currInfo->startIdx+=subShard.size;
            currInfo->size-=subShard.size;

            shard.fileData.push_back(subShard);
        }
        if(shard.fileData.size()){
            fileShards.push_back(shard);
        }
    }



    /*for(auto& filename: mr_spec.inputFiles){
       ifstream in_file(filename, ios::binary);
       in_file.seekg(0, ios::end);
       long file_size = in_file.tellg();
       int currIdx=0;
       while(file_size){
            FileShard aShard();
            FileInfo fileData;
            fileData.filename=filename;
            fileData.startIdx=currIdx;
            
            if(mr_spec.mapSizeBytes>file_size){
                aShard.endIdx=currIdx+file_size-1;
                fileShards.push_back(aShard);
                break;
            }
            aShard.endIdx=currIdx+mr_spec.mapSizeBytes-1;
            fileShards.push_back(aShard);
            
            currIdx+=mr_spec.mapSizeBytes;
            file_size-=mr_spec.mapSizeBytes;
        }

    }*/




    cout <<"num shards"<<fileShards.size() <<endl;
    for(auto & sd: fileShards){
        cout <<"\nshard" <<endl;
        for(auto & sub : sd.fileData){
            cout << "filename: " << sub.filename << " idx:" << sub.startIdx << " size:" <<sub.size << endl;
        }
    }

    cout <<"done"<<endl;

	return true;
}
