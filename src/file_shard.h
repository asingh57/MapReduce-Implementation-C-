#pragma once

#include <vector>
#include "mapreduce_spec.h"
#include <string>

/* CS6210_TASK: Create your own data structure here, where you can hold information about file splits,
     that your master would use for its own bookkeeping and to convey the tasks to the workers for mapping */
struct FileShard {
    string filename;
    int startIdx;
    int endIdx;
};


/* CS6210_TASK: Create fileshards from the list of input files, map_kilobytes etc. using mr_spec you populated  */ 
inline bool shard_files(const MapReduceSpec& mr_spec, std::vector<FileShard>& fileShards) {
    fileShards=std::vector<FileShard>();
    for(auto& filename: mr_spec.inputFiles){
       ifstream in_file(filename, ios::binary);
       in_file.seekg(0, ios::end);
       long file_size = in_file.tellg();
       int currIdx=0;
       while(file_size){
            FileShard aShard;
            aShard.filename=filename;
            aShard.startIdx=currIdx;
            
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

    }
    cout <<"num shards"<<fileShards.size() <<endl;
    /*for(auto & sd: fileShards){
        cout << sd.filename << " " << sd.startIdx << " " <<sd.endIdx << endl;
    }*/


	return true;
}
