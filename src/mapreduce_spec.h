#pragma once

#include <string>
#include <vector>
#include <iostream>
#include <fstream>
#include <sys/stat.h> 
#include <sys/types.h> 

using namespace std;


/* CS6210_TASK: Create your data structure here for storing spec from the config file */
struct MapReduceSpec {
    int numWorkers;
    std::vector<std::string> workerIPaddresses;
    std::vector<std::string> inputFiles;
    string outputDir;
    int numOutputFiles;
    int mapSizeKB;
    string userID;
    //init function
    MapReduceSpec():workerIPaddresses(),inputFiles(),numWorkers(0),
outputDir(""),numOutputFiles(0),mapSizeKB(0),userID(""){}
};


/* CS6210_TASK: Populate MapReduceSpec data structure with the specification from the config file */
inline bool read_mr_spec_from_config_file(const std::string& config_filename, MapReduceSpec& mr_spec) {
    mr_spec =MapReduceSpec();
    ifstream readfile(config_filename);
    std::string line;
    while (getline (readfile, line)) {
        //read config
        if(!line.size()){
            continue;//skip blank lines
        }
        int equalsPos=line.find("=");//find where equals sign is in the ini
        std::string paramName=line.substr(0,equalsPos);
        std::string paramStr=line.substr(equalsPos+1);
        if (paramName.compare("n_workers") == 0){
            mr_spec.numWorkers=std::stoi(paramStr);

            
            cout << "numWorkers" << mr_spec.numWorkers << endl;
        }
        else if (paramName.compare("worker_ipaddr_ports") == 0){
            int currStartPos=equalsPos+1;
            bool endFound=false;
            while(true){
                int endPos=paramStr.size()-1;
                if(paramStr.find(",")!=std::string::npos){
                    currStartPos=paramStr.find(",");
                    mr_spec.workerIPaddresses
                        .push_back(paramStr.substr(0,currStartPos++));

                    
                        cout << "workerIP" << paramStr.substr(0,currStartPos-1) << endl;
                }
                else{
                    mr_spec.workerIPaddresses.push_back(paramStr);
                    cout << "workerIP2" << paramStr << endl;
                    break;
                }
                paramStr=paramStr.substr(currStartPos);
            }
        }
        else if (paramName.compare("input_files") == 0){
            int currStartPos=equalsPos+1;
            bool endFound=false;
            while(true){
                int endPos=paramStr.size()-1;
                if(paramStr.find(",")!=std::string::npos){
                    currStartPos=paramStr.find(",");
                    mr_spec.inputFiles
                        .push_back(paramStr.substr(0,currStartPos++));

                    
                        cout << "input file" << paramStr.substr(0,currStartPos-1) << endl;
                }
                else{
                    mr_spec.inputFiles.push_back(paramStr);
                    cout << "input file2" << paramStr << endl;
                    break;
                }
                paramStr=paramStr.substr(currStartPos);
            }
        }
        else if (paramName.compare("output_dir") == 0){

            mr_spec.outputDir=paramStr;
            cout << "out directory" << mr_spec.outputDir << endl;
        }
        else if (paramName.compare("n_output_files") == 0){

            mr_spec.numOutputFiles=std::stoi(paramStr);
            cout << "out file ct" << mr_spec.numOutputFiles << endl;
        }
        else if (paramName.compare("map_kilobytes") == 0){

            mr_spec.mapSizeKB=std::stoi(paramStr);
            cout << "mapSizeKB" << mr_spec.mapSizeKB << endl;
        }
        else if (paramName.compare("user_id") == 0){

            mr_spec.userID=paramStr;
            cout << "userID" << mr_spec.userID << endl;
        }
        
        
    }
    readfile.close();
	return true;
}


/* CS6210_TASK: validate the specification read from the config file */
inline bool validate_mr_spec(const MapReduceSpec& mr_spec) {
    if(mr_spec.numWorkers<=0 || mr_spec.workerIPaddresses.size()==0 || mr_spec.inputFiles.size()==0 || mr_spec.outputDir.size()==0 || mr_spec.numOutputFiles<=0 || mr_spec.mapSizeKB<=0 || mr_spec.userID.size()==0){

        
        cout << "one param is size zero" << endl;
        return false;
    }

    if (mkdir(mr_spec.outputDir.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH) == -1)
    {
        if( errno == EEXIST ) {
           // already exists
            cout << "output folder already exists" << endl;
        } else {
           return false;
        }
    }

    for(auto &filename: mr_spec.inputFiles){
        if (FILE *file = fopen(filename.c_str(), "r")) {
        fclose(file);
            return true;
        } else {
            cout << "file not valid [" << filename<< "]"<< endl;
            return false;
        }   
    }

    

	return true;
}
