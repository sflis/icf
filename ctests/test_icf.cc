#include <iostream>

#include "icf/icfFile.h"
#include <string>



int main(){

    icf::ICFFile icf_file(std::string("test.icf"));
    // std::cout<<icf_file.file_header_.file_identifier<<std::endl;
    uint8_t data[] = {1,2,3,4,2,3,4};
    icf_file.write((void*)data,7);
    icf_file.write((void*)data,7);

    auto read_data = icf_file.read_at(1);
    std::cout<<(*read_data)[0]<<std::endl;
    std::cout<<(*read_data)[1]<<std::endl;
    std::cout<<(*read_data)[4]<<std::endl;

}