#include <fstream>
#include "icf/archive.h"
#include "icf/icfFile.h"
#include <iostream>

using namespace icf;

ICFFile::ICFFile(std::string path, ICFFile::access_mode mode){
    auto omode = std::ios_base::in | std::ios_base::binary;
    switch(mode){
        case append:
            omode |= std::ios_base::app | std::ios_base::out;
            break;
        case trunc:
            omode |= std::ios_base::trunc | std::ios_base::out;
        default:
            break;
    }

    file_handle_.open(path,omode);

    current_read_pointer_ = file_handle_.tellg();
    file_handle_.seekp(0, std::ios_base::end);
    current_write_pointer_ = file_handle_.tellp();
    auto length  = current_write_pointer_;
    file_handle_.seekp(0);
    // std::cout<<current_read_pointer_<<"  "<<current_write_pointer_<<" "<<data_start_point_<< std::endl;

    if(current_write_pointer_>current_read_pointer_ && mode !=trunc){
        Archive<std::fstream> header_stream(file_handle_);
        header_stream>>file_header_;
        data_start_point_ = file_handle_.tellp();
        size_t obj_size;
        auto bheader_size = sizeof(obj_size);
        auto curr_fp = data_start_point_;

        while(file_handle_.tellp()<length){
            object_index_.push_back(file_handle_.tellp());
            header_stream>>obj_size;
            curr_fp += obj_size + bheader_size;
            file_handle_.seekp(curr_fp);
            // std::cout<<curr_fp<<std::endl;
        }
    }
    else{
        Archive<std::fstream> header_stream(file_handle_);
        header_stream<<file_header_;
        data_start_point_ = file_handle_.tellp();
    }

    // std::cout<<current_read_pointer_<<"  "<<current_write_pointer_<<" "<<data_start_point_<< std::endl;
}

ICFFile::~ICFFile(){
    file_handle_.close();
}



void ICFFile::write(const void* data, std::size_t size){
    file_handle_.seekp(current_write_pointer_);
    Archive<std::fstream> serializer_stream(file_handle_);
    serializer_stream<<size;
    file_handle_.write((char*)data,size);
    current_write_pointer_ = file_handle_.tellp();
}



std::shared_ptr<std::vector<unsigned char> > ICFFile::read_at(uint64_t index){
    Archive<std::fstream> serializer_stream(file_handle_);
    auto fp = object_index_[index];
    size_t obj_size;
    file_handle_.seekp(fp);
    serializer_stream>>obj_size;
    auto data = std::make_shared<std::vector<unsigned char> >(obj_size);

    file_handle_.read((char*) data->data(),obj_size);
    return data;
}