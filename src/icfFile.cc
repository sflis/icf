#include "icf/icfFile.h"
#include <fstream>
#include <iostream>
#include <array>

using namespace icf;





ICFFile::ICFFile(std::string path, ICFFile::access_mode mode, uint32_t bunchsize):
            serializer_stream_(file_handle_),
            n_entries_(0),
            cbunchoffset_(0),
            bunchsize_(bunchsize),
            last_bunch_fp_(0),
            bunch_number_(0){
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

    file_handle_.open(path, omode);

    current_read_pointer_ = file_handle_.tellg();
    file_handle_.seekp(0, std::ios_base::end);
    current_write_pointer_ = file_handle_.tellp();
    auto length = current_write_pointer_;
    file_handle_.seekp(0);

    if(current_write_pointer_>current_read_pointer_ && mode !=trunc){
        serializer_stream_>>file_header_;
        scan_file(file_handle_.tellp());
    }
    else{
        serializer_stream_<<file_header_;
        current_write_pointer_ = file_handle_.tellp();
    }
    // auto bt = ICFBunchTrailer();
    // serializer_stream_>>bt;
}

ICFFile::~ICFFile(){
    file_handle_.close();
}


void ICFFile::scan_file(uint64_t pos){
    // size_t obj_size;
    // auto bheader_size = sizeof(obj_size);
    // auto length = current_write_pointer_;
    // file_handle_.seekp(pos);
    // while(file_handle_.tellp()<length){
    //     object_index_.push_back(file_handle_.tellp());
    //     serializer_stream_>>obj_size;
    //     pos += obj_size + bheader_size;
    //     file_handle_.seekp(pos);
    // }
}

void ICFFile::write(const void* data, std::size_t size){
    auto datacopy = std::make_shared<std::vector<char> >(std::vector<char>(size));
    memcpy(datacopy->data(),data, size);
    write_buffer_.push_back(datacopy);
    n_entries_ +=1;
    cbunchoffset_ += size;
    if(cbunchoffset_> bunchsize_){
        flush();
    }

}

void ICFFile::flush(){
    cbunchoffset_ = 0;
    if(write_buffer_.size()<1){
        return;
    }
    file_handle_.seekp(0, std::ios_base::end);
    auto bunch_start_fp = file_handle_.tellp();
    std::vector<uint32_t> cbunchindex;
    // cbunchindex_
    for(auto &data: write_buffer_){
        cbunchindex.push_back(data->size());
        file_handle_.write(data->data(),data->size());
    }
    auto curr_bt_fp = file_handle_.tellp();
    ICFFile::ICFBunchTrailer bunch_trailer(curr_bt_fp,
                                           curr_bt_fp-last_bunch_fp_,
                                           curr_bt_fp-bunch_start_fp,
                                           write_buffer_.size(),
                                           bunch_number_);
    serializer_stream_<<bunch_trailer;
    for(const auto &index: cbunchindex){
        serializer_stream_<<index;
    }
    uint32_t offset_to_bt_start = file_handle_.tellp() - curr_bt_fp;
    serializer_stream_<<offset_to_bt_start;
    last_bunch_fp_ = curr_bt_fp;
    write_buffer_.clear();
    cbunchoffset_ = 0;
    bunch_number_++;

}


std::shared_ptr<std::vector<unsigned char> > ICFFile::read_at(uint64_t index){


}