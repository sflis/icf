#ifndef ICF_FILE_H__
#define ICF_FILE_H__

#include "icf/archive.h"
#include <cstdio>
#include <stdint.h>
#include <string>
#include <cstring>
// #include <cassert>
// #include <algorithm>

#include <vector>
#include <deque>
#include <list>
#include <set>
#include <map>

#include <exception>
#include <stdexcept>
#include <memory>

#include <fstream>

#include <ctime>
#include <chrono>

namespace icf{

class ICFFile{
public:
    struct ICFFileHeader{

        ICFFileHeader():version_(0),compression_(0),unused_(0),ext_head_len_(0){
            strncpy(file_identifier_,"ICF",4);
            strncpy(file_sub_identifier_,"",4);

        }

        template <class T>
        void Serialize(T& archive)
        {
            time_stamp2_ = std::chrono::system_clock::now();
            time_stamp_ = std::chrono::system_clock::to_time_t(time_stamp2_);
            archive & file_identifier_ &
                    file_sub_identifier_ &
                    version_ &
                    compression_ &
                    time_stamp_&
                    unused_ &
                    ext_head_len_;
            time_stamp2_ = std::chrono::system_clock::from_time_t(time_stamp_);

        }

        char file_identifier_[4];
        char file_sub_identifier_[4];
        uint16_t version_;
        uint16_t compression_;
        std::time_t time_stamp_;
        std::chrono::time_point<std::chrono::system_clock> time_stamp2_;
        uint16_t unused_;
        uint16_t ext_head_len_;
    };


    enum access_mode{read, trunc, append};

    ICFFile(std::string path, ICFFile::access_mode mode=ICFFile::append);

    ~ICFFile();

    void write(const void* data, std::size_t size);

    std::shared_ptr<std::vector<unsigned char> > read_at(uint64_t index);

    void flush();

    void close(){file_handle_.close(); }

    uint64_t size(){return object_index_.size();}

    std::chrono::time_point<std::chrono::system_clock> get_timestamp(){return file_header_.time_stamp2_;}
private:

    void scan_file(uint64_t pos);

    ICFFileHeader file_header_;
    std::fstream file_handle_;
    uint64_t current_read_pointer_;
    uint64_t current_write_pointer_;
    std::vector<uint64_t> object_index_;
    Archive<std::fstream> serializer_stream_;
};




class ICFFileV2{
    struct ICFBunchTrailer{

        ICFBunchTrailer(std::weak_ptr<std::vector<char*> >):version_(0){
            time_stamp2_ = std::chrono::system_clock::now();
            time_stamp_ = std::chrono::system_clock::to_time_t(time_stamp2_);
        }

        void Serialize(Archive<std::fstream>& archive)
        {
            archive & version_;


            archive & unused_ &
                    time_stamp_ & 
                    file_offset_ &
                    prev_bunch_offset_ &
                    bunch_data_offset_ &
                    n_chunks_in_bunch_ &
                    bunch_number_ &
                    flags_;

        }
   
        uint16_t version_;
        uint16_t unused_;
        std::time_t time_stamp_;
        std::chrono::time_point<std::chrono::system_clock> time_stamp2_;
        uint64_t file_offset_;
        uint64_t prev_bunch_offset_;
        uint64_t bunch_data_offset_;
        uint32_t n_chunks_in_bunch_;
        uint32_t bunch_number_;
        uint32_t flags_;

        uint32_t trailer_start_offset_;

    };

public:
    enum access_mode{read, trunc, append};

    ICFFileV2(std::string path, ICFFileV2::access_mode mode=ICFFileV2::append);

    ~ICFFileV2();

    void write(const void* data, std::size_t size);

    std::shared_ptr<std::vector<unsigned char> > read_at(uint64_t index);

    void flush();

    void close(){file_handle_.close(); }

    uint64_t size(){return object_index_.size();}

    std::chrono::time_point<std::chrono::system_clock> get_timestamp(){return file_header_.time_stamp2_;}
private:

    void scan_file(uint64_t pos);

    ICFFile::ICFFileHeader file_header_;
    std::fstream file_handle_;
    uint64_t current_read_pointer_;
    uint64_t current_write_pointer_;
    std::vector<uint64_t> object_index_;
    Archive<std::fstream> serializer_stream_;
    std::vector<std::unique_ptr<char*> > write_buffer_;
    // std::vector<std::unique_prt<char*> > read_buffer_;
};



}//icf namespace


#endif