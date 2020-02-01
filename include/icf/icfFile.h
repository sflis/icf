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
    struct ICFFileHeader{

        ICFFileHeader():version_(0),compression_(0),unused_(0),ext_head_len_(0){
            strncpy(file_identifier_,"ICF",4);
            strncpy(file_sub_identifier_,"",4);

        }

        template <class T>
        void Serialize(T& archive)
        {
            time_stamp_2 = std::chrono::system_clock::now();
            time_stamp_ = std::chrono::system_clock::to_time_t(time_stamp_2);
            archive & file_identifier_ &
                    file_sub_identifier_ &
                    version_ &
                    compression_ &
                    time_stamp_&
                    unused_ &
                    ext_head_len_;
            time_stamp_2 = std::chrono::system_clock::from_time_t(time_stamp_);

        }

        char file_identifier_[4];
        char file_sub_identifier_[4];
        uint16_t version_;
        uint16_t compression_;
        std::time_t time_stamp_;
        std::chrono::time_point<std::chrono::system_clock> time_stamp_2;
        uint16_t unused_;
        uint16_t ext_head_len_;
    };

public:
    enum access_mode{read, trunc, append};

    ICFFile(std::string path, ICFFile::access_mode mode=ICFFile::append);

    ~ICFFile();

    void write(const void* data, std::size_t size);

    std::shared_ptr<std::vector<unsigned char> > read_at(uint64_t index);

    void flush();

    void close(){file_handle_.close(); }

    uint64_t size(){return object_index_.size();}

    std::chrono::time_point<std::chrono::system_clock> get_timestamp(){return file_header_.time_stamp_2;}
private:
    ICFFileHeader file_header_;
    std::fstream file_handle_;
    uint64_t current_read_pointer_;
    uint64_t current_write_pointer_;
    std::vector<uint64_t> object_index_;
    Archive<std::fstream> serializer_stream_;
};




}//icf namespace


#endif