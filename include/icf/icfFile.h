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

namespace icf{

class ICFFile{
    struct ICFFileHeader{

        ICFFileHeader():version(0),compression(0),unused(0),ext_head_len(0){
            strncpy(file_identifier_,"ICF",4);
            strncpy(file_sub_identifier_,"",4);

        }

        template <class T>
        void Serialize(T& archive)
        {
            time_stamp_ = std::time(nullptr);
            archive & file_identifier_ &
                    file_sub_identifier_ &
                    version_ &
                    compression_ &
                    time_stamp_&
                    unused_ &
                    ext_head_len_;
        }

        char file_identifier_[4];
        char file_sub_identifier_[4];
        uint16_t version_;
        uint16_t compression_;
        std::time_t time_stamp_;
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